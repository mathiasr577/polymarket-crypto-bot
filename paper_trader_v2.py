"""
Paper trading para el modelo v2 (TWAP + bandas de precio) — completamente
separado del paper trading v1 (paper_trader.py, modelo Kraken/z-score) y
del trading en vivo. Tablas propias (paper_trades_v2/paper_state_v2) para
no mezclar historiales. Arranca con el mismo balance inicial que v1
(config.PAPER_BALANCE) para que sean directamente comparables.

No arriesga plata real bajo ninguna circunstancia — es la validación en
tiempo real de la política v2 antes de considerar plata real, corriendo en
paralelo mientras se sigue juntando muestra para el backtest retroactivo.
"""
import json
import logging
import threading
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone
from config import DATABASE_URL, PAPER_BALANCE

logger = logging.getLogger(__name__)

DDL = """
CREATE TABLE IF NOT EXISTS paper_trades_v2 (
    id SERIAL PRIMARY KEY,
    market_id TEXT,
    title TEXT,
    asset TEXT,
    side TEXT,
    size FLOAT,
    price FLOAT,
    band TEXT,
    reasons TEXT,
    opened_at TIMESTAMPTZ DEFAULT NOW(),
    resolved_at TIMESTAMPTZ,
    outcome TEXT,
    pnl FLOAT,
    win BOOLEAN
);

CREATE TABLE IF NOT EXISTS paper_state_v2 (
    id INT PRIMARY KEY DEFAULT 1,
    balance FLOAT,
    initial_balance FLOAT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO paper_state_v2 (id, balance, initial_balance)
VALUES (1, %(bal)s, %(bal)s)
ON CONFLICT (id) DO NOTHING;
"""

STAKE = 5.0  # tamaño fijo, coherente con el backtest (simulate_v2_policy usa el mismo)


def _safe_float(val):
    if val is None:
        return None
    try:
        return float(val)
    except Exception:
        return None


class PaperTraderV2:
    def __init__(self):
        self.conn = None
        self._lock = threading.Lock()
        self.balance = PAPER_BALANCE
        self.initial_balance = PAPER_BALANCE
        self.open_positions = {}
        self._connect()

    def _connect(self):
        if not DATABASE_URL:
            logger.warning("No DATABASE_URL — PaperTraderV2 usando solo memoria")
            return
        try:
            self.conn = psycopg2.connect(DATABASE_URL)
            self.conn.autocommit = True
            with self.conn.cursor() as cur:
                cur.execute(DDL, {"bal": PAPER_BALANCE})
            self._load_state()
            logger.info("PaperTraderV2 DB connected")
        except Exception as e:
            logger.error(f"PaperTraderV2 DB error: {e}")
            self.conn = None

    def _load_state(self):
        if not self.conn:
            return
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT balance, initial_balance FROM paper_state_v2 WHERE id=1")
                row = cur.fetchone()
                if row:
                    self.balance = row["balance"]
                    self.initial_balance = row["initial_balance"]
                cur.execute("SELECT * FROM paper_trades_v2 WHERE resolved_at IS NULL")
                for row in cur.fetchall():
                    self.open_positions[row["market_id"]] = dict(row)
        except Exception as e:
            logger.error(f"PaperTraderV2 load state error: {e}")

    def _save_balance(self):
        if not self.conn:
            return
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "UPDATE paper_state_v2 SET balance=%s, updated_at=NOW() WHERE id=1",
                    (self.balance,)
                )
        except Exception as e:
            logger.error(f"PaperTraderV2 save balance error: {e}")

    def open_trade(self, market_id, title, asset, side, price, band, reasons, stake_multiplier=1.0) -> bool:
        """stake_multiplier: escala STAKE según cuántas señales de
        confirmación (D_lead/OFI/presión TWAP) coinciden con la dirección —
        ver STAKE_MULTIPLIER_PER_CONFIRMATION en signal_engine_v2.py y el
        backtest en /api/shadow-sizing. 1.0 = comportamiento de siempre."""
        with self._lock:
            if market_id in self.open_positions:
                return False
            if len(self.open_positions) >= 5:
                logger.debug("PaperTraderV2: max simultaneous positions reached")
                return False
            size = round(STAKE * stake_multiplier, 2)
            if size > self.balance:
                logger.debug("PaperTraderV2: insufficient balance")
                return False

            self.balance -= size
            self._save_balance()

            trade = {
                "market_id": market_id, "title": title, "asset": asset,
                "side": side, "size": size, "price": price, "band": band,
                "reasons": json.dumps(reasons),
                "opened_at": datetime.now(timezone.utc).isoformat(),
            }
            self.open_positions[market_id] = trade

            if self.conn:
                try:
                    with self.conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO paper_trades_v2
                            (market_id, title, asset, side, size, price, band, reasons)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                        """, (
                            market_id, title, asset, side,
                            _safe_float(size), _safe_float(price), band, json.dumps(reasons)
                        ))
                except Exception as e:
                    logger.error(f"PaperTraderV2 insert error: {e}")

            logger.info(f"PAPER v2 TRADE OPENED: {side} {asset} ${size:.2f} @ {price:.2f} band={band} '{title}'")
            return True

    def resolve_trade(self, market_id: str, outcome: str):
        with self._lock:
            trade = self.open_positions.pop(market_id, None)
            if not trade:
                return

            side = trade["side"]
            size = trade["size"]
            entry_price = trade.get("price", 0.5)
            win = (side == outcome)
            # Fee real de Polymarket (31-ago-2026, ver comentario al inicio de
            # shadow_logger.py): fee = shares*0.07*price*(1-price), cobrado
            # solo al taker, en la ENTRADA — equivale a `size*0.07*(1-price)`
            # en términos de costo, independiente de si gana o pierde.
            fee = size * 0.07 * (1.0 - entry_price)
            if win:
                pnl = size * ((1.0 - entry_price) / entry_price) - fee
                self.balance += size + pnl
            else:
                pnl = -size - fee
                self.balance -= fee
            self._save_balance()

            if self.conn:
                try:
                    with self.conn.cursor() as cur:
                        cur.execute("""
                            UPDATE paper_trades_v2
                            SET resolved_at=NOW(), outcome=%s, pnl=%s, win=%s
                            WHERE market_id=%s AND resolved_at IS NULL
                        """, (outcome, pnl, win, market_id))
                except Exception as e:
                    logger.error(f"PaperTraderV2 resolve error: {e}")

            logger.info(
                f"PAPER v2 RESOLVED: {'WIN' if win else 'LOSS'} "
                f"PnL={pnl:+.2f} Balance={self.balance:.2f}"
            )

    def get_stats(self) -> dict:
        with self._lock:
            stats = {
                "balance": self.balance,
                "initial_balance": self.initial_balance,
                "pnl": self.balance - self.initial_balance,
                "roi": (self.balance - self.initial_balance) / self.initial_balance * 100,
                "open_count": len(self.open_positions),
                "open_positions": list(self.open_positions.values()),
                "total_trades": 0,
                "wins": 0,
                "win_rate": 0,
                "total_pnl": 0,
                "best_trade": 0,
                "worst_trade": 0,
                "by_band": {},
                "recent_trades": [],
            }

        if self.conn:
            try:
                with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("""
                        SELECT COUNT(*) as total,
                            COUNT(*) FILTER (WHERE win) as wins,
                            SUM(pnl) as total_pnl,
                            MAX(pnl) as best_trade,
                            MIN(pnl) as worst_trade
                        FROM paper_trades_v2 WHERE resolved_at IS NOT NULL
                    """)
                    row = cur.fetchone()
                    stats["total_trades"] = row["total"] or 0
                    stats["wins"] = row["wins"] or 0
                    stats["win_rate"] = (row["wins"] / row["total"] * 100) if row["total"] else 0
                    stats["total_pnl"] = float(row["total_pnl"] or 0)
                    stats["best_trade"] = float(row["best_trade"] or 0)
                    stats["worst_trade"] = float(row["worst_trade"] or 0)

                    cur.execute("""
                        SELECT band, COUNT(*) as total,
                            COUNT(*) FILTER (WHERE win) as wins
                        FROM paper_trades_v2 WHERE resolved_at IS NOT NULL
                        GROUP BY band
                    """)
                    stats["by_band"] = {r["band"]: dict(r) for r in cur.fetchall()}

                    cur.execute("""
                        SELECT * FROM paper_trades_v2
                        WHERE resolved_at IS NOT NULL
                        ORDER BY resolved_at DESC LIMIT 20
                    """)
                    stats["recent_trades"] = [dict(r) for r in cur.fetchall()]
            except Exception as e:
                logger.error(f"PaperTraderV2 stats error: {e}")

        return stats

    def get_today_by_band(self) -> dict:
        """Mismo desglose por banda que get_stats(), pero filtrado a HOY
        (UTC — el día de trading entero cae en un solo día calendario UTC,
        ver TRADING_START_UTC/TRADING_END_UTC en config.py, así que no hace
        falta el offset ET que usa live_trader_v2 para el rollover).
        Construido 21-ago-2026 para poder comparar directamente el
        resultado de HOY en paper (mismas señales, sin riesgo de ejecución)
        contra el resultado de HOY en plata real — la pregunta concreta
        era si un mal día en vivo también aparece en paper (día de mercado
        raro para el modelo) o solo en vivo (algo de la ejecución real)."""
        result = {"total_trades": 0, "wins": 0, "win_rate": 0, "total_pnl": 0, "by_band": {}}
        if not self.conn:
            return result
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT COUNT(*) as total,
                        COUNT(*) FILTER (WHERE win) as wins,
                        SUM(pnl) as total_pnl
                    FROM paper_trades_v2
                    WHERE resolved_at IS NOT NULL AND resolved_at::date = (NOW() AT TIME ZONE 'UTC')::date
                """)
                row = cur.fetchone()
                result["total_trades"] = row["total"] or 0
                result["wins"] = row["wins"] or 0
                result["win_rate"] = round(row["wins"] / row["total"] * 100, 1) if row["total"] else 0
                result["total_pnl"] = round(float(row["total_pnl"] or 0), 2)

                cur.execute("""
                    SELECT band, COUNT(*) as n,
                        COUNT(*) FILTER (WHERE win) as wins,
                        SUM(pnl) as pnl
                    FROM paper_trades_v2
                    WHERE resolved_at IS NOT NULL AND resolved_at::date = (NOW() AT TIME ZONE 'UTC')::date
                    GROUP BY band
                """)
                for r in cur.fetchall():
                    n = r["n"] or 0
                    result["by_band"][r["band"]] = {
                        "n": n,
                        "wins": r["wins"] or 0,
                        "win_rate": round((r["wins"] or 0) / n * 100, 1) if n else 0,
                        "pnl": round(float(r["pnl"] or 0), 2),
                    }
        except Exception as e:
            logger.error(f"PaperTraderV2 get_today_by_band error: {e}")
        return result


_trader_v2 = PaperTraderV2()


def get_trader_v2():
    return _trader_v2
