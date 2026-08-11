import json
import logging
import threading
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone
from config import DATABASE_URL, PAPER_BALANCE

logger = logging.getLogger(__name__)

DDL = """
CREATE TABLE IF NOT EXISTS paper_trades (
    id SERIAL PRIMARY KEY,
    market_id TEXT,
    title TEXT,
    asset TEXT,
    side TEXT,
    size FLOAT,
    price FLOAT,
    confidence TEXT,
    reasons TEXT,
    signal_rsi FLOAT,
    signal_ema9 FLOAT,
    signal_ema21 FLOAT,
    momentum TEXT,
    opened_at TIMESTAMPTZ DEFAULT NOW(),
    resolved_at TIMESTAMPTZ,
    outcome TEXT,
    pnl FLOAT,
    win BOOLEAN
);

CREATE TABLE IF NOT EXISTS paper_state (
    id INT PRIMARY KEY DEFAULT 1,
    balance FLOAT,
    initial_balance FLOAT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO paper_state (id, balance, initial_balance)
VALUES (1, %(bal)s, %(bal)s)
ON CONFLICT (id) DO NOTHING;
"""

def _safe_float(val):
    """Convert numpy floats or None to plain Python float."""
    if val is None:
        return None
    try:
        return float(val)
    except Exception:
        return None

class PaperTrader:
    def __init__(self):
        self.conn = None
        self._lock = threading.Lock()
        self.balance = PAPER_BALANCE
        self.initial_balance = PAPER_BALANCE
        self.open_positions = {}
        self._connect()

    def _connect(self):
        if not DATABASE_URL:
            logger.warning("No DATABASE_URL — using in-memory state only")
            return
        try:
            self.conn = psycopg2.connect(DATABASE_URL)
            self.conn.autocommit = True
            with self.conn.cursor() as cur:
                cur.execute(DDL, {"bal": PAPER_BALANCE})
            self._load_state()
            logger.info("PaperTrader DB connected")
        except Exception as e:
            logger.error(f"PaperTrader DB error: {e}")
            self.conn = None

    def _load_state(self):
        if not self.conn:
            return
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT balance, initial_balance FROM paper_state WHERE id=1")
                row = cur.fetchone()
                if row:
                    self.balance = row["balance"]
                    self.initial_balance = row["initial_balance"]
                cur.execute("SELECT * FROM paper_trades WHERE resolved_at IS NULL")
                rows = cur.fetchall()
                for row in rows:
                    self.open_positions[row["market_id"]] = dict(row)
        except Exception as e:
            logger.error(f"Load state error: {e}")

    def _save_balance(self):
        if not self.conn:
            return
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "UPDATE paper_state SET balance=%s, updated_at=NOW() WHERE id=1",
                    (self.balance,)
                )
        except Exception as e:
            logger.error(f"Save balance error: {e}")

    def open_trade(self, market_id, title, asset, side, size, price,
                   confidence, reasons, indicators) -> bool:
        with self._lock:
            if market_id in self.open_positions:
                return False
            if len(self.open_positions) >= 5:
                logger.debug("Max simultaneous positions reached")
                return False
            if size > self.balance:
                logger.debug("Insufficient paper balance")
                return False

            self.balance -= size
            self._save_balance()

            trade = {
                "market_id": market_id,
                "title": title,
                "asset": asset,
                "side": side,
                "size": size,
                "price": price,
                "confidence": confidence,
                "reasons": json.dumps(reasons),
                "signal_rsi": _safe_float(indicators.get("rsi")),
                "signal_ema9": _safe_float(indicators.get("ema9")),
                "signal_ema21": _safe_float(indicators.get("ema21")),
                "momentum": indicators.get("momentum"),
                "opened_at": datetime.now(timezone.utc).isoformat(),
            }

            self.open_positions[market_id] = trade

            if self.conn:
                try:
                    with self.conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO paper_trades
                            (market_id, title, asset, side, size, price, confidence,
                             reasons, signal_rsi, signal_ema9, signal_ema21, momentum)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            RETURNING id
                        """, (
                            market_id, title, asset, side,
                            _safe_float(size), _safe_float(price),
                            confidence, json.dumps(reasons),
                            _safe_float(indicators.get("rsi")),
                            _safe_float(indicators.get("ema9")),
                            _safe_float(indicators.get("ema21")),
                            indicators.get("momentum")
                        ))
                        row = cur.fetchone()
                        trade["db_id"] = row[0] if row else None
                except Exception as e:
                    logger.error(f"Insert trade error: {e}")

            logger.info(f"PAPER TRADE OPENED: {side} {asset} ${size:.2f} on '{title}'")
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
            if win:
                # Realistic: return depends on entry price, minus 7% fee
                pnl = size * ((1.0 - entry_price) / entry_price) * (1 - 0.07)
            else:
                pnl = -size
            # On win: get back size + profit. On loss: lose size.
            self.balance += size + pnl if win else 0
            self._save_balance()

            if self.conn:
                try:
                    with self.conn.cursor() as cur:
                        cur.execute("""
                            UPDATE paper_trades
                            SET resolved_at=NOW(), outcome=%s, pnl=%s, win=%s
                            WHERE market_id=%s AND resolved_at IS NULL
                        """, (outcome, pnl, win, market_id))
                except Exception as e:
                    logger.error(f"Resolve trade error: {e}")

            logger.info(
                f"PAPER TRADE RESOLVED: {'WIN' if win else 'LOSS'} "
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
                "by_asset": {},
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
                        FROM paper_trades WHERE resolved_at IS NOT NULL
                    """)
                    row = cur.fetchone()
                    stats["total_trades"] = row["total"] or 0
                    stats["wins"] = row["wins"] or 0
                    stats["win_rate"] = (row["wins"] / row["total"] * 100) if row["total"] else 0
                    stats["total_pnl"] = float(row["total_pnl"] or 0)
                    stats["best_trade"] = float(row["best_trade"] or 0)
                    stats["worst_trade"] = float(row["worst_trade"] or 0)

                    cur.execute("""
                        SELECT asset, COUNT(*) as total,
                            COUNT(*) FILTER (WHERE win) as wins
                        FROM paper_trades WHERE resolved_at IS NOT NULL
                        GROUP BY asset
                    """)
                    stats["by_asset"] = {r["asset"]: dict(r) for r in cur.fetchall()}

                    cur.execute("""
                        SELECT * FROM paper_trades
                        WHERE resolved_at IS NOT NULL
                        ORDER BY resolved_at DESC LIMIT 20
                    """)
                    stats["recent_trades"] = [dict(r) for r in cur.fetchall()]

            except Exception as e:
                logger.error(f"Stats error: {e}")

        return stats

    def get_win_rate(self, n=50) -> float:
        if not self.conn:
            return 0.5
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) FILTER (WHERE win)::float / NULLIF(COUNT(*),0)
                    FROM (
                        SELECT win FROM paper_trades
                        WHERE resolved_at IS NOT NULL
                        ORDER BY resolved_at DESC LIMIT %s
                    ) t
                """, (n,))
                row = cur.fetchone()
                return float(row[0]) if row and row[0] else 0.5
        except Exception:
            return 0.5

    def get_win_rate_by_side(self, side: str, n=20):
        """Win rate reciente de un lado (UP/DOWN) sobre las últimas n
        resoluciones de paper trading. Se usa paper (no live) porque corre
        24/7 sin las pausas del circuit breaker, así que junta muestra más
        rápido con la misma señal real.

        Devuelve (win_rate, n_muestras) o None si no hay DB o no hay datos.
        """
        if not self.conn:
            return None
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) FILTER (WHERE win)::float / NULLIF(COUNT(*),0), COUNT(*)
                    FROM (
                        SELECT win FROM paper_trades
                        WHERE resolved_at IS NOT NULL AND side=%s
                        ORDER BY resolved_at DESC LIMIT %s
                    ) t
                """, (side, n))
                row = cur.fetchone()
                if not row or not row[1]:
                    return None
                return (float(row[0]) if row[0] is not None else None, int(row[1]))
        except Exception as e:
            logger.error(f"Win rate by side error: {e}")
            return None


_trader = PaperTrader()

def get_trader():
    return _trader