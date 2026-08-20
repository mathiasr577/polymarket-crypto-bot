"""
Live trader para el modelo v2 (TWAP + bandas de precio + confirmaciones) —
ejecuta órdenes REALES en Polymarket. Reusa toda la lógica de seguridad que
ya se probó y arregló a mano en live_trader.py (v1) durante semanas de
trading real: reserva síncrona + ejecución en hilo aparte para no bloquear
el tick de main.py, extracción de precio/costo real de fill
(makingAmount/takingAmount, no un campo "price" directo — confirmado contra
respuestas reales), manejo de fills de estado DESCONOCIDO (cancel_order()
no lanza excepción cuando la orden ya se llenó — se confirmó con un caso
real donde eso hizo perder de vista una posición ganadora), circuit
breaker de drawdown diario, y sizing como % del balance en vez de monto
fijo.

Dos mejoras sobre v1 (v1 nunca las tuvo, encontradas al construir esto):
1. Todo se persiste en Postgres (live_trades_v2, live_state_v2) — v1 vive
   solo en memoria, así que un restart de Railway borraba su historial y
   reseteaba el tracking de drawdown a mitad de día. Acá no.
2. Guarda la banda (cheap/mid_confirmed/favorite) de cada trade, para
   poder ver rendimiento real en vivo por banda — no solo el total.

NO se activa solo con PAPER_TRADING=false — necesita además
LIVE_V2_ENABLED=true explícito (ver config.py) como freno extra antes de
arriesgar plata real con un modelo nuevo.
"""
import json
import logging
import threading
import time
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone, timedelta

from config import DRAWDOWN_LIMIT, DATABASE_URL

logger = logging.getLogger(__name__)

MAX_LIVE_TRADES = 1_000_000  # sin techo real de trades — corre indefinido
TRADE_SIZE = 5.0             # techo de sizing, mismo valor conservador que v1 al arrancar
MAX_NO_FILLS_HISTORY = 20

# Mismo esquema de sizing proporcional al balance que ya validamos en v1
# (agregado el 10-ago-2026 tras ver que $5 fijo era >13% de una cuenta
# chica y el circuit breaker cortaba el día antes de juntar muestra).
TARGET_RISK_PCT = 0.08
MIN_TRADE_USD = 2.0

ET_OFFSET = timedelta(hours=-4)  # mismo offset fijo que usa el resto del código

_FILL_PRICE_FIELDS = ("price", "avgPrice", "average_price", "averagePrice", "fillPrice", "fill_price")

DDL = """
CREATE TABLE IF NOT EXISTS live_trades_v2 (
    id SERIAL PRIMARY KEY,
    market_id TEXT,
    title TEXT,
    asset TEXT,
    side TEXT,
    band TEXT,
    size FLOAT,
    price FLOAT,
    decision_price FLOAT,
    token_id TEXT,
    reasons TEXT,
    opened_at TIMESTAMPTZ DEFAULT NOW(),
    resolved_at TIMESTAMPTZ,
    outcome TEXT,
    pnl FLOAT,
    win BOOLEAN,
    order_resp TEXT,
    latency_ms INT
);

CREATE TABLE IF NOT EXISTS live_state_v2 (
    id INT PRIMARY KEY DEFAULT 1,
    day_start_balance FLOAT,
    current_trading_day DATE,
    today_pnl FLOAT DEFAULT 0,
    drawdown_paused BOOLEAN DEFAULT FALSE,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO live_state_v2 (id) VALUES (1) ON CONFLICT (id) DO NOTHING;
"""


def _extract_fill_info(resp: dict, decision_price: float, intended_size: float) -> tuple[float, float]:
    """Idéntica a la de live_trader.py — ver ese archivo para el contexto
    de por qué makingAmount/takingAmount y no un campo "price" directo."""
    taking = resp.get("takingAmount")
    making = resp.get("makingAmount")
    if taking is not None and making is not None:
        try:
            shares = float(taking)
            cost = float(making)
            if shares > 0 and cost > 0:
                return cost / shares, cost
        except (TypeError, ValueError):
            pass

    for key in _FILL_PRICE_FIELDS:
        val = resp.get(key)
        if val is None:
            continue
        try:
            fv = float(val)
        except (TypeError, ValueError):
            continue
        if fv > 0:
            return fv, intended_size

    logger.warning(
        f"No pude identificar el precio/costo real de fill v2 (probé makingAmount/"
        f"takingAmount y {_FILL_PRICE_FIELDS}) — uso decisión {decision_price:.3f} "
        f"y tamaño ${intended_size:.2f}. Respuesta completa: {resp}"
    )
    return decision_price, intended_size


def _safe_float(val):
    if val is None:
        return None
    try:
        return float(val)
    except Exception:
        return None


class LiveTraderV2:
    def __init__(self):
        self._lock = threading.Lock()
        self.total_trades = 0
        self.open_positions = {}
        self.results = []
        self.attempted_markets = set()
        self.active_assets = set()
        self.no_fill_count = 0
        self.blocked_count = 0
        self.recent_no_fills = []
        self.unknown_fills = []
        self._client = None
        self.day_start_balance = None
        self.current_trading_day = None
        self.today_pnl = 0.0
        self._drawdown_paused = False
        self.conn = None
        self._init_client()
        self._connect_db()

    def _init_client(self):
        try:
            from order_executor import get_client
            self._client = get_client()
            if self._client:
                logger.info("LiveTraderV2: CLOB client ready")
            else:
                logger.error("LiveTraderV2: No CLOB client")
        except Exception as e:
            logger.error(f"LiveTraderV2 init error: {e}")

    def _connect_db(self):
        if not DATABASE_URL:
            logger.warning("LiveTraderV2: no DATABASE_URL — sin persistencia (como v1)")
            return
        try:
            self.conn = psycopg2.connect(DATABASE_URL)
            self.conn.autocommit = True
            with self.conn.cursor() as cur:
                cur.execute(DDL)
            self._load_state()
            logger.info("LiveTraderV2 DB connected")
        except Exception as e:
            logger.error(f"LiveTraderV2 DB error: {e}")
            self.conn = None

    def _load_state(self):
        if not self.conn:
            return
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM live_state_v2 WHERE id=1")
                row = cur.fetchone()
                if row:
                    self.day_start_balance = row["day_start_balance"]
                    self.current_trading_day = row["current_trading_day"]
                    self.today_pnl = row["today_pnl"] or 0.0
                    self._drawdown_paused = row["drawdown_paused"] or False

                cur.execute("SELECT * FROM live_trades_v2 WHERE resolved_at IS NULL")
                for row in cur.fetchall():
                    self.open_positions[row["market_id"]] = dict(row)

                # total_trades cuenta CUALQUIER fila insertada (fills reales,
                # se inserta solo en el camino de éxito de _execute_order) —
                # no solo las resueltas. Un restart de Railway a mitad de día
                # con posiciones todavía abiertas las hubiera dejado afuera
                # del conteo para siempre (nunca se re-suman al resolverse,
                # solo _execute_order incrementa total_trades). Encontrado en
                # la revisión final antes de ir a plata real.
                cur.execute("SELECT COUNT(*) as n FROM live_trades_v2")
                self.total_trades = cur.fetchone()["n"] or 0
            logger.info(
                f"LiveTraderV2 state restored: day_start_balance={self.day_start_balance} "
                f"today_pnl={self.today_pnl} open_positions={len(self.open_positions)} "
                f"total_trades={self.total_trades}"
            )
        except Exception as e:
            logger.error(f"LiveTraderV2 load state error: {e}")

    def _save_day_state(self):
        if not self.conn:
            return
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    UPDATE live_state_v2 SET
                        day_start_balance=%s, current_trading_day=%s,
                        today_pnl=%s, drawdown_paused=%s, updated_at=NOW()
                    WHERE id=1
                """, (self.day_start_balance, self.current_trading_day, self.today_pnl, self._drawdown_paused))
        except Exception as e:
            logger.error(f"LiveTraderV2 save day state error: {e}")

    def _check_day_rollover(self):
        """Idéntica en espíritu a live_trader.py — ver ese archivo para el
        bug real que motivó el patrón de reintento en vez de aceptar 0."""
        today = (datetime.now(timezone.utc) + ET_OFFSET).date()
        with self._lock:
            is_new_day = self.current_trading_day != today
            needs_balance = is_new_day or not self.day_start_balance or self.day_start_balance <= 0
            if not needs_balance:
                return
            if is_new_day:
                self.current_trading_day = today
                self.today_pnl = 0.0
                self._drawdown_paused = False

        balance = self.get_balance()
        if not balance or balance <= 0:
            logger.error(
                f"LiveTraderV2: no pude obtener balance para drawdown (got {balance}) — "
                f"circuit breaker inactivo hasta que funcione; reintenta el próximo tick"
            )
            return

        with self._lock:
            self.day_start_balance = balance
        self._save_day_state()
        logger.info(f"LiveTraderV2 trading day {today}: day_start_balance=${balance:.2f}")

    def can_trade(self) -> bool:
        self._check_day_rollover()
        with self._lock:
            if (
                self._client is None or
                self.total_trades >= MAX_LIVE_TRADES or
                len(self.open_positions) >= 2
            ):
                return False
            if self.day_start_balance and self.day_start_balance > 0:
                drawdown = -self.today_pnl / self.day_start_balance
                if drawdown >= DRAWDOWN_LIMIT:
                    if not self._drawdown_paused:
                        self._drawdown_paused = True
                        self._save_day_state()
                        logger.warning(
                            f"LiveTraderV2 drawdown limit hit: -{drawdown*100:.1f}% of today's "
                            f"start balance (${self.day_start_balance:.2f}) — pausing until tomorrow"
                        )
                    return False
            return True

    def record_blocked(self):
        with self._lock:
            self.blocked_count += 1

    def _current_trade_size(self) -> float:
        with self._lock:
            balance = (self.day_start_balance or 0) + self.today_pnl
        if balance <= 0:
            return MIN_TRADE_USD
        size = balance * TARGET_RISK_PCT
        return round(min(TRADE_SIZE, max(MIN_TRADE_USD, size)), 2)

    def get_balance(self) -> float:
        try:
            from order_executor import get_balance
            return get_balance()
        except Exception as e:
            logger.error(f"LiveTraderV2 balance error: {e}")
            return 0.0

    def open_trade(self, market_id, title, asset, side, price, token_id,
                   band, reasons, tokens=None) -> bool:
        """Misma reserva-síncrona-más-ejecución-en-hilo-aparte que v1 — ver
        live_trader.py para el porqué (no bloquear el tick mientras se
        espera hasta 55s por un fill)."""
        with self._lock:
            if self.total_trades >= MAX_LIVE_TRADES:
                return False
            if market_id in self.open_positions:
                return False
            if market_id in self.attempted_markets:
                return False
            if len(self.open_positions) >= 2:
                return False
            if asset in self.active_assets:
                logger.info(f"LiveTraderV2: skipping {asset} — order already pending for this asset")
                return False
            self.attempted_markets.add(market_id)
            self.active_assets.add(asset)

        intended_size = self._current_trade_size()

        t = threading.Thread(
            target=self._execute_order,
            args=(market_id, title, asset, side, price, token_id, band, reasons, tokens, intended_size),
            daemon=True,
        )
        t.start()
        return True

    def _execute_order(self, market_id, title, asset, side, price, token_id, band, reasons, tokens, intended_size):
        try:
            t0 = time.time()

            from order_executor import place_order

            alt_token_id = None
            if tokens:
                alt_side = "DOWN" if side == "UP" else "UP"
                alt_token_id = tokens.get(alt_side)

            resp = place_order(
                token_id=token_id,
                price=round(price, 2),
                size=intended_size,
                side="BUY",
                alt_token_id=alt_token_id,
            )

            latency = time.time() - t0

            with self._lock:
                self.active_assets.discard(asset)

            if "error" in resp:
                error_msg = resp["error"]
                logger.error(f"LiveTraderV2 order failed: {error_msg}")

                if resp.get("unknown_fill"):
                    logger.error(
                        f"⚠️⚠️ LiveTraderV2 estado de fill DESCONOCIDO: {side} {asset.upper()} "
                        f"band={band} market={market_id} order_id={resp.get('order_id')} — "
                        f"revisar manualmente en Polymarket. No se reintenta este mercado."
                    )
                    with self._lock:
                        self.unknown_fills.append({
                            "market_id": market_id, "asset": asset, "side": side, "band": band,
                            "order_id": resp.get("order_id"), "price": price,
                            "time": datetime.now(timezone.utc).strftime("%H:%M UTC"),
                        })
                    return

                if "not filled" in error_msg or "cancelled" in error_msg:
                    self._record_no_fill(asset, side, price)
                with self._lock:
                    self.attempted_markets.discard(market_id)
                return

            status = resp.get("status", "")
            if status != "matched":
                logger.warning(f"LiveTraderV2 order not matched (status={status}) — skipping")
                self._record_no_fill(asset, side, price)
                with self._lock:
                    self.attempted_markets.discard(market_id)
                return

            fill_price, real_cost = _extract_fill_info(resp, price, intended_size)

            with self._lock:
                self.total_trades += 1
                trade = {
                    "market_id": market_id, "title": title, "asset": asset, "side": side,
                    "band": band, "size": real_cost, "price": fill_price,
                    "decision_price": price, "token_id": token_id, "reasons": json.dumps(reasons),
                    "opened_at": datetime.now(timezone.utc).isoformat(),
                    "order_resp": str(resp), "latency_ms": round(latency * 1000),
                }
                self.open_positions[market_id] = trade

            if self.conn:
                try:
                    with self.conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO live_trades_v2
                            (market_id, title, asset, side, band, size, price, decision_price,
                             token_id, reasons, order_resp, latency_ms)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """, (
                            market_id, title, asset, side, band,
                            _safe_float(real_cost), _safe_float(fill_price), _safe_float(price),
                            token_id, json.dumps(reasons), str(resp), round(latency * 1000)
                        ))
                except Exception as e:
                    logger.error(f"LiveTraderV2 insert trade error: {e}")

            logger.info(
                f"🔴 LIVE V2 TRADE: {side} {asset.upper()} band={band} ${real_cost:.2f} "
                f"@ fill={fill_price:.3f} (decision={price:.2f}) | latency={latency*1000:.0f}ms | {reasons}"
            )

        except Exception as e:
            with self._lock:
                self.active_assets.discard(asset)
                self.attempted_markets.discard(market_id)
            logger.error(f"LiveTraderV2 execute error: {e}")

    def _record_no_fill(self, asset, side, price):
        with self._lock:
            self.no_fill_count += 1
            entry = {
                "asset": asset, "side": side, "price": price,
                "time": datetime.now(timezone.utc).strftime("%H:%M UTC"),
            }
            self.recent_no_fills.insert(0, entry)
            if len(self.recent_no_fills) > MAX_NO_FILLS_HISTORY:
                self.recent_no_fills = self.recent_no_fills[:MAX_NO_FILLS_HISTORY]

    def resolve_trade(self, market_id: str, outcome: str):
        with self._lock:
            trade = self.open_positions.pop(market_id, None)
            if not trade:
                return
            side = trade["side"]
            win = (side == outcome)
            entry_price = trade["price"]
            cost = trade.get("size", TRADE_SIZE)
            if win:
                pnl = cost * ((1.0 - entry_price) / entry_price) * (1 - 0.07)
            else:
                pnl = -cost
            result = {**trade, "outcome": outcome, "win": win, "pnl": pnl}
            self.results.append(result)
            self.today_pnl += pnl

        self._save_day_state()

        if self.conn:
            try:
                with self.conn.cursor() as cur:
                    cur.execute("""
                        UPDATE live_trades_v2
                        SET resolved_at=NOW(), outcome=%s, pnl=%s, win=%s
                        WHERE market_id=%s AND resolved_at IS NULL
                    """, (outcome, _safe_float(pnl), win, market_id))
            except Exception as e:
                logger.error(f"LiveTraderV2 resolve persist error: {e}")

        logger.info(
            f"🔴 LIVE V2 RESULT: {'WIN' if win else 'LOSS'} {side} {trade['asset'].upper()} "
            f"band={trade.get('band')} | PnL={pnl:+.2f} | entry={entry_price:.2f}"
        )

    def get_stats(self) -> dict:
        with self._lock:
            wins = [r for r in self.results if r["win"]]
            losses = [r for r in self.results if not r["win"]]
            total_pnl = sum(r["pnl"] for r in self.results)
            best = max((r["pnl"] for r in self.results), default=0)
            worst = min((r["pnl"] for r in self.results), default=0)

            by_asset = {}
            by_band = {}
            for r in self.results:
                a = r.get("asset", "unknown")
                by_asset.setdefault(a, {"total": 0, "wins": 0})
                by_asset[a]["total"] += 1
                if r["win"]:
                    by_asset[a]["wins"] += 1

                b = r.get("band", "unknown")
                by_band.setdefault(b, {"total": 0, "wins": 0, "pnl": 0.0})
                by_band[b]["total"] += 1
                by_band[b]["pnl"] += r["pnl"]
                if r["win"]:
                    by_band[b]["wins"] += 1

            cash_balance = 0.0
            try:
                from order_executor import get_balance
                cash_balance = get_balance()
            except Exception:
                pass

            return {
                "total_trades": self.total_trades,
                "completed": len(self.results),
                "wins": len(wins),
                "losses": len(losses),
                "win_rate": len(wins) / len(self.results) * 100 if self.results else 0,
                "total_pnl": total_pnl,
                "pnl": total_pnl,
                "roi": (total_pnl / sum(r.get("size", TRADE_SIZE) for r in self.results) * 100) if self.results else 0,
                "best_trade": best,
                "worst_trade": worst,
                "open_count": len(self.open_positions),
                "open_positions": list(self.open_positions.values()),
                "recent_trades": list(reversed(self.results[-10:])),
                "max_trades": MAX_LIVE_TRADES,
                "done": self.total_trades >= MAX_LIVE_TRADES,
                "cash_balance": cash_balance,
                "no_fill_count": self.no_fill_count,
                "blocked_count": self.blocked_count,
                "recent_no_fills": list(self.recent_no_fills[:10]),
                "unknown_fills": list(self.unknown_fills),
                "by_asset": by_asset,
                "by_band": by_band,
                "balance": cash_balance,
                "today_pnl": self.today_pnl,
                "day_start_balance": self.day_start_balance,
                "drawdown_paused": self._drawdown_paused,
            }


_live_trader_v2 = LiveTraderV2()


def get_live_trader_v2():
    return _live_trader_v2
