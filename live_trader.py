"""
Live trader — ejecuta órdenes reales en Polymarket.
$5 por trade, limit orders solamente.
"""
import logging
import threading
from datetime import datetime, timezone, timedelta

from config import DRAWDOWN_LIMIT

logger = logging.getLogger(__name__)

MAX_LIVE_TRADES = 999
TRADE_SIZE = 5.0
MAX_NO_FILLS_HISTORY = 20

# ET en verano (EDT) es UTC-4. Mismo offset fijo que usan TRADING_START_UTC/
# TRADING_END_UTC en config.py — no es DST-aware, es consistente con el resto
# del código.
ET_OFFSET = timedelta(hours=-4)

class LiveTrader:
    def __init__(self):
        self._lock = threading.Lock()
        self.total_trades = 0
        self.open_positions = {}
        self.results = []
        self.attempted_markets = set()
        self.active_assets = set()
        self.no_fill_count = 0
        self.blocked_count = 0
        self.recent_no_fills = []  # últimos intentos sin liquidez
        self._client = None
        self.day_start_balance = None
        self.current_trading_day = None
        self.today_pnl = 0.0
        self._drawdown_paused = False
        self._init_client()

    def _init_client(self):
        try:
            from order_executor import get_client
            self._client = get_client()
            if self._client:
                logger.info("LiveTrader: CLOB client ready")
            else:
                logger.error("LiveTrader: No CLOB client")
        except Exception as e:
            logger.error(f"LiveTrader init error: {e}")

    def _check_day_rollover(self):
        """Reinicia el tracking de drawdown diario al cruzar la medianoche ET.

        Si get_balance() falla o devuelve 0 (error transitorio de red/API),
        NO se acepta ese valor como day_start_balance — se reintenta en el
        próximo tick. Antes, un solo fallo dejaba day_start_balance=0.0 para
        todo el día, y como `if self.day_start_balance and ... > 0` es False
        con 0.0, el freno de drawdown quedaba desactivado en silencio el
        resto de la jornada.
        """
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
                f"Could not fetch balance for drawdown tracking (got {balance}) — "
                f"drawdown circuit breaker inactive until this succeeds; retrying next tick"
            )
            return

        with self._lock:
            self.day_start_balance = balance
        logger.info(f"Trading day {today}: day_start_balance=${balance:.2f}")

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
                        logger.warning(
                            f"Drawdown limit hit: -{drawdown*100:.1f}% of today's "
                            f"start balance (${self.day_start_balance:.2f}) — "
                            f"pausing new live trades until tomorrow"
                        )
                    return False
            return True

    def record_blocked(self):
        """Llamar cuando una señal es bloqueada por precio/delta."""
        with self._lock:
            self.blocked_count += 1

    def get_balance(self) -> float:
        try:
            from order_executor import get_balance
            return get_balance()
        except Exception as e:
            logger.error(f"Balance error: {e}")
            return 0.0

    def open_trade(self, market_id, title, asset, side, price, token_id,
                   reasons, indicators, tokens=None) -> bool:
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
                logger.info(f"Skipping {asset} — order already pending for this asset")
                return False
            self.attempted_markets.add(market_id)
            self.active_assets.add(asset)

        try:
            import time
            t0 = time.time()

            from order_executor import place_order

            alt_token_id = None
            if tokens:
                alt_side = "DOWN" if side == "UP" else "UP"
                alt_token_id = tokens.get(alt_side)

            resp = place_order(
                token_id=token_id,
                price=round(price, 2),
                size=TRADE_SIZE,
                side="BUY",
                alt_token_id=alt_token_id,
            )

            latency = time.time() - t0

            with self._lock:
                self.active_assets.discard(asset)

            if "error" in resp:
                error_msg = resp["error"]
                logger.error(f"Order failed: {error_msg}")
                # Si es un no-fill (límite no llenado), registrarlo
                if "not filled" in error_msg or "cancelled" in error_msg:
                    self._record_no_fill(asset, side, price)
                return False

            status = resp.get("status", "")
            if status != "matched":
                logger.warning(f"Order not matched (status={status}) — skipping")
                self._record_no_fill(asset, side, price)
                return False

            with self._lock:
                self.total_trades += 1
                trade = {
                    "market_id": market_id,
                    "title": title,
                    "asset": asset,
                    "side": side,
                    "size": TRADE_SIZE,
                    "price": price,
                    "token_id": token_id,
                    "reasons": reasons,
                    "opened_at": datetime.now(timezone.utc).isoformat(),
                    "order_resp": str(resp),
                    "latency_ms": round(latency * 1000),
                }
                self.open_positions[market_id] = trade

            logger.info(
                f"LIVE TRADE #{self.total_trades}/{MAX_LIVE_TRADES}: "
                f"{side} {asset.upper()} ${TRADE_SIZE} @ {price:.2f} | "
                f"latency={latency*1000:.0f}ms | {reasons}"
            )
            return True

        except Exception as e:
            with self._lock:
                self.active_assets.discard(asset)
            logger.error(f"Live trade error: {e}")
            return False

    def _record_no_fill(self, asset, side, price):
        """Registra un intento sin liquidez."""
        with self._lock:
            self.no_fill_count += 1
            entry = {
                "asset": asset,
                "side": side,
                "price": price,
                "time": datetime.now(timezone.utc).strftime("%H:%M UTC"),
            }
            self.recent_no_fills.insert(0, entry)
            # Mantener solo los últimos N
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
            if win:
                pnl = TRADE_SIZE * ((1.0 - entry_price) / entry_price) * (1 - 0.07)
            else:
                pnl = -TRADE_SIZE
            result = {**trade, "outcome": outcome, "win": win, "pnl": pnl}
            self.results.append(result)
            self.today_pnl += pnl
            logger.info(
                f"LIVE RESULT: {'WIN' if win else 'LOSS'} "
                f"{side} {trade['asset'].upper()} | "
                f"PnL={pnl:+.2f} | entry={entry_price:.2f}"
            )

    def get_stats(self) -> dict:
        with self._lock:
            wins = [r for r in self.results if r["win"]]
            losses = [r for r in self.results if not r["win"]]
            total_pnl = sum(r["pnl"] for r in self.results)
            best = max((r["pnl"] for r in self.results), default=0)
            worst = min((r["pnl"] for r in self.results), default=0)

            # Cash balance real del CLOB
            cash_balance = 0.0
            try:
                from order_executor import get_balance
                cash_balance = get_balance()
            except Exception:
                pass

            # By asset stats
            by_asset = {}
            for r in self.results:
                a = r.get("asset", "unknown")
                if a not in by_asset:
                    by_asset[a] = {"total": 0, "wins": 0}
                by_asset[a]["total"] += 1
                if r["win"]:
                    by_asset[a]["wins"] += 1

            return {
                "total_trades": self.total_trades,
                "completed": len(self.results),
                "wins": len(wins),
                "losses": len(losses),
                "win_rate": len(wins) / len(self.results) * 100 if self.results else 0,
                "total_pnl": total_pnl,
                "pnl": total_pnl,
                "roi": (total_pnl / (len(self.results) * TRADE_SIZE) * 100) if self.results else 0,
                "best_trade": best,
                "worst_trade": worst,
                "open_count": len(self.open_positions),
                "open_positions": list(self.open_positions.values()),
                "results": self.results,
                "recent_trades": list(reversed(self.results[-10:])),
                "max_trades": MAX_LIVE_TRADES,
                "done": self.total_trades >= MAX_LIVE_TRADES,
                "cash_balance": cash_balance,
                "no_fill_count": self.no_fill_count,
                "blocked_count": self.blocked_count,
                "recent_no_fills": list(self.recent_no_fills[:10]),
                "by_asset": by_asset,
                "balance": cash_balance,
                "today_pnl": self.today_pnl,
                "day_start_balance": self.day_start_balance,
                "drawdown_paused": self._drawdown_paused,
            }


_live_trader = LiveTrader()

def get_live_trader():
    return _live_trader