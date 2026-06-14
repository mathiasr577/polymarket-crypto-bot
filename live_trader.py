"""
Live trader — ejecuta órdenes reales en Polymarket.
Limitado a MAX_LIVE_TRADES trades para el test inicial.
"""
import logging
import threading
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

MAX_LIVE_TRADES = 4
TRADE_SIZE = 5.0

class LiveTrader:
    def __init__(self):
        self._lock = threading.Lock()
        self.total_trades = 0
        self.open_positions = {}
        self.results = []
        self._client = None
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

    def can_trade(self) -> bool:
        with self._lock:
            return (self._client is not None and self.total_trades < MAX_LIVE_TRADES and len(self.open_positions) < 2)

    def get_balance(self) -> float:
        try:
            from order_executor import get_balance
            return get_balance()
        except Exception as e:
            logger.error(f"Balance error: {e}")
            return 0.0

    def open_trade(self, market_id, title, asset, side, price, token_id, reasons, indicators) -> bool:
        with self._lock:
            if self.total_trades >= MAX_LIVE_TRADES:
                logger.info(f"Live test complete — {MAX_LIVE_TRADES} trades done")
                return False
            if market_id in self.open_positions:
                return False
            if len(self.open_positions) >= 2:
                return False
        try:
            import time
            t0 = time.time()
            from order_executor import place_order
            resp = place_order(token_id=token_id, price=round(price, 2), size=TRADE_SIZE, side="BUY")
            latency = time.time() - t0
            if "error" in resp:
                logger.error(f"Order failed: {resp['error']}")
                return False
            with self._lock:
                self.total_trades += 1
                trade = {"market_id": market_id, "title": title, "asset": asset, "side": side, "size": TRADE_SIZE, "price": price, "token_id": token_id, "reasons": reasons, "opened_at": datetime.now(timezone.utc).isoformat(), "order_resp": str(resp), "latency_ms": round(latency * 1000)}
                self.open_positions[market_id] = trade
            logger.info(f"LIVE TRADE #{self.total_trades}/{MAX_LIVE_TRADES}: {side} {asset.upper()} $5 @ {price:.2f} | latency={latency*1000:.0f}ms | {reasons}")
            return True
        except Exception as e:
            logger.error(f"Live trade error: {e}")
            return False

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
            logger.info(f"LIVE RESULT: {'WIN' if win else 'LOSS'} {side} {trade['asset'].upper()} | PnL={pnl:+.2f} | entry={entry_price:.2f}")

    def get_stats(self) -> dict:
        with self._lock:
            wins = [r for r in self.results if r["win"]]
            total_pnl = sum(r["pnl"] for r in self.results)
            return {"total_trades": self.total_trades, "completed": len(self.results), "wins": len(wins), "losses": len(self.results) - len(wins), "win_rate": len(wins) / len(self.results) * 100 if self.results else 0, "total_pnl": total_pnl, "open_count": len(self.open_positions), "open_positions": list(self.open_positions.values()), "results": self.results, "max_trades": MAX_LIVE_TRADES, "done": self.total_trades >= MAX_LIVE_TRADES}

_live_trader = LiveTrader()

def get_live_trader():
    return _live_trader
