import time
import threading
import requests
import logging
from collections import deque
import numpy as np
from config import PRICE_INTERVAL

logger = logging.getLogger(__name__)

class PriceFeed:
    def __init__(self, maxlen=200):
        self.prices = {
            "bitcoin": deque(maxlen=maxlen),
            "ethereum": deque(maxlen=maxlen),
        }
        self.timestamps = {
            "bitcoin": deque(maxlen=maxlen),
            "ethereum": deque(maxlen=maxlen),
        }
        # Order flow: (buy_volume, sell_volume) per tick
        self.order_flow = {
            "bitcoin": deque(maxlen=50),
            "ethereum": deque(maxlen=50),
        }
        self._lock = threading.Lock()
        self._running = False
        self._thread = None

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        logger.info("PriceFeed started")

    def stop(self):
        self._running = False

    def _loop(self):
        while self._running:
            try:
                self._fetch_prices()
                self._fetch_order_flow()
            except Exception as e:
                logger.error(f"PriceFeed error: {e}")
            time.sleep(PRICE_INTERVAL)

    def _fetch_prices(self):
        r = requests.get(
            "https://api.kraken.com/0/public/Ticker",
            params={"pair": "XBTUSD,ETHUSD"},
            timeout=10
        )
        r.raise_for_status()
        data = r.json().get("result", {})
        btc = float(list(data.get("XXBTZUSD", {}).get("c", [0]))[0])
        eth = float(list(data.get("XETHZUSD", {}).get("c", [0]))[0])
        now = time.time()
        with self._lock:
            if btc > 0:
                self.prices["bitcoin"].append(btc)
                self.timestamps["bitcoin"].append(now)
            if eth > 0:
                self.prices["ethereum"].append(eth)
                self.timestamps["ethereum"].append(now)
        logger.debug(f"BTC={btc} ETH={eth}")

    def _fetch_order_flow(self):
        """
        Fetch recent trades from Binance to calculate buy vs sell pressure.
        Buy pressure = trades initiated by buyer (taker side = buy)
        """
        pairs = {
            "bitcoin": "BTCUSDT",
            "ethereum": "ETHUSDT",
        }
        for asset, symbol in pairs.items():
            try:
                r = requests.get(
                    "https://api.binance.com/api/v3/aggTrades",
                    params={"symbol": symbol, "limit": 100},
                    timeout=8
                )
                if r.status_code != 200:
                    continue
                trades = r.json()
                buy_vol = 0.0
                sell_vol = 0.0
                for t in trades:
                    qty = float(t.get("q", 0))
                    # isBuyerMaker=True means seller initiated (sell pressure)
                    if t.get("m", False):
                        sell_vol += qty
                    else:
                        buy_vol += qty
                with self._lock:
                    self.order_flow[asset].append({
                        "buy": buy_vol,
                        "sell": sell_vol,
                        "ratio": buy_vol / (buy_vol + sell_vol) if (buy_vol + sell_vol) > 0 else 0.5,
                        "ts": time.time(),
                    })
            except Exception as e:
                logger.debug(f"Order flow {asset}: {e}")

    def get_indicators(self, asset: str) -> dict | None:
        with self._lock:
            prices = list(self.prices[asset])
            timestamps = list(self.timestamps[asset])
            flow_history = list(self.order_flow[asset])

        if len(prices) < 6:
            return None

        arr = np.array(prices, dtype=float)
        now = time.time()

        # --- Momentum: last 3 prices (90 seconds) ---
        recent = arr[-4:]  # last 4 prices = last ~90s
        changes = np.diff(recent)
        if all(c > 0 for c in changes):
            momentum = "up"
        elif all(c < 0 for c in changes):
            momentum = "down"
        else:
            momentum = "neutral"

        # --- Short-term price change (last 2 minutes) ---
        n_2min = min(4, len(arr))  # ~4 samples = 2 min
        pct_2min = (arr[-1] - arr[-n_2min]) / arr[-n_2min] if arr[-n_2min] != 0 else 0

        # --- Volatility (last 5 samples) ---
        n_vol = min(10, len(arr))
        vol = abs(arr[-1] - arr[-n_vol]) / arr[-n_vol] if arr[-n_vol] != 0 else 0

        # --- Order flow (last 3 ticks) ---
        buy_ratio = 0.5
        if flow_history:
            recent_flow = flow_history[-3:]
            avg_ratio = np.mean([f["ratio"] for f in recent_flow])
            buy_ratio = float(avg_ratio)

        # --- RSI short (6 period for fast signal) ---
        rsi = self._rsi(arr, 6) if len(arr) >= 7 else 50.0

        return {
            "price": float(arr[-1]),
            "momentum": momentum,
            "pct_2min": float(pct_2min),
            "volatility": float(vol),
            "buy_ratio": float(buy_ratio),   # >0.5 = buying pressure
            "rsi": float(rsi),
            "price_count": len(prices),
        }

    def get_latest(self, asset: str) -> float | None:
        with self._lock:
            if not self.prices[asset]:
                return None
            return self.prices[asset][-1]

    def _rsi(self, arr, period=6):
        if len(arr) < period + 1:
            return 50.0
        deltas = np.diff(arr)[-period:]
        gains = np.where(deltas > 0, deltas, 0).mean()
        losses = np.where(deltas < 0, -deltas, 0).mean()
        if losses == 0:
            return 100.0
        rs = gains / losses
        return 100 - (100 / (1 + rs))


_feed = PriceFeed()

def start_feed():
    _feed.start()

def get_feed():
    return _feed