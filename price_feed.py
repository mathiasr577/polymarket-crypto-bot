import time
import threading
import requests
import logging
from collections import deque
from datetime import datetime, timezone
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
                self._fetch()
            except Exception as e:
                logger.error(f"PriceFeed error: {e}")
            time.sleep(PRICE_INTERVAL)

    def _fetch(self):
        r = requests.get(
            "https://api.binance.com/api/v3/ticker/price",
            params={"symbols": '["BTCUSDT","ETHUSDT"]'},
            timeout=10
        )
        r.raise_for_status()
        data = {item["symbol"]: float(item["price"]) for item in r.json()}
        now = time.time()
        with self._lock:
            self.prices["bitcoin"].append(data["BTCUSDT"])
            self.timestamps["bitcoin"].append(now)
            self.prices["ethereum"].append(data["ETHUSDT"])
            self.timestamps["ethereum"].append(now)
        logger.debug(f"BTC={data['BTCUSDT']} ETH={data['ETHUSDT']}")

    def get_latest(self, asset):
        with self._lock:
            if not self.prices[asset]:
                return None
            return self.prices[asset][-1]

    def get_history(self, asset, n=50):
        with self._lock:
            return list(self.prices[asset])[-n:]

    def get_indicators(self, asset):
        with self._lock:
            prices = list(self.prices[asset])

        if len(prices) < 22:
            return None

        arr = np.array(prices, dtype=float)

        rsi = self._rsi(arr, 14)
        ema9 = self._ema(arr, 9)
        ema21 = self._ema(arr, 21)
        momentum = self._momentum(arr)

        n_vol = min(10, len(arr))
        vol = abs(arr[-1] - arr[-n_vol]) / arr[-n_vol] if arr[-n_vol] != 0 else 0

        return {
            "price": arr[-1],
            "rsi": rsi,
            "ema9": ema9[-1] if len(ema9) else None,
            "ema21": ema21[-1] if len(ema21) else None,
            "ema9_prev": ema9[-2] if len(ema9) > 1 else None,
            "ema21_prev": ema21[-2] if len(ema21) > 1 else None,
            "momentum": momentum,
            "volatility": vol,
            "price_count": len(prices),
        }

    def _rsi(self, arr, period=14):
        if len(arr) < period + 1:
            return 50.0
        deltas = np.diff(arr)[-period:]
        gains = np.where(deltas > 0, deltas, 0).mean()
        losses = np.where(deltas < 0, -deltas, 0).mean()
        if losses == 0:
            return 100.0
        rs = gains / losses
        return 100 - (100 / (1 + rs))

    def _ema(self, arr, period):
        if len(arr) < period:
            return arr
        k = 2 / (period + 1)
        ema = [arr[:period].mean()]
        for price in arr[period:]:
            ema.append(price * k + ema[-1] * (1 - k))
        return np.array(ema)

    def _momentum(self, arr, n=3):
        if len(arr) < n + 1:
            return "neutral"
        changes = [arr[-(i)] - arr[-(i+1)] for i in range(1, n+1)]
        if all(c < 0 for c in changes):
            return "down"
        if all(c > 0 for c in changes):
            return "up"
        return "neutral"


_feed = PriceFeed()

def start_feed():
    _feed.start()

def get_feed():
    return _feed