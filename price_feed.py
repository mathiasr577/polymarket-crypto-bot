"""
Price feed con ref_price tracking.
Guarda el precio al inicio de cada ventana de 5 minutos (múltiplo de 300s).
Ese es el precio de referencia que usa Polymarket para resolver el mercado.
"""
import time
import threading
import requests
import logging
from collections import deque
import numpy as np
from config import PRICE_INTERVAL

logger = logging.getLogger(__name__)

VOL_LOOKBACK_SAMPLES = 20   # ~10 min de historia a PRICE_INTERVAL=30s
MIN_VOL_SAMPLES = 6

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
        # Precio al inicio de cada ventana de 5 minutos
        # key: window_ts (múltiplo de 300), value: price
        self.window_ref_prices = {
            "bitcoin": {},
            "ethereum": {},
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
            "https://api.kraken.com/0/public/Ticker",
            params={"pair": "XBTUSD,ETHUSD"},
            timeout=10
        )
        r.raise_for_status()
        data = r.json().get("result", {})
        btc = float(list(data.get("XXBTZUSD", {}).get("c", [0]))[0])
        eth = float(list(data.get("XETHZUSD", {}).get("c", [0]))[0])
        now = time.time()

        # Calcular ventana actual
        window_ts = int(now // 300) * 300

        with self._lock:
            if btc > 0:
                self.prices["bitcoin"].append(btc)
                self.timestamps["bitcoin"].append(now)
                # Guardar precio de referencia si es el primero de esta ventana
                if window_ts not in self.window_ref_prices["bitcoin"]:
                    self.window_ref_prices["bitcoin"][window_ts] = btc
                    logger.info(f"BTC ref price for window {window_ts}: ${btc:,.2f}")
                # Limpiar ventanas viejas (más de 30 minutos)
                old = [k for k in self.window_ref_prices["bitcoin"] if k < window_ts - 1800]
                for k in old:
                    del self.window_ref_prices["bitcoin"][k]

            if eth > 0:
                self.prices["ethereum"].append(eth)
                self.timestamps["ethereum"].append(now)
                if window_ts not in self.window_ref_prices["ethereum"]:
                    self.window_ref_prices["ethereum"][window_ts] = eth
                    logger.info(f"ETH ref price for window {window_ts}: ${eth:,.2f}")
                old = [k for k in self.window_ref_prices["ethereum"] if k < window_ts - 1800]
                for k in old:
                    del self.window_ref_prices["ethereum"][k]

        logger.debug(f"BTC=${btc:,.2f} ETH=${eth:,.2f} window={window_ts}")

    def get_ref_price(self, asset: str, window_ts: int) -> float | None:
        """Obtener precio de referencia para una ventana específica."""
        with self._lock:
            return self.window_ref_prices[asset].get(window_ts)

    def get_current_window_ref(self, asset: str) -> float | None:
        """Precio de referencia de la ventana actual."""
        window_ts = int(time.time() // 300) * 300
        with self._lock:
            return self.window_ref_prices[asset].get(window_ts)

    def get_latest(self, asset: str) -> float | None:
        with self._lock:
            if not self.prices[asset]:
                return None
            return self.prices[asset][-1]

    def get_indicators(self, asset: str) -> dict | None:
        with self._lock:
            prices = list(self.prices[asset])

        if len(prices) < 4:
            return None

        arr = np.array(prices, dtype=float)

        # Momentum últimos 3 precios (~90s)
        recent = arr[-4:]
        changes = np.diff(recent)
        if all(c > 0 for c in changes):
            momentum = "up"
        elif all(c < 0 for c in changes):
            momentum = "down"
        else:
            momentum = "neutral"

        # % cambio últimos 2 min (~4 muestras)
        n = min(4, len(arr))
        pct_2min = (arr[-1] - arr[-n]) / arr[-n] if arr[-n] != 0 else 0

        # Volatilidad
        n_vol = min(10, len(arr))
        vol = abs(arr[-1] - arr[-n_vol]) / arr[-n_vol] if arr[-n_vol] != 0 else 0

        # RSI rápido (6 períodos)
        rsi = self._rsi(arr, 6) if len(arr) >= 7 else 50.0

        # Ref price de la ventana actual
        window_ts = int(time.time() // 300) * 300
        with self._lock:
            ref_price = self.window_ref_prices[asset].get(window_ts)

        return {
            "price": float(arr[-1]),
            "momentum": momentum,
            "pct_2min": float(pct_2min),
            "volatility": float(vol),
            "rsi": float(rsi),
            "ref_price": ref_price,
            "price_count": len(prices),
        }

    def get_volatility_per_sqrt_sec(self, asset: str) -> float | None:
        """Volatilidad realizada de retornos log, normalizada por sqrt(segundo).

        Se usa para convertir un delta de precio crudo en un z-score ajustado
        por el tiempo restante hasta el cierre del mercado.
        """
        with self._lock:
            prices = list(self.prices[asset])[-VOL_LOOKBACK_SAMPLES:]

        if len(prices) < MIN_VOL_SAMPLES:
            return None

        arr = np.array(prices, dtype=float)
        if np.any(arr <= 0):
            return None

        log_returns = np.diff(np.log(arr))
        sigma_sample = float(np.std(log_returns, ddof=1))
        if sigma_sample <= 0:
            return None

        return sigma_sample / (PRICE_INTERVAL ** 0.5)

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