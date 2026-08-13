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
EWMA_HALFLIFE_MINUTES = 4   # a los 4 min, una muestra pesa la mitad que la actual

TREND_LOOKBACK_MINUTES = 12  # horizonte corto: reacciona rápido a reversiones genuinas
MIN_TREND_SAMPLES = 10

# Horizonte largo: detecta tendencias sostenidas de varias horas que el corto
# no puede ver (solo mira 12 min). Agregado el 10 ago tras dos días seguidos
# (8 y 9 de agosto) donde las apuestas DOWN perdieron sistemáticamente
# (25% de win rate ambos días) mientras UP rendía bien (62-80%) — la señal
# corta no alcanza a capturar una tendencia que viene sostenida por horas.
TREND_LOOKBACK_MINUTES_LONG = 90
MIN_TREND_SAMPLES_LONG = 60   # no confiar en la señal larga hasta tener ~30 min reales

class PriceFeed:
    def __init__(self, maxlen=400):
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
        # Último precio visto dentro de cada ventana de 5 minutos (se va
        # actualizando en cada fetch). Se usa para el shadow-mode logger
        # (chainlink_feed.py / shadow_logger.py) como aproximación en vivo
        # al precio de "cierre" — separado de window_ref_prices porque ese
        # dict solo guarda el primero (open) y signal_engine.py depende de
        # que siga siendo así.
        self.window_last_prices = {
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
                self.window_last_prices["bitcoin"][window_ts] = btc
                # Limpiar ventanas viejas (más de 30 minutos)
                old = [k for k in self.window_ref_prices["bitcoin"] if k < window_ts - 1800]
                for k in old:
                    del self.window_ref_prices["bitcoin"][k]
                old = [k for k in self.window_last_prices["bitcoin"] if k < window_ts - 1800]
                for k in old:
                    del self.window_last_prices["bitcoin"][k]

            if eth > 0:
                self.prices["ethereum"].append(eth)
                self.timestamps["ethereum"].append(now)
                if window_ts not in self.window_ref_prices["ethereum"]:
                    self.window_ref_prices["ethereum"][window_ts] = eth
                    logger.info(f"ETH ref price for window {window_ts}: ${eth:,.2f}")
                self.window_last_prices["ethereum"][window_ts] = eth
                old = [k for k in self.window_ref_prices["ethereum"] if k < window_ts - 1800]
                for k in old:
                    del self.window_ref_prices["ethereum"][k]
                old = [k for k in self.window_last_prices["ethereum"] if k < window_ts - 1800]
                for k in old:
                    del self.window_last_prices["ethereum"][k]

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

    def get_window_last_price(self, asset: str, window_ts: int) -> float | None:
        """Último precio de Kraken visto dentro de una ventana de 5 min
        específica — usado por el shadow-mode logger como aproximación al
        precio de cierre (no confundir con get_current_window_ref, que
        siempre devuelve el precio de APERTURA de la ventana)."""
        with self._lock:
            return self.window_last_prices[asset].get(window_ts)

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

        Pondera los retornos con EWMA (estilo RiskMetrics) en vez de un
        promedio uniforme sobre la ventana: una muestra ruidosa de hace 30s
        pesa más que una de hace 10 min, así que el estimador reacciona más
        rápido a cambios de régimen de volatilidad dentro de la misma
        ventana de lookback.

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
        n = len(log_returns)
        if n == 0:
            return None

        halflife_samples = EWMA_HALFLIFE_MINUTES * 60 / PRICE_INTERVAL
        decay = 0.5 ** (1 / halflife_samples)
        # weights[i] corresponde a log_returns[i]; el más reciente (último
        # índice) pesa 1, decayendo hacia atrás en el tiempo.
        weights = decay ** np.arange(n - 1, -1, -1)
        weights = weights / weights.sum()

        variance = float(np.sum(weights * log_returns ** 2))
        if variance <= 0:
            return None

        sigma_sample = float(np.sqrt(variance))
        return sigma_sample / (PRICE_INTERVAL ** 0.5)

    def get_trend_drift_per_sec(self, asset: str) -> float | None:
        """Pendiente de una regresión lineal de log(precio) vs tiempo sobre
        los últimos TREND_LOOKBACK_MINUTES minutos (horizonte corto, ~12 min).
        Devuelve el drift estimado en retorno fraccional por segundo
        (positivo = tendencia alcista, negativo = bajista).
        """
        return self._trend_drift(asset, TREND_LOOKBACK_MINUTES, MIN_TREND_SAMPLES)

    def get_trend_drift_per_sec_long(self, asset: str) -> float | None:
        """Igual que get_trend_drift_per_sec pero sobre un horizonte largo
        (~90 min), para detectar tendencias sostenidas de varias horas que
        el horizonte corto no puede ver."""
        return self._trend_drift(asset, TREND_LOOKBACK_MINUTES_LONG, MIN_TREND_SAMPLES_LONG)

    def _trend_drift(self, asset: str, lookback_minutes: float, min_samples: int) -> float | None:
        """Usa una regresión (no solo los dos extremos) para que un par de
        muestras ruidosas en el borde de la ventana no dominen la estimación.
        """
        lookback_samples = int(lookback_minutes * 60 / PRICE_INTERVAL)
        with self._lock:
            prices = list(self.prices[asset])[-lookback_samples:]
            timestamps = list(self.timestamps[asset])[-lookback_samples:]

        if len(prices) < min_samples:
            return None

        arr = np.array(prices, dtype=float)
        if np.any(arr <= 0):
            return None

        ts = np.array(timestamps, dtype=float)
        t_rel = ts - ts[0]
        if t_rel[-1] <= 0:
            return None

        slope, _ = np.polyfit(t_rel, np.log(arr), 1)
        return float(slope)

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