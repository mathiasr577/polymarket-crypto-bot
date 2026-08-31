"""
Feed de solo lectura contra la API pública de Kalshi (KXBTC15M/KXETH15M) —
mismo espíritu que order_flow_feed.py y chainlink_feed.py: no participa en
NINGUNA decisión de trading todavía, solo alimenta shadow_logger.py para
validar si el lean de Kalshi predice algo sobre Polymarket ANTES de
confiar en la idea, mismo criterio que con lead/OFI/presión.

Origen (31-ago-2026): sugerencia de una IA externa consultada, después de
recuperar el scanner conjunto Kalshi/Polymarket (`arb_scan.joint_snapshots`
en el proyecto `kalshi-paper-trading`) y correr un primer backtest de
lead-lag sobre ~172K puntos por asset: el lean de Kalshi (yes_ask - 0.5)
predice el lean FUTURO de Polymarket por encima de lo que Polymarket ya
sabe de sí mismo, con significancia real (t=12-15) hasta ~60s de horizonte,
decayendo después. Efecto real pero chico en magnitud — vale la pena
agregarlo como candidato a 4ta confirmación, no reemplaza nada.

Deliberadamente un feed PROPIO en este repo (no una dependencia cruzada
contra la DB del otro proyecto) — así este bot no depende de que
kalshi-paper-trading siga vivo (ya se cayó una vez sin que nadie lo
notara durante 2 semanas). Reutiliza la lógica de polling ya verificada
en kalshi-paper-trading/src/kalshi_client.py (API pública, sin auth,
verificado ahí en agosto 2026), no la wsocket-based porque Kalshi no
ofrece esa cadencia para estos mercados — es REST, se poll-ea.
"""
import logging
import threading
import time
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

BASE = "https://api.elections.kalshi.com/trade-api/v2"
SERIES_MAP = {"bitcoin": "KXBTC15M", "ethereum": "KXETH15M"}
POLL_INTERVAL_SEC = 10
STALE_THRESHOLD_SEC = 60  # 6 polls perdidos seguidos


def _parse_ts(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


class KalshiFeed:
    def __init__(self):
        self._lock = threading.Lock()
        self._latest = {}  # asset -> dict con yes_ask, no_ask, close_time, strike, fetched_at
        self._running = False
        self._thread = None

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._run_forever, daemon=True)
        self._thread.start()
        logger.info("KalshiFeed started")

    def stop(self):
        self._running = False

    def _run_forever(self):
        while self._running:
            for asset, series in SERIES_MAP.items():
                try:
                    self._poll_one(asset, series)
                except Exception as e:
                    logger.warning(f"KalshiFeed poll error ({asset}): {e}")
            time.sleep(POLL_INTERVAL_SEC)

    def _poll_one(self, asset: str, series: str):
        resp = requests.get(
            f"{BASE}/markets",
            params={"series_ticker": series, "status": "open", "limit": 10},
            timeout=8,
        )
        resp.raise_for_status()
        markets = resp.json().get("markets", [])
        if not markets:
            return
        markets.sort(key=lambda m: m["close_time"])
        m = markets[0]

        def _price(field):
            v = m.get(field)
            return float(v) if v not in (None, "") else None

        with self._lock:
            self._latest[asset] = {
                "ticker": m.get("ticker"),
                "yes_ask": _price("yes_ask_dollars"),
                "no_ask": _price("no_ask_dollars"),
                "close_time": _parse_ts(m["close_time"]) if m.get("close_time") else None,
                "strike": m.get("floor_strike"),
                "fetched_at": time.time(),
            }

    def get_snapshot(self, asset: str) -> dict:
        """None-safe. yes_ask/lean/seconds_left en None si no hay data
        fresca (feed caído o recién arrancando) — igual patrón que
        chainlink_feed.get_snapshot()'s feed_connected."""
        with self._lock:
            data = self._latest.get(asset)

        if not data:
            return {"feed_connected": False, "yes_ask": None, "lean": None, "seconds_left": None}

        age = time.time() - data["fetched_at"]
        connected = age < STALE_THRESHOLD_SEC
        yes_ask = data["yes_ask"] if connected else None
        lean = (yes_ask - 0.5) if yes_ask is not None else None
        seconds_left = None
        if connected and data["close_time"]:
            seconds_left = (data["close_time"] - datetime.now(timezone.utc)).total_seconds()

        return {
            "feed_connected": connected,
            "feed_lag_ms": age * 1000,
            "yes_ask": yes_ask,
            "no_ask": data["no_ask"] if connected else None,
            "lean": lean,
            "strike": data["strike"],
            "seconds_left": seconds_left,
            "ticker": data["ticker"],
        }


_feed = KalshiFeed()


def start_kalshi_feed():
    _feed.start()


def get_kalshi_feed():
    return _feed
