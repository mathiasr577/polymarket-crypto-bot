"""
Feed de solo lectura contra la API real de FOMO (fomoapi.io) — rastreo
prospectivo de consenso entre traders top de memecoins, para un
proyecto DISTINTO del bot de Polymarket: la idea (9-sep-2026) es usar
esta señal para operar de forma independiente en Coinbase (DEX de
Solana via Jupiter, confirmado real y accesible), NO copiar 1:1 a
ninguna wallet — eso ya se descartó a propósito por ser fácil de
manipular con una wallet señuelo (ver conversación sobre la app FOMO
y "exit liquidity").

Verificado en vivo antes de escribir esto (no asumido de la doc):
- El endpoint /v2/tokens/activity que hace exactamente esto ya viene
  armado está MUERTO (stale desde 23-ago-2026, confirmado con una
  llamada real) — hay que reconstruirlo nosotros desde el stream de
  alertas en vivo.
- El WebSocket wss://api.fomoapi.io/ws/alerts?key=... SÍ es real y en
  tiempo real (realtime:true, delaySeconds:0) con la key gratis
  (realtime los primeros 7 días, después 15s de delay).

Señal: cuando >=CONSENSUS_MIN_TRADERS traders DISTINTOS compran el
mismo token dentro de CONSENSUS_WINDOW_MINUTES, se loguea el evento.
Todavía no se calcula qué pasó con el precio después — se agrega en
un segundo paso, una vez que se vea cuánto pasa esto y en qué chains
(Solana vs Base vs Robinhood tienen fuentes de precio distintas, no
tiene sentido construir las tres a ciegas).

Solo lectura/logging — no ejecuta ninguna orden real todavía.
"""
import json
import logging
import threading
import time
from collections import defaultdict, deque

import websocket
import psycopg2

from config import DATABASE_URL, FOMO_API_KEY

logger = logging.getLogger(__name__)

WS_URL = "wss://api.fomoapi.io/ws/alerts"
CONSENSUS_MIN_TRADERS = 3
CONSENSUS_WINDOW_MINUTES = 15
RECONNECT_DELAY_SEC = 5


class FomoFeed:
    def __init__(self):
        self._running = False
        self._thread = None
        self._conn = None
        self._connect_db()
        # token_address -> deque[(trader, ts_ms, usd_value)] — solo BUY,
        # para detectar consenso de compra (no vende, que es señal distinta)
        self._buys_by_token = defaultdict(deque)
        self._consensus_seen = set()  # token_address ya logueado en esta racha

    def _connect_db(self):
        if not DATABASE_URL:
            return
        try:
            self._conn = psycopg2.connect(DATABASE_URL)
            self._conn.autocommit = True
        except Exception as e:
            logger.error(f"FomoFeed DB connect error: {e}")
            self._conn = None

    def start(self):
        if not FOMO_API_KEY:
            logger.warning("FomoFeed: FOMO_API_KEY no configurada, no arranca")
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_forever, daemon=True)
        self._thread.start()
        logger.info("FomoFeed started")

    def stop(self):
        self._running = False

    def _run_forever(self):
        while self._running:
            try:
                url = f"{WS_URL}?key={FOMO_API_KEY}"
                ws = websocket.WebSocketApp(
                    url,
                    on_message=self._on_message,
                    on_error=lambda w, e: logger.warning(f"FomoFeed WS error: {e}"),
                    on_close=lambda w, code, msg: logger.warning(f"FomoFeed WS closed: {code} {msg}"),
                )
                ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as e:
                logger.error(f"FomoFeed run_forever error: {e}")
            if self._running:
                time.sleep(RECONNECT_DELAY_SEC)

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
        except Exception:
            return
        if data.get("type") != "alert":
            return
        try:
            self._handle_alert(data)
        except Exception as e:
            logger.debug(f"FomoFeed handle_alert error: {e}")

    def _handle_alert(self, alert: dict):
        token_address = alert.get("tokenAddress")
        if not token_address:
            return

        self._log_alert(alert)

        if alert.get("alertType") != "buy":
            return

        trader = alert.get("trader")
        ts_ms = alert.get("ts") or int(time.time() * 1000)
        usd_value = alert.get("usdValue") or 0

        dq = self._buys_by_token[token_address]
        dq.append((trader, ts_ms, usd_value))
        cutoff = ts_ms - CONSENSUS_WINDOW_MINUTES * 60 * 1000
        while dq and dq[0][1] < cutoff:
            dq.popleft()

        unique_traders = {t for t, _, _ in dq}
        if len(unique_traders) >= CONSENSUS_MIN_TRADERS:
            # evitar re-loguear el mismo consenso en cada compra adicional —
            # solo el momento en que se CRUZA el umbral por primera vez en
            # esta racha. Se resetea si el token sale de la ventana (dq vacío).
            key = (token_address, len(unique_traders))
            if token_address not in self._consensus_seen:
                self._consensus_seen.add(token_address)
                self._log_consensus_event(alert, dq, unique_traders)
        elif not dq:
            self._consensus_seen.discard(token_address)

    def _log_alert(self, alert: dict):
        if not self._conn:
            return
        try:
            with self._conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO fomo_alerts
                    (fomo_alert_id, trader, token, token_address, chain, chain_id,
                     alert_type, usd_value, text, exchange_ts_ms)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (fomo_alert_id) DO NOTHING
                """, (
                    alert.get("id"), alert.get("trader"), alert.get("token"),
                    alert.get("tokenAddress"), alert.get("chain"), alert.get("chainId"),
                    alert.get("alertType"), alert.get("usdValue"), alert.get("text"),
                    alert.get("ts"),
                ))
        except Exception as e:
            logger.debug(f"FomoFeed log_alert error: {e}")

    def _log_consensus_event(self, alert: dict, dq: deque, unique_traders: set):
        if not self._conn:
            return
        try:
            total_usd = sum(u for _, _, u in dq)
            with self._conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO fomo_consensus_events
                    (token, token_address, chain, unique_traders, trader_handles,
                     total_usd, window_minutes, first_alert_ts_ms, last_alert_ts_ms)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    alert.get("token"), alert.get("tokenAddress"), alert.get("chain"),
                    len(unique_traders), ",".join(sorted(unique_traders)),
                    total_usd, CONSENSUS_WINDOW_MINUTES, dq[0][1], dq[-1][1],
                ))
            logger.info(
                f"🔥 Consenso FOMO: {alert.get('token')} ({alert.get('chain')}) — "
                f"{len(unique_traders)} traders distintos en {CONSENSUS_WINDOW_MINUTES}min, ~${total_usd:.0f}"
            )
        except Exception as e:
            logger.debug(f"FomoFeed log_consensus error: {e}")


_feed = None


def start_fomo_feed():
    global _feed
    _feed = FomoFeed()
    _feed.start()
    return _feed


def get_fomo_feed():
    return _feed
