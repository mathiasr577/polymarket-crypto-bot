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
        self._msg_count = 0

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
        self._msg_count += 1
        if self._msg_count % 2000 == 0:
            try:
                self._cleanup_empty_tokens()
            except Exception as e:
                logger.debug(f"FomoFeed cleanup error: {e}")
        try:
            data = json.loads(message)
        except Exception:
            return
        msg_type = data.get("type")
        if msg_type == "welcome":
            # 9-sep-2026: al reconectar, el server reenvía un buffer de
            # alertas recientes marcadas "replay": true — sin esto, esas
            # compras se vuelven a contar en _buys_by_token y podrían
            # inflar el total_usd de un consenso (el conteo de traders
            # únicos no se ve afectado, pero el total sí). Más simple y
            # seguro resetear la ventana en cada reconexión — como mucho
            # se pierden unos segundos de historial, algo aceptable.
            self._buys_by_token.clear()
            self._consensus_seen.clear()
            logger.info(f"FomoFeed reconectado: {data}")
            # 9-sep-2026, encontrado en revisión: FOMO reconoce la key de
            # forma inconsistente — a veces (visto en producción, no solo
            # en pruebas manuales) el welcome viene con realtime:false,
            # como si no hubiera key (delay de 60s, feed demo). Sin este
            # chequeo, esa degradación quedaría en silencio por TODA la
            # vida de esa conexión (podrían ser horas o días) — nada tira
            # error, solo se ve peor calidad de dato. Si pasa, se fuerza
            # una reconexión en vez de aceptarla.
            if not data.get("realtime"):
                logger.warning(
                    f"FomoFeed: welcome sin realtime (key no reconocida esta vez) — forzando reconexión: {data}"
                )
                try:
                    ws.close()
                except Exception:
                    pass
            return
        if msg_type != "alert":
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
            # esta racha.
            if token_address not in self._consensus_seen:
                self._consensus_seen.add(token_address)
                self._log_consensus_event(alert, dq, unique_traders)
        else:
            # 9-sep-2026, bug real encontrado en revisión: esto antes estaba
            # en un `elif not dq`, que nunca podía ser cierto porque recién
            # se le acaba de hacer append al item actual (dq nunca queda
            # vacío en este punto) — el token quedaba marcado como "ya visto"
            # para siempre, sin importar cuánto se enfriara después. Ahora
            # se re-arma en cuanto el conteo de traders únicos cae por
            # debajo del umbral, así una racha nueva semanas después sí
            # se vuelve a loguear como evento nuevo.
            self._consensus_seen.discard(token_address)

    def _reconnect_db(self):
        """9-sep-2026: la conexión a Postgres se abría una sola vez al
        arrancar, sin reintento — el proxy de Railway ya se vio flaky
        (timeouts) varias veces en este proyecto. Sin esto, un solo corte
        de conexión mataba el logging para siempre hasta el próximo
        restart del proceso, silenciosamente (los except solo loguean en
        debug)."""
        try:
            if self._conn:
                self._conn.close()
        except Exception:
            pass
        self._connect_db()

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
            logger.debug(f"FomoFeed log_alert error: {e} — reconectando DB")
            self._reconnect_db()

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
            logger.debug(f"FomoFeed log_consensus error: {e} — reconectando DB")
            self._reconnect_db()

    def _cleanup_empty_tokens(self):
        """9-sep-2026: sin esto, _buys_by_token acumula una entrada por
        cada token distinto que se vio ALGUNA VEZ, para siempre — en
        varios días corriendo sobre 'cualquier memecoin', eso es
        potencialmente miles de keys que ya no importan. Se llama cada
        2000 mensajes desde _on_message (no en cada uno, no hace falta)."""
        empty = [tok for tok, dq in self._buys_by_token.items() if not dq]
        for tok in empty:
            del self._buys_by_token[tok]


_feed = None


def start_fomo_feed():
    global _feed
    _feed = FomoFeed()
    _feed.start()
    return _feed


def get_fomo_feed():
    return _feed
