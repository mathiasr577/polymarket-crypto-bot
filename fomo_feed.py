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
mismo token dentro de CONSENSUS_WINDOW_MINUTES Y el USD combinado
supera CONSENSUS_MIN_TOTAL_USD, se loguea el evento.

10-sep-2026: con el umbral inicial (3 traders, sin mínimo de USD)
salían ~20 eventos/hora, TODOS con exactamente 3 traders — ruido. Se
subió a 4 traders + $50k combinado. Y se agregó tracking de precio del
token a +15min/+1h/+6h/+24h vía DexScreener (gratis, sin key, cubre
todas las chains) — sin esto no hay forma de saber si el consenso
predice algo. Los checkpoints se llenan contra la DB (no memoria)
para sobrevivir reconexiones en la ventana de 24h.

Solo lectura/logging — no ejecuta ninguna orden real todavía.
"""
import json
import logging
import threading
import time
from collections import defaultdict, deque

import websocket
import psycopg2
import requests

from config import DATABASE_URL, FOMO_API_KEY

logger = logging.getLogger(__name__)

WS_URL = "wss://api.fomoapi.io/ws/alerts"
# 10-sep-2026: subido de 3 a 4 traders + mínimo de USD combinado. Con
# el umbral de 3 salían ~20 eventos/hora y TODOS tenían exactamente 3
# traders (nunca más) — señal claramente demasiado floja, era ruido.
CONSENSUS_MIN_TRADERS = 4
CONSENSUS_MIN_TOTAL_USD = 50000
CONSENSUS_WINDOW_MINUTES = 15
RECONNECT_DELAY_SEC = 5
# 13-sep-2026, bug real encontrado en chequeo de salud: la conexión se
# quedó "viva" (sin on_error ni on_close) pero dejó de recibir mensajes
# por 92 minutos seguidos — el loop de reconexión de _run_forever nunca
# se enteró porque nada tiró excepción. Se confirmó en vivo que la API
# de FOMO seguía funcionando bien en paralelo (script standalone
# recibiendo mensajes normal en ese mismo momento), así que el corte fue
# del socket/hilo local, no del servidor. Con una tasa normal de
# cientos/hora, no recibir nada en 3 minutos es inequívocamente anormal.
WATCHDOG_TIMEOUT_SEC = 180

# Chequeos de precio post-evento (DexScreener, gratis, multi-chain).
DEXSCREENER = "https://api.dexscreener.com/latest/dex/tokens"
PRICE_CHECKPOINTS = [("price_15m", 15 * 60), ("price_1h", 3600),
                     ("price_6h", 6 * 3600), ("price_24h", 24 * 3600)]


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
        self._ws = None
        self._last_msg_ts = time.time()

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
        self._watchdog_thread = threading.Thread(target=self._watchdog_loop, daemon=True)
        self._watchdog_thread.start()
        logger.info("FomoFeed started")

    def stop(self):
        self._running = False

    def _run_forever(self):
        while self._running:
            try:
                url = f"{WS_URL}?key={FOMO_API_KEY}"
                self._last_msg_ts = time.time()  # resetear al reconectar — no marcar stale antes de que llegue nada
                ws = websocket.WebSocketApp(
                    url,
                    on_message=self._on_message,
                    on_error=lambda w, e: logger.warning(f"FomoFeed WS error: {e}"),
                    on_close=lambda w, code, msg: logger.warning(f"FomoFeed WS closed: {code} {msg}"),
                )
                self._ws = ws
                ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as e:
                logger.error(f"FomoFeed run_forever error: {e}")
            if self._running:
                time.sleep(RECONNECT_DELAY_SEC)

    def _watchdog_loop(self):
        """Ver comentario en WATCHDOG_TIMEOUT_SEC — la única forma real
        encontrada de detectar un socket colgado que nunca dispara
        on_error/on_close por sí solo."""
        while self._running:
            time.sleep(30)
            idle = time.time() - self._last_msg_ts
            if idle > WATCHDOG_TIMEOUT_SEC:
                logger.warning(f"FomoFeed watchdog: sin mensajes hace {idle:.0f}s — forzando reconexión")
                try:
                    if self._ws:
                        self._ws.close()
                except Exception as e:
                    logger.debug(f"FomoFeed watchdog close error: {e}")
                self._last_msg_ts = time.time()  # evitar cierres repetidos mientras reconecta

    def _on_message(self, ws, message):
        self._last_msg_ts = time.time()
        self._msg_count += 1
        if self._msg_count % 2000 == 0:
            try:
                self._cleanup_empty_tokens()
            except Exception as e:
                logger.debug(f"FomoFeed cleanup error: {e}")
        if self._msg_count % 400 == 0:
            # ~cada pocos minutos dado el volumen de alertas — llena los
            # precios post-evento (15m/1h/6h/24h) de los consensos.
            try:
                self._update_pending_prices()
            except Exception as e:
                logger.debug(f"FomoFeed pending-prices error: {e}")
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

        # 13-sep-2026, bug real encontrado en chequeo de salud: limpiar
        # _buys_by_token en el "welcome" (9-sep) evita que el buffer VIEJO
        # se acumule sin límite entre reconexiones, pero no evita que el
        # buffer de replay de la conexión NUEVA se procese como si fueran
        # compras en vivo — visto en producción disparando el MISMO
        # consenso dos veces (mismos traders, mismos timestamps de origen)
        # con un crossed_at y un price_at_event distintos y falsos, porque
        # el precio se pide "ahora" para un evento que en realidad pasó
        # hace horas. Sin esto, cada reconexión durante un corte largo del
        # feed puede seguir generando filas fantasma en
        # fomo_consensus_events con precios capturados en el momento
        # equivocado — justo lo que el tracking de precio necesita evitar.
        if alert.get("replay"):
            return

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
        total_usd = sum(u for _, _, u in dq)
        if len(unique_traders) >= CONSENSUS_MIN_TRADERS and total_usd >= CONSENSUS_MIN_TOTAL_USD:
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

    def _dexscreener_price(self, token_address: str):
        """(priceUsd, liquidezUsd) del par más líquido, o (None, None).
        Gratis, sin key, multi-chain (solana/base/bsc/robinhood/eth)."""
        try:
            r = requests.get(f"{DEXSCREENER}/{token_address}", timeout=6)
            if r.status_code != 200:
                return None, None
            pairs = r.json().get("pairs") or []
            if not pairs:
                return None, None
            best = max(pairs, key=lambda p: (p.get("liquidity") or {}).get("usd") or 0)
            price = best.get("priceUsd")
            liq = (best.get("liquidity") or {}).get("usd")
            return (float(price) if price else None, float(liq) if liq else None)
        except Exception:
            return None, None

    def _log_consensus_event(self, alert: dict, dq: deque, unique_traders: set):
        if not self._conn:
            return
        try:
            total_usd = sum(u for _, _, u in dq)
            price_now, liq_now = self._dexscreener_price(alert.get("tokenAddress"))
            with self._conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO fomo_consensus_events
                    (token, token_address, chain, unique_traders, trader_handles,
                     total_usd, window_minutes, first_alert_ts_ms, last_alert_ts_ms,
                     price_at_event, liq_usd_at_event)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    alert.get("token"), alert.get("tokenAddress"), alert.get("chain"),
                    len(unique_traders), ",".join(sorted(unique_traders)),
                    total_usd, CONSENSUS_WINDOW_MINUTES, dq[0][1], dq[-1][1],
                    price_now, liq_now,
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

    def _update_pending_prices(self):
        """Llena price_15m/1h/6h/24h de eventos de consenso cuya edad ya
        pasó cada checkpoint. Va contra la DB (no memoria) para que
        sobreviva reconexiones — la ventana es de 24h. Se marca
        price_done cuando ya se llenó el checkpoint de 24h."""
        if not self._conn:
            return
        try:
            with self._conn.cursor() as cur:
                cur.execute("""
                    SELECT id, token_address, EXTRACT(EPOCH FROM (now() - crossed_at)),
                           price_15m, price_1h, price_6h, price_24h
                    FROM fomo_consensus_events
                    WHERE price_done = FALSE AND crossed_at > now() - interval '30 hours'
                    ORDER BY crossed_at
                    LIMIT 40
                """)
                rows = cur.fetchall()
        except Exception as e:
            logger.debug(f"FomoFeed pending-prices query error: {e}")
            self._reconnect_db()
            return

        for rid, addr, age_sec, p15, p1, p6, p24 in rows:
            have = {"price_15m": p15, "price_1h": p1, "price_6h": p6, "price_24h": p24}
            due = [(col, sec) for col, sec in PRICE_CHECKPOINTS if age_sec >= sec and have[col] is None]
            if not due:
                continue
            price, _ = self._dexscreener_price(addr)
            if price is None:
                continue
            try:
                with self._conn.cursor() as cur:
                    for col, _sec in due:
                        cur.execute(f"UPDATE fomo_consensus_events SET {col}=%s WHERE id=%s", (price, rid))
                    # si ya llenamos el de 24h (o el evento tiene >26h y no
                    # se pudo antes), cerrar
                    if any(col == "price_24h" for col, _ in due) or age_sec > 26 * 3600:
                        cur.execute("UPDATE fomo_consensus_events SET price_done=TRUE WHERE id=%s", (rid,))
            except Exception as e:
                logger.debug(f"FomoFeed pending-prices update error [{rid}]: {e}")
                self._reconnect_db()


_feed = None


def start_fomo_feed():
    global _feed
    _feed = FomoFeed()
    _feed.start()
    return _feed


def get_fomo_feed():
    return _feed
