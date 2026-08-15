"""
Cliente RTDS de Polymarket (wss://ws-live-data.polymarket.com) — feed de
solo lectura, en paralelo al feed de Kraken existente (price_feed.py).

No participa en NINGUNA decisión de trading todavía: solo alimenta el
shadow-mode logger (shadow_logger.py) para poder comparar el precio de
referencia que Polymarket usa REALMENTE para resolver (TWAP de Chainlink)
contra el spot de Kraken que hoy usa el modelo en vivo (signal_engine.py).

Se suscribe a AMBAS ventanas TWAP (30s y 60s, topics crypto_prices_twap_thirty
y crypto_prices_twap_sixty) más el spot de Chainlink (crypto_prices_chainlink),
para BTC y ETH — sin asumir cuál ventana está activa en cada momento. Hubo un
dato sin verificar de forma independiente (una IA externa citó un tweet de
@PolymarketDevs afirmando que los mercados de 5 min pasan de TWAP 30s a 60s
el 14-ago-2026; no se pudo confirmar en ninguna fuente primaria ni en
búsquedas — ver conversación). En vez de apostar a esa lectura, este feed
guarda las dos ventanas y deja que el campo `window_s` de cada mensaje real
diga la verdad.

Formato de suscripción y payload verificados directamente contra
docs.polymarket.com/market-data/websocket/rtds y
docs.polymarket.com/market-data/chainlink-twap (no contra lo que dijo
ninguna IA externa):

    {"action": "subscribe", "subscriptions": [
        {"topic": "crypto_prices_twap_thirty", "type": "update",
         "filters": "{\"symbol\":\"btc/usd\"}"}
    ]}

    {"topic": "crypto_prices_twap_thirty", "type": "update",
     "timestamp": <ms>, "payload": {
        "symbol": "btc/usd", "value": 67234.5,
        "full_accuracy_value": "67234500000000000000000",
        "timestamp": <unix s, observación de Chainlink>, "window_s": 30}}

Importante: RTDS NO tiene replay/historial — solo entrega actualizaciones
hacia adelante desde que la conexión está abierta (documentado y confirmado
en la investigación previa). Por eso este feed corre 24/7 en su propio hilo
y va acumulando, por cada ventana de 5 min y cada asset, el primer valor
visto ("open") y el último valor visto ("last", se va actualizando) — el
"last" en el momento en que Polymarket marca el mercado como resuelto es
nuestra mejor aproximación en vivo al valor de cierre real, sin necesidad de
reconstruir nada después.
"""
import json
import logging
import threading
import time
from collections import defaultdict

import websocket

logger = logging.getLogger(__name__)

RTDS_URL = "wss://ws-live-data.polymarket.com"
PING_INTERVAL_SEC = 5
STALE_THRESHOLD_SEC = 30  # ver _watchdog_loop
WINDOW_SECONDS = 300
OLD_WINDOW_CUTOFF_SEC = 1800  # limpiar ventanas de más de 30 min, igual que price_feed.py

SYMBOLS = ["btc/usd", "eth/usd"]
ASSET_TO_SYMBOL = {"bitcoin": "btc/usd", "ethereum": "eth/usd"}
SYMBOL_TO_ASSET = {v: k for k, v in ASSET_TO_SYMBOL.items()}

TOPIC_TWAP30 = "crypto_prices_twap_thirty"
TOPIC_TWAP60 = "crypto_prices_twap_sixty"
TOPIC_SPOT = "crypto_prices_chainlink"


class ChainlinkFeed:
    def __init__(self):
        self._lock = threading.Lock()
        # último valor puntual visto por símbolo/kind ("twap_30"/"twap_60"/"spot")
        self._latest = defaultdict(dict)
        # por ventana de 5 min: open (primer valor visto) y last (más reciente)
        # self._window[30]["bitcoin"][window_ts] = {"open":..,"open_ts":..,"last":..,"last_ts":..}
        self._window = {30: defaultdict(dict), 60: defaultdict(dict)}
        # "presión TWAP": integral en el tiempo de (spot - twap60) dentro de
        # la ventana actual — no es lo mismo que spot_now - twap60_now (un
        # solo punto): si el spot lleva 20s pegado por encima del TWAP pesa
        # más que un toque de 500ms que ya volvió. Idea propuesta por una
        # IA externa consultada (ver conversación, 15-ago-2026), evaluada
        # como razonable y barata de construir con datos que ya recibimos
        # — todavía sin validar con datos reales, se guarda en shadow-mode
        # nada más por ahora, no se usa para ninguna decisión de trading.
        self._pressure = defaultdict(dict)
        self._connected = False
        self._reconnect_count = -1  # la primera conexión no cuenta como reconexión
        self._last_message_time = None
        self._ws = None
        self._running = False
        self._thread = None

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._run_forever, daemon=True)
        self._thread.start()
        threading.Thread(target=self._watchdog_loop, daemon=True).start()
        logger.info("ChainlinkFeed (RTDS) started")

    def _watchdog_loop(self):
        """Segunda capa de defensa, independiente del blindaje en
        _on_message: el ritmo normal observado es ~1 mensaje/seg. Si estamos
        "conectados" pero no llegó nada en STALE_THRESHOLD_SEC, algo mató la
        recepción sin pasar por on_close (visto en producción: quedaba
        congelado 15-90+ min). En vez de esperar a diagnosticar la causa
        exacta, forzamos el cierre para que _run_forever reconecte solo."""
        while self._running:
            time.sleep(10)
            with self._lock:
                connected = self._connected
                last_msg = self._last_message_time
            if connected and last_msg and (time.time() - last_msg > STALE_THRESHOLD_SEC):
                logger.warning(
                    f"ChainlinkFeed watchdog: sin mensajes hace {time.time()-last_msg:.0f}s "
                    f"estando 'conectado' — forzando reconexión"
                )
                try:
                    self._ws.close()
                except Exception as e:
                    logger.debug(f"Watchdog close error: {e}")

    def stop(self):
        self._running = False
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass

    # -- conexión --------------------------------------------------------

    def _build_subscriptions(self):
        # IMPORTANTE: NO se manda "filters" acá. Se probó en vivo contra
        # wss://ws-live-data.polymarket.com (13-ago-2026) y con
        # filters='{"symbol":"btc/usd"}' (el formato exacto que documenta
        # docs.polymarket.com) el servidor responde con topic="crypto_prices"
        # (el feed genérico, sin window_s/full_accuracy_value) en vez del
        # topic pedido — y con filters como dict plano no responde nada. Sin
        # filtro, en cambio, cada topic entrega el firehose completo de
        # todos los pares (btc, eth, sol, xrp, doge, bnb, hype, ...) con el
        # payload documentado correcto (confirmado con datos reales, no
        # asumido). Filtramos por símbolo del lado del cliente en
        # _on_message, que es barato dado el volumen (unos pocos mensajes
        # por segundo por topic).
        return [
            {"topic": TOPIC_TWAP30, "type": "update"},
            {"topic": TOPIC_TWAP60, "type": "update"},
            {"topic": TOPIC_SPOT, "type": "update"},
        ]

    def _run_forever(self):
        backoff = 1
        while self._running:
            connect_started_at = time.time()
            try:
                self._ws = websocket.WebSocketApp(
                    RTDS_URL,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self._ws.run_forever()
            except Exception as e:
                logger.error(f"ChainlinkFeed connection error: {e}")

            with self._lock:
                self._connected = False
            if not self._running:
                return

            # Si la conexión duró un rato (no fue un crash-loop inmediato),
            # resetear el backoff — si no, esto solo crecía para siempre
            # (1s, 2s, 4s... 30s) sin volver nunca a 1s, aunque cada
            # desconexión fuera un evento aislado horas después de la
            # anterior. Detectado revisando logs reales de producción.
            if time.time() - connect_started_at > 60:
                backoff = 1
            logger.warning(f"ChainlinkFeed disconnected — reconnecting in {backoff}s")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)

    def _on_open(self, ws):
        with self._lock:
            self._connected = True
            self._reconnect_count += 1
            reconnects = self._reconnect_count
        sub_msg = {"action": "subscribe", "subscriptions": self._build_subscriptions()}
        ws.send(json.dumps(sub_msg))
        logger.info(f"ChainlinkFeed connected & subscribed (reconnect #{reconnects})")
        threading.Thread(target=self._ping_loop, args=(ws,), daemon=True).start()

    def _ping_loop(self, ws):
        # RTDS pide un frame de texto literal "PING" cada 5s para mantener
        # viva la conexión (no es el ping/pong a nivel de protocolo WS).
        while self._running and ws is self._ws:
            try:
                ws.send("PING")
            except Exception:
                return
            time.sleep(PING_INTERVAL_SEC)

    def _on_error(self, ws, error):
        logger.error(f"ChainlinkFeed WS error: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        logger.warning(f"ChainlinkFeed WS closed: {close_status_code} {close_msg}")
        with self._lock:
            self._connected = False

    # -- mensajes ----------------------------------------------------------

    def _on_message(self, ws, message):
        # TODO cualquier excepción que se escape de este método se propaga
        # dentro del callback interno de websocket-client. Se observó en
        # producción (13/14-ago-2026): la conexión seguía "conectada"
        # (nunca disparaba on_close) pero dejaba de recibir datos frescos
        # durante 15-90+ minutos seguidos — self._latest quedaba congelado
        # en el mismo valor mientras twap*_open nunca se llenaba para la
        # ventana actual. Confirmado con una prueba en vivo de 7 minutos
        # (391 mensajes, ~330 cambios de valor, sin cortes) que el feed real
        # SÍ actualiza ~1/seg sin pausas — la causa tiene que ser algo local
        # que mata el hilo de lectura sin pasar por _on_close. Antes acá
        # solo se protegía el json.loads(); el resto del procesamiento
        # (ej. data.get(...) si data no es un dict) podía tirar una
        # excepción sin capturar. Ahora todo el cuerpo queda blindado.
        try:
            self._handle_message(message)
        except Exception as e:
            logger.error(f"ChainlinkFeed _on_message crash on {message!r}: {e!r}")

    def _handle_message(self, message):
        if message in ("PONG", "PING"):
            return
        try:
            data = json.loads(message)
        except Exception as e:
            logger.debug(f"ChainlinkFeed unparseable message: {message!r} ({e})")
            return

        if not isinstance(data, dict):
            return

        topic = data.get("topic")
        payload = data.get("payload")
        if not topic or not isinstance(payload, dict):
            return

        symbol = payload.get("symbol")
        asset = SYMBOL_TO_ASSET.get(symbol)
        if not asset:
            return

        if topic == TOPIC_TWAP30:
            kind, window_s = "twap_30", 30
        elif topic == TOPIC_TWAP60:
            kind, window_s = "twap_60", 60
        elif topic == TOPIC_SPOT:
            kind, window_s = "spot", None
        else:
            return

        value = payload.get("value")
        chainlink_ts = payload.get("timestamp")
        reported_window_s = payload.get("window_s", window_s)
        now = time.time()

        with self._lock:
            self._latest[symbol][kind] = {
                "value": value,
                "full_accuracy_value": payload.get("full_accuracy_value"),
                "chainlink_timestamp": chainlink_ts,
                "window_s": reported_window_s,
                "received_at": now,
            }
            self._last_message_time = now

            if window_s is not None and value is not None:
                window_ts = int(now // WINDOW_SECONDS) * WINDOW_SECONDS
                bucket = self._window[window_s][asset]
                w = bucket.setdefault(window_ts, {})
                if "open" not in w:
                    w["open"] = value
                    w["open_ts"] = chainlink_ts
                w["last"] = value
                w["last_ts"] = chainlink_ts
                w["last_window_s"] = reported_window_s

                old = [k for k in bucket if k < window_ts - OLD_WINDOW_CUTOFF_SEC]
                for k in old:
                    del bucket[k]

            # Presión: solo el spot y el TWAP60 mueven la divergencia que
            # nos interesa (spot - twap60). Se actualiza en cada mensaje de
            # cualquiera de los dos, usando el último valor conocido del otro.
            if kind in ("spot", "twap_60"):
                self._update_pressure(asset, symbol, now)

    def _update_pressure(self, asset, symbol, now):
        """Debe llamarse con self._lock ya tomado. Integral trapezoidal
        simple: asume que la divergencia se mantuvo constante en el último
        valor conocido desde la última actualización hasta ahora."""
        spot_entry = self._latest[symbol].get("spot")
        twap_entry = self._latest[symbol].get("twap_60")
        if not spot_entry or not twap_entry:
            return
        spot = spot_entry.get("value")
        twap60 = twap_entry.get("value")
        if spot is None or twap60 is None:
            return

        divergence = spot - twap60
        window_ts = int(now // WINDOW_SECONDS) * WINDOW_SECONDS
        bucket = self._pressure[asset]
        p = bucket.setdefault(window_ts, {"integral": 0.0, "last_update": None, "last_divergence": None, "samples": 0})
        if p["last_update"] is not None:
            dt = now - p["last_update"]
            prev = p["last_divergence"] if p["last_divergence"] is not None else divergence
            p["integral"] += prev * dt
        p["last_update"] = now
        p["last_divergence"] = divergence
        p["samples"] += 1

        old = [k for k in bucket if k < window_ts - OLD_WINDOW_CUTOFF_SEC]
        for k in old:
            del bucket[k]

    def get_pressure(self, asset: str, window_ts: int = None) -> dict:
        """Integral de (spot - twap60) desde la apertura de la ventana
        hasta ahora, más la divergencia puntual actual (para ver si la
        presión está creciendo o cerrándose). Vacío si no hay datos aún."""
        if window_ts is None:
            window_ts = int(time.time() // WINDOW_SECONDS) * WINDOW_SECONDS
        with self._lock:
            p = dict(self._pressure.get(asset, {}).get(window_ts, {}))
        if not p:
            return {"integral": None, "last_divergence": None, "samples": 0}
        return {
            "integral": p.get("integral"),
            "last_divergence": p.get("last_divergence"),
            "samples": p.get("samples", 0),
        }

    # -- lectura -------------------------------------------------------

    def get_snapshot(self, asset: str) -> dict:
        """Últimos valores puntuales conocidos para un asset, más metadata
        de confiabilidad del feed (para poder invalidar mercados con huecos
        de datos en vez de rellenarlos silenciosamente)."""
        symbol = ASSET_TO_SYMBOL.get(asset)
        with self._lock:
            data = dict(self._latest.get(symbol, {}))
            connected = self._connected
            reconnects = self._reconnect_count
            last_msg = self._last_message_time

        now = time.time()
        lag_ms = (now - last_msg) * 1000 if last_msg else None

        def _get(kind, field):
            entry = data.get(kind)
            return entry.get(field) if entry else None

        return {
            "chainlink_spot": _get("spot", "value"),
            "chainlink_spot_ts": _get("spot", "chainlink_timestamp"),
            "twap30_now": _get("twap_30", "value"),
            "twap30_now_ts": _get("twap_30", "chainlink_timestamp"),
            "twap30_window_s": _get("twap_30", "window_s"),
            "twap60_now": _get("twap_60", "value"),
            "twap60_now_ts": _get("twap_60", "chainlink_timestamp"),
            "twap60_window_s": _get("twap_60", "window_s"),
            "feed_connected": connected,
            "feed_reconnect_count": max(reconnects, 0),
            "feed_lag_ms": lag_ms,
        }

    def get_window_twap(self, asset: str, window_s: int, window_ts: int = None) -> dict:
        """open/last del TWAP (30 o 60) para la ventana de 5 min dada
        (por defecto la actual). Vacío si todavía no llegó ningún dato."""
        if window_ts is None:
            window_ts = int(time.time() // WINDOW_SECONDS) * WINDOW_SECONDS
        with self._lock:
            return dict(self._window.get(window_s, {}).get(asset, {}).get(window_ts, {}))


_feed = ChainlinkFeed()


def start_chainlink_feed():
    _feed.start()


def get_chainlink_feed():
    return _feed
