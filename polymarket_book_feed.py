"""
Feed de solo lectura al libro de órdenes real de Polymarket (CLOB market
channel) — mismo espíritu que chainlink_feed.py/order_flow_feed.py: no
participa en NINGUNA decisión de trading todavía, solo alimenta la
descomposición de ejecución para investigar favorite (ver shadow_logger.py,
log_execution_snapshot / ExecutionDecompositionScheduler).

Origen (31-ago-2026): sugerencia de una IA externa consultada — separar,
para cada señal de favorite, cuánto del edge se pierde en el delay de 250ms
del taker vs en el spread vs en el fee, y simular fills maker (post-only)
contra el libro real en vez de asumir "tocó mi precio = me llenó".

Formato de suscripción y payload verificados EN VIVO (31-ago-2026, no solo
contra la doc — ver docs.polymarket.com/developers/CLOB/websocket/
market-channel, que en un punto no coincidía con lo real: los eventos
price_change SÍ traen best_bid/best_ask por defecto, sin necesitar
"custom_feature_enabled"):

    wss://ws-subscriptions-clob.polymarket.com/ws/market
    {"assets_ids": ["<token_id>", ...], "type": "market"}

    # snapshot completo al suscribirse (o al reconectar)
    {"market": "0x...", "asset_id": "...", "timestamp": "<ms>",
     "hash": "...", "bids": [{"price":"0.08","size":"33343.4"}, ...],
     "asks": [{"price":"0.09","size":"163939.58"}, ...]}
    # (nota: no siempre trae "event_type" en el snapshot inicial)

    # actualizaciones incrementales, llegan MUY seguido cerca del cierre
    # de ventana (868 mensajes en 12s en la prueba real) — el handler NO
    # debe hacer trabajo pesado.
    {"market": "0x...", "event_type": "price_change",
     "price_changes": [
        {"asset_id": "...", "price": "0.99", "size": "2788.56",
         "side": "SELL", "hash": "...", "best_bid": "0.98", "best_ask": "0.99"},
        ...
     ], "timestamp": "<ms>"}

Suscripción dinámica: el set de token_ids relevante cambia cada ~5min
(rotan las ventanas). set_tokens() solo reconecta si el set deseado
cambió de verdad — evita reconectar en cada tick por las dudas.

Alcance deliberado de v1: profundidad completa del libro (bids/asks) se
guarda tal cual llega en el último snapshot "book", pero NO se reconstruye
de forma incremental nivel por nivel con cada price_change (para eso
haría falta parchear cada nivel exacto, más trabajo del que se justifica
para el v1). best_bid/best_ask/spread SÍ se mantienen en tiempo real
(vienen directo en cada price_change) — es lo que hace falta para medir
delay_drag y spread_drag, el resto puede esperar a una v2 si hace falta.

1-sep-2026 (sugerido por la otra IA, verificado contra la doc real antes
de implementar): se agrega el tape de trades (evento last_trade_price) y
el tamaño en el mejor bid/ask — hacían falta para dos cosas del diseño
del backtest de maker: (a) clasificar fills de forma conservadora
(certain/possible/no_fill necesita saber si pasó volumen real, no solo
si el precio tocó el nivel) y (b) estimar el pool de rebate por mercado
y la dilución contra otros makers (tamaño en reposo = liquidez de otros
market makers). Formato de last_trade_price verificado contra la doc
(no se pudo confirmar en vivo todavía — varios intentos de conexión
cayeron en ventanas sin actividad — así que el parseo es defensivo y
loguea el payload crudo las primeras veces, mismo criterio que con FAK):

    {"topic": "market", "type": "last_trade_price", "payload": {
        "market": "0x...", "tokenId": "...", "price": "0.08",
        "size": "219.217767", "feeRateBps": "0", "side": "SELL",
        "timestamp": "<ms>", "transactionHash": "0x..."}}

Nota el envoltorio distinto (top-level "type"+"payload", no "event_type"
plano como book/price_change) — parseo defensivo para los dos formatos.
"""
import json
import logging
import threading
import time

import websocket

logger = logging.getLogger(__name__)

BOOK_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
PING_INTERVAL_SEC = 15
STALE_THRESHOLD_SEC = 30


MAX_TRADE_AGE_SEC = 120  # no hace falta guardar más que un par de veces la ventana de foto (~3s) más margen
_LAST_TRADE_SAMPLE_LOGGED = {"done": False}
_UNKNOWN_EVENTS_LOGGED = {}  # event_type/type -> cuántas veces ya se logueó (máx 2 c/u)


class PolymarketBookFeed:
    def __init__(self):
        self._lock = threading.Lock()
        self._books = {}  # asset_id -> {best_bid, best_ask, bids, asks, updated_at}
        self._trades = {}  # asset_id -> list of {price, size, side, ts, tx_hash}, más nuevo al final
        self._desired_tokens = set()
        self._subscribed_tokens = set()
        self._connected = False
        self._last_message_time = None
        self._ws = None
        self._running = False
        self._thread = None
        self._reconnect_requested = threading.Event()

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._run_forever, daemon=True)
        self._thread.start()
        threading.Thread(target=self._watchdog_loop, daemon=True).start()
        logger.info("PolymarketBookFeed started")

    def stop(self):
        self._running = False
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass

    def set_tokens(self, token_ids):
        """Llamar en cada tick con el set de token_ids actualmente
        relevante (mercados abiertos que nos importan). Solo reconecta si
        cambió de verdad."""
        new_set = set(token_ids)
        with self._lock:
            if new_set == self._desired_tokens:
                return
            self._desired_tokens = new_set
        if new_set:
            self._reconnect_requested.set()
            if self._ws:
                try:
                    self._ws.close()
                except Exception:
                    pass

    def _watchdog_loop(self):
        while self._running:
            time.sleep(10)
            with self._lock:
                connected = self._connected
                last_msg = self._last_message_time
                has_tokens = bool(self._desired_tokens)
            if has_tokens and connected and last_msg and (time.time() - last_msg > STALE_THRESHOLD_SEC):
                logger.warning(
                    f"PolymarketBookFeed watchdog: sin mensajes hace {time.time()-last_msg:.0f}s "
                    f"estando 'conectado' — forzando reconexión"
                )
                try:
                    self._ws.close()
                except Exception:
                    pass

    def _run_forever(self):
        backoff = 1
        while self._running:
            with self._lock:
                tokens = list(self._desired_tokens)
            if not tokens:
                time.sleep(1)
                continue

            connect_started_at = time.time()
            try:
                self._ws = websocket.WebSocketApp(
                    BOOK_WS_URL,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self._ws.run_forever(ping_interval=PING_INTERVAL_SEC)
            except Exception as e:
                logger.error(f"PolymarketBookFeed connection error: {e}")

            with self._lock:
                self._connected = False
            if not self._running:
                return

            if time.time() - connect_started_at > 60:
                backoff = 1
            time.sleep(backoff)
            backoff = min(backoff * 2, 15)

    def _on_open(self, ws):
        with self._lock:
            self._connected = True
            tokens = list(self._desired_tokens)
            self._subscribed_tokens = set(tokens)
        try:
            ws.send(json.dumps({"assets_ids": tokens, "type": "market"}))
            logger.info(f"PolymarketBookFeed connected & suscripto a {len(tokens)} tokens")
        except Exception as e:
            logger.error(f"PolymarketBookFeed subscribe error: {e}")

    def _on_error(self, ws, error):
        logger.error(f"PolymarketBookFeed WS error: {error}")

    def _on_close(self, ws, code, msg):
        with self._lock:
            self._connected = False

    def _on_message(self, ws, message):
        try:
            self._handle_message(message)
        except Exception as e:
            logger.debug(f"PolymarketBookFeed _on_message error: {e!r}")

    def _handle_message(self, message):
        now = time.time()
        try:
            data = json.loads(message)
        except Exception:
            return
        with self._lock:
            self._last_message_time = now

        # Un mensaje puede venir como lista (varios eventos) o dict — visto
        # ambos en la prueba en vivo, se manejan los dos.
        events = data if isinstance(data, list) else [data]
        for ev in events:
            if not isinstance(ev, dict):
                continue
            event_type = ev.get("event_type")

            if event_type == "price_change":
                for pc in ev.get("price_changes", []) or []:
                    asset_id = pc.get("asset_id")
                    if not asset_id:
                        continue
                    bb = pc.get("best_bid")
                    ba = pc.get("best_ask")
                    with self._lock:
                        book = self._books.setdefault(asset_id, {})
                        if bb is not None:
                            book["best_bid"] = _to_float(bb)
                        if ba is not None:
                            book["best_ask"] = _to_float(ba)
                        book["updated_at"] = now

            elif "asset_id" in ev and ("bids" in ev or "asks" in ev):
                # snapshot completo (evento "book", a veces sin event_type)
                asset_id = ev.get("asset_id")
                bids = ev.get("bids") or []
                asks = ev.get("asks") or []
                best_bid, best_bid_size = _best_level(bids, pick_max=True)
                best_ask, best_ask_size = _best_level(asks, pick_max=False)
                with self._lock:
                    book = self._books.setdefault(asset_id, {})
                    book["bids"] = bids
                    book["asks"] = asks
                    if best_bid is not None:
                        book["best_bid"] = best_bid
                        book["best_bid_size"] = best_bid_size
                    if best_ask is not None:
                        book["best_ask"] = best_ask
                        book["best_ask_size"] = best_ask_size
                    book["updated_at"] = now

            elif ev.get("type") == "last_trade_price" or event_type == "last_trade_price":
                # ver docstring — envoltorio distinto (payload anidado),
                # y todavía sin verificar en vivo, log agresivo a propósito.
                # El log va ANTES de intentar parsear nada — si el nombre
                # real de algún campo no coincide con lo que asumimos acá,
                # igual queremos ver el payload crudo en vez de perdernos
                # el evento entero en silencio (bug real encontrado
                # 1-sep-2026: estaba después del chequeo de asset_id).
                if not _LAST_TRADE_SAMPLE_LOGGED["done"]:
                    logger.info(f"🔍 last_trade_price primera muestra real: {json.dumps(ev)[:500]}")
                    _LAST_TRADE_SAMPLE_LOGGED["done"] = True
                payload = ev.get("payload") if isinstance(ev.get("payload"), dict) else ev
                asset_id = payload.get("tokenId") or payload.get("asset_id")
                if not asset_id:
                    continue
                trade = {
                    "price": _to_float(payload.get("price")),
                    "size": _to_float(payload.get("size")),
                    "side": payload.get("side"),
                    "ts": now,
                    "tx_hash": payload.get("transactionHash"),
                }
                with self._lock:
                    dq = self._trades.setdefault(asset_id, [])
                    dq.append(trade)
                    cutoff = now - MAX_TRADE_AGE_SEC
                    while dq and dq[0]["ts"] < cutoff:
                        dq.pop(0)

            elif event_type or ev.get("type"):
                # Catch-all: cualquier event_type/type que no sea book,
                # price_change o last_trade_price — si la doc está mal de
                # nuevo (ya pasó una vez con best_bid_ask) esto es lo que
                # nos permite verlo en vez de perderlo en silencio.
                _seen = _UNKNOWN_EVENTS_LOGGED.setdefault(event_type or ev.get("type"), 0)
                if _seen < 2:
                    logger.info(f"🔍 event_type/type no reconocido: {json.dumps(ev)[:500]}")
                    _UNKNOWN_EVENTS_LOGGED[event_type or ev.get("type")] = _seen + 1

    def get_book(self, asset_id: str) -> dict:
        """None-safe. Devuelve el último estado conocido del libro para
        ese token_id, o dict vacío/stale si no hay data fresca."""
        with self._lock:
            book = self._books.get(asset_id)
        if not book:
            return {"feed_connected": False, "best_bid": None, "best_ask": None, "spread": None}
        age = time.time() - book.get("updated_at", 0)
        connected = age < STALE_THRESHOLD_SEC
        bb = book.get("best_bid")
        ba = book.get("best_ask")
        spread = (ba - bb) if (bb is not None and ba is not None) else None
        return {
            "feed_connected": connected,
            "feed_lag_ms": age * 1000,
            "best_bid": bb if connected else None,
            "best_ask": ba if connected else None,
            "best_bid_size": book.get("best_bid_size") if connected else None,
            "best_ask_size": book.get("best_ask_size") if connected else None,
            "spread": spread if connected else None,
            "bids": book.get("bids") if connected else None,
            "asks": book.get("asks") if connected else None,
        }

    def get_new_trades(self, asset_id: str, since_ts: float) -> list:
        """Trades reales (evento last_trade_price) para ese token desde
        since_ts (epoch seconds, time.time()) — usar el ts del último
        drenado para no relogear los mismos trades en cada foto."""
        with self._lock:
            trades = list(self._trades.get(asset_id, []))
        return [t for t in trades if t["ts"] > since_ts]


def _best_level(levels: list, pick_max: bool) -> tuple:
    """Encuentra el mejor precio (máximo para bids, mínimo para asks) Y
    su tamaño — los arrays de bids/asks no vienen en un orden garantizado
    (visto en vivo: a veces ascendente, a veces descendente), así que no
    alcanza con tomar el primer elemento."""
    best_price, best_size = None, None
    for level in levels or []:
        p = _to_float(level.get("price"))
        if p is None:
            continue
        if best_price is None or (p > best_price if pick_max else p < best_price):
            best_price = p
            best_size = _to_float(level.get("size"))
    return best_price, best_size


def _to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


_feed = PolymarketBookFeed()


def start_book_feed():
    _feed.start()


def get_book_feed():
    return _feed
