"""
Cliente del websocket público de trades de Bybit (futuros perpetuos
lineales BTCUSDT/ETHUSDT) — feed de solo lectura, mismo espíritu que
chainlink_feed.py: no participa en NINGUNA decisión de trading todavía,
solo alimenta shadow_logger.py para validar si el order flow (trades que
efectivamente ocurrieron, no el order book) predice algo sobre hacia dónde
se mueve el TWAP, antes de confiar en la idea.

Origen: sugerencia de una IA externa consultada (ver conversación,
15-ago-2026) — "Order Flow Imbalance" (OFI) de trades agresivos (taker),
distinto de inferir dirección del order book (que un paper académico
citado en la misma conversación mostró que solo acierta ~59% del lado
real). Acá no se infiere nada: Bybit expone explícitamente qué lado fue
el agresor (`S`: "Buy"/"Sell") en cada trade.

Verificado en vivo contra el servidor real antes de escribir este archivo
(15-ago-2026, no se confió en la documentación sin probarla — mismo
criterio que con el RTDS de Polymarket): esquema exacto confirmado,
`T`(ms)/`s`/`S`/`v`/`p`, llega a varios mensajes por segundo.

    wss://stream.bybit.com/v5/public/linear
    {"req_id":"x","op":"subscribe","args":["publicTrade.BTCUSDT","publicTrade.ETHUSDT"]}
    ping cada 20s: {"req_id":"x","op":"ping"}

    {"topic":"publicTrade.ETHUSDT","type":"snapshot","ts":...,
     "data":[{"T":1786822585000,"s":"ETHUSDT","S":"Buy","v":"0.01","p":"1883.66", ...}]}

OFI (Order Flow Imbalance) = (compras_agresivas - ventas_agresivas) en
dólares, sobre una ventana móvil corta (1s/5s/15s/30s), normalizado por el
volumen total de esa ventana. Mide quién está cruzando el spread para
conseguir ejecución YA — información distinta de "el precio subió".
"""
import json
import logging
import threading
import time
from collections import deque, defaultdict

import websocket

logger = logging.getLogger(__name__)

BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/linear"
PING_INTERVAL_SEC = 20
STALE_THRESHOLD_SEC = 30  # ver _watchdog_loop en chainlink_feed.py, mismo patrón

SYMBOLS = ["BTCUSDT", "ETHUSDT"]
SYMBOL_TO_ASSET = {"BTCUSDT": "bitcoin", "ETHUSDT": "ethereum"}

MAX_TRADE_AGE_SEC = 60  # no hace falta guardar más que la ventana más larga que usamos (30s) + margen


class OrderFlowFeed:
    def __init__(self):
        self._lock = threading.Lock()
        # self._trades[asset] = deque of (ts_sec, side "Buy"/"Sell", notional_usd)
        self._trades = {a: deque() for a in SYMBOL_TO_ASSET.values()}
        self._connected = False
        self._reconnect_count = -1
        self._last_message_time = None
        self._ws = None
        self._running = False
        self._thread = None

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._run_forever, daemon=True)
        self._thread.start()
        threading.Thread(target=self._watchdog_loop, daemon=True).start()
        logger.info("OrderFlowFeed (Bybit) started")

    def stop(self):
        self._running = False
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
            if connected and last_msg and (time.time() - last_msg > STALE_THRESHOLD_SEC):
                logger.warning(
                    f"OrderFlowFeed watchdog: sin mensajes hace {time.time()-last_msg:.0f}s "
                    f"estando 'conectado' — forzando reconexión"
                )
                try:
                    self._ws.close()
                except Exception as e:
                    logger.debug(f"Watchdog close error: {e}")

    def _run_forever(self):
        backoff = 1
        while self._running:
            connect_started_at = time.time()
            try:
                self._ws = websocket.WebSocketApp(
                    BYBIT_WS_URL,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self._ws.run_forever()
            except Exception as e:
                logger.error(f"OrderFlowFeed connection error: {e}")

            with self._lock:
                self._connected = False
            if not self._running:
                return

            if time.time() - connect_started_at > 60:
                backoff = 1
            logger.warning(f"OrderFlowFeed disconnected — reconnecting in {backoff}s")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)

    def _on_open(self, ws):
        with self._lock:
            self._connected = True
            self._reconnect_count += 1
            reconnects = self._reconnect_count
        sub = {"req_id": "sub1", "op": "subscribe", "args": [f"publicTrade.{s}" for s in SYMBOLS]}
        ws.send(json.dumps(sub))
        logger.info(f"OrderFlowFeed connected & subscribed (reconnect #{reconnects})")
        threading.Thread(target=self._ping_loop, args=(ws,), daemon=True).start()

    def _ping_loop(self, ws):
        while self._running and ws is self._ws:
            try:
                ws.send(json.dumps({"req_id": "ping", "op": "ping"}))
            except Exception:
                return
            time.sleep(PING_INTERVAL_SEC)

    def _on_error(self, ws, error):
        logger.error(f"OrderFlowFeed WS error: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        logger.warning(f"OrderFlowFeed WS closed: {close_status_code} {close_msg}")
        with self._lock:
            self._connected = False

    def _on_message(self, ws, message):
        try:
            self._handle_message(message)
        except Exception as e:
            logger.error(f"OrderFlowFeed _on_message crash on {message!r}: {e!r}")

    def _handle_message(self, message):
        try:
            data = json.loads(message)
        except Exception:
            return
        if not isinstance(data, dict):
            return

        topic = data.get("topic")
        if not topic or not topic.startswith("publicTrade."):
            return

        items = data.get("data")
        if not isinstance(items, list):
            return

        now = time.time()
        with self._lock:
            self._last_message_time = now
            for item in items:
                if not isinstance(item, dict):
                    continue
                symbol = item.get("s")
                asset = SYMBOL_TO_ASSET.get(symbol)
                side = item.get("S")
                if not asset or side not in ("Buy", "Sell"):
                    continue
                try:
                    price = float(item.get("p"))
                    size = float(item.get("v"))
                    ts_ms = item.get("T")
                    ts_sec = ts_ms / 1000.0 if ts_ms else now
                except (TypeError, ValueError):
                    continue

                notional = price * size
                dq = self._trades[asset]
                dq.append((ts_sec, side, notional))

                cutoff = now - MAX_TRADE_AGE_SEC
                while dq and dq[0][0] < cutoff:
                    dq.popleft()

    def get_ofi(self, asset: str, window_s: float) -> dict:
        """Order Flow Imbalance sobre los últimos window_s segundos:
        (compras_agresivas - ventas_agresivas) en USD, normalizado por el
        volumen total de la ventana. None si no hay trades en la ventana."""
        now = time.time()
        cutoff = now - window_s
        with self._lock:
            trades = [t for t in self._trades.get(asset, ()) if t[0] >= cutoff]
            connected = self._connected
            last_msg = self._last_message_time

        buy = sum(n for _, s, n in trades if s == "Buy")
        sell = sum(n for _, s, n in trades if s == "Sell")
        total = buy + sell
        lag_ms = (now - last_msg) * 1000 if last_msg else None

        return {
            "ofi": (buy - sell) / total if total > 0 else None,
            "buy_notional": round(buy, 2),
            "sell_notional": round(sell, 2),
            "n_trades": len(trades),
            "feed_connected": connected,
            "feed_lag_ms": lag_ms,
        }


_feed = OrderFlowFeed()


def start_order_flow_feed():
    _feed.start()


def get_order_flow_feed():
    return _feed
