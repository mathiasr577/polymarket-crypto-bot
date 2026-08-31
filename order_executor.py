"""
Real order executor — only used when PAPER_TRADING=false

31-ago-2026 (sugerido por la otra IA, verificado contra el código fuente
real de py-clob-client-v2 antes de implementar — nunca contra la
respuesta de un FAK real todavía, ver USE_FAK_ORDERS abajo): agregado
place_order_fak(), que reemplaza el flujo viejo (orden LIMIT → esperar
55s → cancel_order() → resolver ambigüedad con get_order()) por una
orden de mercado FAK (Fill-And-Kill): se llena lo que hay disponible
hasta el precio tope al instante, cancela el resto automáticamente. Sin
espera de 55s, sin cancelación ambigua — ese problema entero deja de
existir porque el CLOB resuelve todo en una sola llamada.

place_order() (LIMIT, con la espera de 55s) se deja intacta como
fallback — ver USE_FAK_ORDERS en live_trader_v2.py para el flag que
decide cuál se usa.
"""
import logging
import time
from config import PRIVATE_KEY, FUNDER, CHAIN_ID, CLOB_HOST

logger = logging.getLogger(__name__)

# Esto es un piso/techo de SEGURIDAD (evitar fat-fingers / precios
# corruptos), no una decisión de estrategia — esa banda le corresponde a
# cada signal_engine (v1: 0.45-0.80; v2: banda barata <0.55, potencialmente
# hasta ~0.20). Antes este archivo tenía su propio MIN_PRICE=0.25 que
# coincidía por casualidad con v1 pero hubiera bloqueado en silencio los
# trades de v2 en la banda barata si algún día se conecta a plata real —
# encontrado en revisión de código, corregido antes de que importara.
MAX_PRICE = 0.98
MIN_PRICE = 0.02

_client = None

def get_client():
    global _client
    if _client is not None:
        return _client
    try:
        from py_clob_client_v2 import ClobClient
        _client = ClobClient(
            host=CLOB_HOST,
            chain_id=CHAIN_ID,
            key=PRIVATE_KEY,
            funder=FUNDER,
            signature_type=3,
        )
        creds = _client.create_or_derive_api_key()
        _client.set_api_creds(creds)
        logger.info("CLOB client initialized")
    except Exception as e:
        logger.error(f"CLOB init error: {e}")
        _client = None
    return _client


def place_order(token_id: str, price: float, size: float, side: str = "BUY",
                alt_token_id: str = None, max_slippage_pct: float = 0.03) -> dict:
    """
    Place LIMIT order with max_slippage_pct tolerance (default 3%, same as
    before — pasar un valor más chico donde los centavos de precio importan
    más, ej. precios altos donde el margen esperado por trade es de pocos
    centavos y un 3% se lo come; ver live_trader_v2.py TIGHT_SLIPPAGE_PCT).
    size = dollars to spend (e.g. 5.0 = $5)
    Leaves order open for 55s max — Polymarket cancels at close if not filled.
    """
    client = get_client()
    if not client:
        return {"error": "No CLOB client"}

    if price > MAX_PRICE or price < MIN_PRICE:
        logger.warning(f"Scanner price {price:.2f} out of range — skipping")
        return {"error": f"scanner price out of range: {price:.2f}"}

    limit_price = round(min(price * (1 + max_slippage_pct), MAX_PRICE), 2)
    # size is dollars, convert to shares
    shares = round(size / price, 2)

    try:
        from py_clob_client_v2.clob_types import OrderArgsV2, OrderPayload
        logger.info(f"LIMIT order: BUY {shares} shares @ {limit_price:.2f} (${size:.2f}, scanner={price:.2f}, token={token_id[:20]}...)")

        order_args = OrderArgsV2(
            token_id=token_id,
            price=limit_price,
            size=shares,
            side=side,
        )
        resp = client.create_and_post_order(order_args)
        logger.info(f"Order response: {resp}")

        status = resp.get("status", "")
        order_id = resp.get("orderID", "")

        if status == "matched":
            logger.info(f"LIMIT order filled immediately ✅ @ {limit_price:.2f}")
            return resp

        if status == "live" and order_id:
            logger.info(f"Order live — waiting up to 55s for fill...")
            time.sleep(55)
            try:
                cancel_resp = client.cancel_order(OrderPayload(orderID=order_id))
                logger.info(f"Order cancelled after timeout: {cancel_resp}")
            except Exception as ce:
                logger.error(
                    f"⚠️ Cancel lanzó una excepción para orden {order_id} — "
                    f"estado de fill DESCONOCIDO (puede haberse llenado o no): {ce}. "
                    f"Revisar manualmente en Polymarket."
                )
                return {"error": "cancel raised exception, fill status unknown", "unknown_fill": True, "order_id": order_id}

            # cancel_order() NO lanza excepción cuando la orden ya se llenó —
            # devuelve 200 OK con un cuerpo que dice explícitamente
            # "order can't be found - already canceled or matched" dentro de
            # not_canceled. Confirmado contra una respuesta real de producción
            # (04 ago 2026) donde esto correspondía a una orden que SÍ se había
            # llenado con plata real. Solo se confirma "no se llenó" cuando el
            # order_id aparece explícitamente en "canceled".
            canceled_ids = cancel_resp.get("canceled") if isinstance(cancel_resp, dict) else None
            if canceled_ids and order_id in canceled_ids:
                return {"error": "limit not filled, cancelled"}

            logger.error(
                f"⚠️ No se pudo confirmar la cancelación de la orden {order_id} — "
                f"estado de fill DESCONOCIDO: {cancel_resp}. Consultando get_order antes de rendirse..."
            )
            # 25-ago-2026: cruzando 44 unknown_fill de un día contra el
            # historial real de Polymarket, las 44 SÍ se habían llenado
            # completas — el "desconocido" es casi siempre en realidad un
            # llenado que terminó de completarse justo antes de que
            # llegara nuestro cancel. client.get_order(order_id) se
            # verificó contra 9 respuestas reales de producción (25-ago-
            # 2026) — siempre devuelve status/size_matched/original_size/
            # price de forma consistente, y en las 9 confirmó exactamente
            # lo que después aparecía en el historial real de Polymarket.
            # Se usa acá para resolver la ambigüedad con datos reales en
            # vez de asumir el peor caso siempre.
            order_status = None
            try:
                if hasattr(client, "get_order"):
                    order_status = client.get_order(order_id)
                    logger.info(f"🔍 get_order({order_id[:20]}...) diagnóstico: {order_status}")
            except Exception as ge:
                logger.warning(f"🔍 get_order diagnóstico falló: {type(ge).__name__}: {ge}")

            if isinstance(order_status, dict):
                gstatus = str(order_status.get("status", "")).upper()
                try:
                    size_matched = float(order_status.get("size_matched") or 0)
                    original_size = float(order_status.get("original_size") or 0)
                except (TypeError, ValueError):
                    size_matched, original_size = 0.0, 0.0
                fill_price = order_status.get("price")

                if gstatus == "MATCHED" and size_matched > 0 and fill_price:
                    cost = round(size_matched * float(fill_price), 6)
                    logger.info(
                        f"✅ get_order confirmó fill real (antes DESCONOCIDO): "
                        f"{size_matched}/{original_size} @ {fill_price} (orden {order_id})"
                    )
                    return {
                        "status": "matched",
                        "takingAmount": str(size_matched),
                        "makingAmount": str(cost),
                        "orderID": order_id,
                        "resolved_via": "get_order_after_ambiguous_cancel",
                    }
                if gstatus in ("CANCELED", "CANCELLED") or size_matched == 0:
                    logger.info(f"get_order confirmó que la orden {order_id} NO se llenó (status={gstatus}).")
                    return {"error": "limit not filled, cancelled (confirmed via get_order)"}
                logger.warning(
                    f"get_order devolvió algo no reconocido para {order_id}: "
                    f"status={gstatus} size_matched={size_matched} original_size={original_size} — "
                    f"tratando como DESCONOCIDO por seguridad."
                )

            return {"error": "cancel not confirmed, fill status unknown", "unknown_fill": True, "order_id": order_id}

        return {"error": f"unexpected status: {status}"}

    except Exception as e:
        logger.warning(f"Limit order failed: {e} — skipping")
        return {"error": str(e)}


def place_order_fak(token_id: str, price: float, size: float, side: str = "BUY",
                     max_slippage_pct: float = 0.03) -> dict:
    """
    Orden de mercado FAK (Fill-And-Kill): se llena lo que haya disponible
    hasta price_cap al instante, el resto se cancela automáticamente —
    sin la espera de 55s ni la cancelación ambigua de place_order().
    size = dólares a gastar (igual semántica que place_order).

    price_cap se pasa explícito (no se deja en 0/"auto-calculado") porque
    la doc de MarketOrderArgsV2 no dice con certeza qué hace el
    auto-cálculo — usamos el mismo techo de slippage que ya veníamos
    calculando a mano, así el comportamiento de riesgo no cambia, solo
    cambia CÓMO se ejecuta.

    31-ago-2026: implementado y verificado contra el código fuente de
    py-clob-client-v2 (create_market_order/post_order comparten el mismo
    endpoint y el mismo parser de respuesta que las órdenes LIMIT), pero
    todavía SIN una respuesta real de producción de un FAK para confirmar
    la forma exacta del payload (a diferencia del fix de get_order, que sí
    se verificó contra 9 respuestas reales antes de confiar en él) — por
    eso el logging de la respuesta cruda es más agresivo acá, para poder
    confirmarlo con los primeros fills reales antes de asumir que
    _extract_fill_info() la interpreta bien.
    """
    client = get_client()
    if not client:
        return {"error": "No CLOB client"}

    if price > MAX_PRICE or price < MIN_PRICE:
        logger.warning(f"Scanner price {price:.2f} out of range — skipping")
        return {"error": f"scanner price out of range: {price:.2f}"}

    price_cap = round(min(price * (1 + max_slippage_pct), MAX_PRICE), 2)

    try:
        from py_clob_client_v2 import MarketOrderArgs, OrderType, Side
        logger.info(
            f"FAK order: BUY ${size:.2f} @ price_cap {price_cap:.2f} "
            f"(scanner={price:.2f}, token={token_id[:20]}...)"
        )

        order_args = MarketOrderArgs(
            token_id=token_id,
            amount=size,
            side=Side.BUY if side == "BUY" else Side.SELL,
            price=price_cap,
            order_type=OrderType.FAK,
        )
        resp = client.create_and_post_market_order(
            order_args=order_args,
            order_type=OrderType.FAK,
        )
        # Log agresivo a propósito (ver docstring) — necesitamos varias
        # respuestas reales antes de confiar en que _extract_fill_info()
        # la interpreta igual que a las órdenes LIMIT.
        logger.info(f"🔍 FAK raw response (verificando forma real): {resp}")
        return resp

    except Exception as e:
        logger.warning(f"FAK order failed: {e} — skipping")
        return {"error": str(e)}


_BALANCE_FIELDS = ("balance", "collateral", "amount", "value")


def get_balance() -> float:
    """
    get_balance_allowance() devuelve un dict, no un número — float(raw)
    directo fallaba SIEMPRE (no de forma intermitente), lo que significa que
    el circuit breaker de drawdown nunca pudo tomar un balance válido desde
    que se implementó. El schema exacto no está documentado en este repo,
    así que se prueban las claves más probables; si ninguna aparece, se
    loguea el dict completo para poder confirmar la clave correcta con los
    logs de Railway, y se cae a 0.0 (mismo comportamiento anterior, pero
    ahora visible y con reintento en vez de silencioso).
    """
    client = get_client()
    if not client:
        return 0.0
    try:
        from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType
        raw = client.get_balance_allowance(
            params=BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        )
        if isinstance(raw, dict):
            for key in _BALANCE_FIELDS:
                if key in raw:
                    try:
                        return float(raw[key]) / 1e6
                    except (TypeError, ValueError):
                        continue
            logger.error(f"get_balance_allowance devolvió un dict sin claves reconocidas: {raw}")
            return 0.0
        return float(raw) / 1e6
    except Exception as e:
        logger.error(f"Balance error: {e}")
        return 0.0