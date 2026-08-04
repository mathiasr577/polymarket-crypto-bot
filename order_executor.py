"""
Real order executor — only used when PAPER_TRADING=false
Uses LIMIT order with 3% slippage tolerance.
Leaves order open for 55s — Polymarket cancels automatically at market close.
"""
import logging
import time
from config import PRIVATE_KEY, FUNDER, CHAIN_ID, CLOB_HOST

logger = logging.getLogger(__name__)

MAX_PRICE = 0.80
MIN_PRICE = 0.25

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
                alt_token_id: str = None) -> dict:
    """
    Place LIMIT order with 3% slippage tolerance.
    size = dollars to spend (e.g. 5.0 = $5)
    Leaves order open for 55s max — Polymarket cancels at close if not filled.
    """
    client = get_client()
    if not client:
        return {"error": "No CLOB client"}

    if price > MAX_PRICE or price < MIN_PRICE:
        logger.warning(f"Scanner price {price:.2f} out of range — skipping")
        return {"error": f"scanner price out of range: {price:.2f}"}

    limit_price = round(min(price * 1.03, MAX_PRICE), 2)
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
                f"estado de fill DESCONOCIDO: {cancel_resp}. Revisar manualmente en Polymarket."
            )
            return {"error": "cancel not confirmed, fill status unknown", "unknown_fill": True, "order_id": order_id}

        return {"error": f"unexpected status: {status}"}

    except Exception as e:
        logger.warning(f"Limit order failed: {e} — skipping")
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