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
    Leaves order open for 55s max — Polymarket cancels at close if not filled.
    No risk of buying at wrong price since limit is enforced by the exchange.
    """
    client = get_client()
    if not client:
        return {"error": "No CLOB client"}

    if price > MAX_PRICE or price < MIN_PRICE:
        logger.warning(f"Scanner price {price:.2f} out of range — skipping")
        return {"error": f"scanner price out of range: {price:.2f}"}

    limit_price = round(min(price * 1.03, MAX_PRICE), 2)
    shares = round(size / price, 2)

    try:
        from py_clob_client_v2.clob_types import OrderArgsV2, OrderPayload
        logger.info(f"LIMIT order: BUY {shares} shares @ {limit_price:.2f} (scanner={price:.2f}, token={token_id[:20]}...)")

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
            # Wait 55s — market will auto-cancel if not filled at close
            logger.info(f"Order live — waiting up to 55s for fill...")
            time.sleep(55)
            try:
                cancel_resp = client.cancel_order(OrderPayload(orderID=order_id))
                logger.info(f"Order cancelled after timeout: {cancel_resp}")
            except Exception as ce:
                logger.warning(f"Cancel failed (may already be filled or expired): {ce}")
            return {"error": "limit not filled, cancelled"}

        return {"error": f"unexpected status: {status}"}

    except Exception as e:
        logger.warning(f"Limit order failed: {e} — skipping")
        return {"error": str(e)}


def get_balance() -> float:
    client = get_client()
    if not client:
        return 0.0
    try:
        from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType
        raw = client.get_balance_allowance(
            params=BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        )
        return float(raw) / 1e6
    except Exception as e:
        logger.error(f"Balance error: {e}")
        return 0.0