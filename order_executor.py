"""
Real order executor — only used when PAPER_TRADING=false
"""
import logging
from config import PRIVATE_KEY, FUNDER, CHAIN_ID, CLOB_HOST

logger = logging.getLogger(__name__)

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


def place_order(token_id: str, price: float, size: float, side: str = "BUY") -> dict:
    client = get_client()
    if not client:
        return {"error": "No CLOB client"}

    # Try market order first (FAK = Fill and Kill)
    try:
        from py_clob_client_v2.clob_types import MarketOrderArgsV2, OrderType
        amount_usdc = float(f"{size * price:.2f}")
        resp = client.create_and_post_market_order(MarketOrderArgsV2(
            token_id=token_id,
            amount=amount_usdc,
            side=side,
            order_type=OrderType.FAK,
        ))
        logger.info(f"Market order placed: {resp}")
        return resp
    except Exception as e:
        logger.warning(f"Market order failed: {e} — trying limit order")

    # Fallback: limit order at current price (expires when market closes)
    try:
        from py_clob_client_v2.clob_types import OrderArgs
        resp = client.create_and_post_order(OrderArgs(
            token_id=token_id,
            price=round(price, 4),
            size=round(size, 2),
            side=side,
        ))
        logger.info(f"Limit order placed: {resp}")
        return resp
    except Exception as e2:
        logger.error(f"Order error: {e2}")
        return {"error": str(e2)}


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