"""
Real order executor — only used when PAPER_TRADING=false
Checks real order book price before executing.
"""
import logging
import requests
from config import PRIVATE_KEY, FUNDER, CHAIN_ID, CLOB_HOST

logger = logging.getLogger(__name__)

_client = None

MIN_PRICE = 0.25
MAX_PRICE = 0.80

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


def get_real_ask_price(token_id: str) -> float | None:
    """
    Get the real best ask price from the CLOB order book via HTTP.
    Returns None if order book doesn't exist or has no asks.
    """
    try:
        r = requests.get(
            f"{CLOB_HOST}/book",
            params={"token_id": token_id},
            timeout=5
        )
        if r.status_code != 200:
            logger.debug(f"Order book error {r.status_code}: {r.text[:100]}")
            return None
        
        data = r.json()
        asks = data.get("asks", [])
        
        if not asks:
            logger.debug(f"No asks in order book for {token_id[:20]}...")
            return None
        
        # asks are sorted ascending by price — first is best ask (cheapest to buy)
        best_ask = float(asks[0].get("price", 1.0))
        logger.info(f"Real ask price: {best_ask:.3f}")
        return best_ask
        
    except Exception as e:
        logger.debug(f"Order book fetch error: {e}")
        return None


def place_order(token_id: str, price: float, size: float, side: str = "BUY") -> dict:
    client = get_client()
    if not client:
        return {"error": "No CLOB client"}

    # Check real price from CLOB order book
    real_price = get_real_ask_price(token_id)
    
    if real_price is not None:
        if real_price > MAX_PRICE:
            logger.warning(f"Real ask {real_price:.3f} > {MAX_PRICE} — skipping, too expensive")
            return {"error": f"real price too high: {real_price:.3f}"}
        if real_price < MIN_PRICE:
            logger.warning(f"Real ask {real_price:.3f} < {MIN_PRICE} — skipping, no liquidity")
            return {"error": f"real price too low: {real_price:.3f}"}
        logger.info(f"Real ask {real_price:.3f} OK — executing")
        # Use real price for amount calculation
        exec_price = real_price
    else:
        # If we can't read the order book, use scanner price
        # but only if it's in range
        if price > MAX_PRICE or price < MIN_PRICE:
            return {"error": f"scanner price out of range: {price:.3f}"}
        logger.warning(f"Could not read order book, using scanner price {price:.3f}")
        exec_price = price

    try:
        from py_clob_client_v2.clob_types import MarketOrderArgsV2, OrderType
        amount_usdc = float(f"{size * exec_price:.2f}")
        resp = client.create_and_post_market_order(MarketOrderArgsV2(
            token_id=token_id,
            amount=amount_usdc,
            side=side,
            order_type=OrderType.FAK,
        ))
        logger.info(f"Market order placed: {resp}")
        return resp
    except Exception as e:
        logger.warning(f"Market order failed: {e} — skipping")
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