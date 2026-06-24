"""
Real order executor — only used when PAPER_TRADING=false
Market orders only (FAK). Verifies real price before executing.
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


def get_best_ask(token_id: str) -> float:
    """Get the real best ask price from the order book."""
    client = get_client()
    if not client:
        return 1.0
    try:
        book = client.get_order_book(token_id)
        asks = book.asks if hasattr(book, 'asks') else []
        if asks:
            # asks are sorted ascending, first is best ask
            best = float(asks[0].price)
            logger.info(f"Best ask for token: {best:.3f}")
            return best
        return 1.0
    except Exception as e:
        logger.debug(f"Order book error: {e}")
        return 1.0


def place_order(token_id: str, price: float, size: float, side: str = "BUY") -> dict:
    client = get_client()
    if not client:
        return {"error": "No CLOB client"}

    # Check real price in order book before executing
    real_price = get_best_ask(token_id)

    if real_price > 0.80:
        logger.warning(f"Real price {real_price:.3f} > 0.80 — skipping, too expensive")
        return {"error": f"real price too high: {real_price:.3f}"}

    if real_price < 0.20:
        logger.warning(f"Real price {real_price:.3f} < 0.20 — skipping, no liquidity")
        return {"error": f"real price too low: {real_price:.3f}"}

    logger.info(f"Real price {real_price:.3f} OK — executing FAK order")

    try:
        from py_clob_client_v2.clob_types import MarketOrderArgsV2, OrderType
        # Use real price for amount calculation
        amount_usdc = float(f"{size * real_price:.2f}")
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