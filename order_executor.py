"""
Real order executor — only used when PAPER_TRADING=false
Validates with CLOB /price and /book before executing FAK.
"""
import logging
import requests
from decimal import Decimal
from config import PRIVATE_KEY, FUNDER, CHAIN_ID, CLOB_HOST

logger = logging.getLogger(__name__)

MIN_PRICE = Decimal("0.25")
MAX_PRICE = Decimal("0.80")
CLOB = "https://clob.polymarket.com"

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


def _clob_get(path, params, timeout=2.0):
    r = requests.get(f"{CLOB}{path}", params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


def get_buy_price(token_id: str) -> Decimal | None:
    try:
        data = _clob_get("/price", {"token_id": token_id, "side": "BUY"})
        return Decimal(str(data["price"]))
    except Exception as e:
        logger.debug(f"CLOB /price error: {e}")
        return None


def liquidity_ok(token_id: str, amount_usdc: float) -> tuple[bool, str, Decimal | None]:
    amount = Decimal(str(round(amount_usdc, 2)))

    # Step 1: /price BUY — fuente principal
    buy_price = get_buy_price(token_id)
    if buy_price is None:
        return False, "no CLOB buy price", None

    if buy_price < MIN_PRICE or buy_price > MAX_PRICE:
        return False, f"buy price out of range: {buy_price}", buy_price

    # Step 2: /book para profundidad
    try:
        book = _clob_get("/book", {"token_id": token_id})
        asks = book.get("asks", [])
        asks = sorted(asks, key=lambda x: Decimal(str(x["price"])))

        remaining = amount
        weighted_cost = Decimal("0")
        total_shares = Decimal("0")

        for level in asks:
            price = Decimal(str(level["price"]))
            size = Decimal(str(level["size"]))

            if price > MAX_PRICE:
                break

            level_cost = price * size
            take_cost = min(remaining, level_cost)
            take_shares = take_cost / price

            weighted_cost += take_cost
            total_shares += take_shares
            remaining -= take_cost

            if remaining <= Decimal("0"):
                avg_price = weighted_cost / total_shares
                return True, f"liquidity ok avg={avg_price:.4f}", avg_price

        # Si /book no tiene suficiente pero /price existe y amount <= $5, dejar pasar
        if amount <= Decimal("5"):
            return True, f"book thin, using /price fallback: {buy_price}", buy_price

        return False, "not enough ask liquidity under max price", buy_price

    except Exception as e:
        # /book falló pero /price está bien — permitir trade pequeño
        if amount <= Decimal("5"):
            return True, f"book failed, /price fallback: {buy_price}", buy_price
        return False, f"book failed, amount too large: {e}", buy_price


def place_order(token_id: str, price: float, size: float, side: str = "BUY") -> dict:
    client = get_client()
    if not client:
        return {"error": "No CLOB client"}

    amount_usdc = round(size * price, 2)

    ok, reason, real_price = liquidity_ok(token_id, amount_usdc)
    if not ok:
        logger.warning(f"SKIP {token_id[:20]}...: {reason}")
        return {"error": reason}

    logger.info(f"EXECUTE: {reason}")

    try:
        from py_clob_client_v2.clob_types import MarketOrderArgsV2, OrderType
        resp = client.create_and_post_market_order(MarketOrderArgsV2(
            token_id=token_id,
            amount=float(amount_usdc),
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