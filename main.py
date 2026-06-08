"""
Polymarket Crypto Bot — main orchestrator
Paper trading mode by default.
"""
import logging
import threading
import time
import requests
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

import config
from price_feed import start_feed, get_feed
from market_scanner import start_scanner, get_scanner
from signal_engine import generate_signal, kelly_size
from paper_trader import get_trader
from dashboard import create_dashboard

# ─────────────────────────── helpers ───────────────────────────

def is_trading_hours() -> bool:
    h = datetime.now(timezone.utc).hour
    return config.TRADING_START_UTC <= h < config.TRADING_END_UTC

def is_drawdown_ok(trader) -> bool:
    stats = trader.get_stats()
    drawdown = (stats["initial_balance"] - stats["balance"]) / stats["initial_balance"]
    return drawdown < config.DRAWDOWN_LIMIT

# ────────────────── market outcome resolver ─────────────────────

def resolve_markets(trader, scanner, feed):
    """
    For each open paper position, check if the market has resolved.
    We query Gamma API for market status and determine winner.
    """
    open_positions = list(trader.open_positions.values())
    if not open_positions:
        return

    for pos in open_positions:
        market_id = pos["market_id"]
        try:
            r = requests.get(
                f"{config.GAMMA_API}/markets/{market_id}",
                timeout=10
            )
            if r.status_code != 200:
                continue
            m = r.json()

            # Check if closed/resolved
            closed = m.get("closed") or m.get("resolved") or False
            if not closed:
                continue

            # Determine winner from outcomes + resolutionId or tokens
            outcome = _determine_outcome(m)
            if outcome:
                trader.resolve_trade(market_id, outcome)

        except Exception as e:
            logger.error(f"Resolve check error for {market_id}: {e}")


def _determine_outcome(m: dict) -> str | None:
    """
    Try to determine if UP or DOWN won from market data.
    """
    import json

    # Check resolutionId or winner
    outcomes = m.get("outcomes")
    if isinstance(outcomes, str):
        try:
            outcomes = json.loads(outcomes)
        except Exception:
            return None

    # Some Gamma responses have resolvedOutcome
    resolved = m.get("resolvedOutcome") or m.get("resolved_outcome")
    if resolved is not None:
        idx = int(resolved)
        if outcomes and idx < len(outcomes):
            val = outcomes[idx].strip().upper()
            if "UP" in val:
                return "UP"
            elif "DOWN" in val:
                return "DOWN"

    # Fallback: check prices (the winner token → price=1.0)
    tokens = m.get("tokens") or []
    for tok in tokens:
        outcome_name = (tok.get("outcome") or "").strip().upper()
        price = float(tok.get("price") or 0)
        if price >= 0.99:
            if "UP" in outcome_name:
                return "UP"
            elif "DOWN" in outcome_name:
                return "DOWN"

    return None


# ─────────────────────── trading loop ──────────────────────────

def trading_loop():
    scanner = get_scanner()
    feed = get_feed()
    trader = get_trader()

    logger.info("Trading loop started")
    time.sleep(90)  # warm-up: let feed collect 3+ prices

    while True:
        try:
            _tick(scanner, feed, trader)
        except Exception as e:
            logger.error(f"Tick error: {e}")
        time.sleep(30)


def _tick(scanner, feed, trader):
    if not is_trading_hours():
        logger.debug("Outside trading hours")
        return

    if not is_drawdown_ok(trader):
        logger.warning("Drawdown limit reached — paused")
        return

    # Check for resolved markets
    resolve_markets(trader, scanner, feed)

    # Get active markets
    markets = scanner.get_markets()
    if not markets:
        logger.debug("No active markets found")
        return

    # Track which assets we've already traded this tick
    traded_assets = set(p.get("asset") for p in trader.open_positions.values())

    for market in markets:
        asset = market["asset"]  # "bitcoin" or "ethereum"
        market_id = market["id"]

        # Skip if already in this market
        if market_id in trader.open_positions:
            continue

        # Get indicators
        indicators = feed.get_indicators(asset)
        if indicators is None:
            logger.debug(f"No indicators for {asset} yet")
            continue

        # Generate signal
        signal = generate_signal(indicators)
        if signal["blocked"]:
            logger.debug(f"{asset}: blocked — {signal['block_reason']}")
            continue

        side = signal["side"]
        confidence = signal["confidence"]

        # Get token_id for the correct outcome
        tokens = market["tokens"]
        token_id = tokens.get(side)
        if not token_id:
            logger.warning(f"No token_id for {side} in market {market_id}")
            continue

        # Get current market price
        price = _get_token_price(token_id)
        if price is None:
            price = 0.50  # fallback mid

        # Kelly sizing
        win_rate = trader.get_win_rate(50)
        size = kelly_size(trader.balance, win_rate, confidence)

        # Paper trade
        opened = trader.open_trade(
            market_id=market_id,
            title=market["title"],
            asset=asset,
            side=side,
            size=size,
            price=price,
            confidence=confidence,
            reasons=signal["reasons"],
            indicators=indicators,
        )

        if opened:
            logger.info(
                f"✅ Trade: {side} {asset.upper()} ${size:.2f} "
                f"[{confidence}] market={market['title'][:40]}"
            )


def _get_token_price(token_id: str) -> float | None:
    try:
        r = requests.get(
            f"{config.CLOB_HOST}/price",
            params={"token_id": token_id, "side": "BUY"},
            timeout=5
        )
        if r.status_code == 200:
            data = r.json()
            return float(data.get("price", 0.5))
    except Exception:
        pass
    return None


# ──────────────────────── price getter for dashboard ─────────────────────────

def get_prices_snapshot():
    feed = get_feed()
    result = {}
    for asset in ["bitcoin", "ethereum"]:
        result[asset] = feed.get_indicators(asset)
    return result


# ────────────────────────────── main ─────────────────────────────────────────

def main():
    mode_label = "📄 PAPER TRADING" if config.PAPER_TRADING else "🔴 LIVE TRADING"
    logger.info(f"Starting bot — mode: {mode_label}")

    # Start data services
    start_feed()
    start_scanner()

    trader = get_trader()

    # Start trading loop in background thread
    t = threading.Thread(target=trading_loop, daemon=True)
    t.start()

    # Flask dashboard
    flask_app = create_dashboard(
        get_stats_fn=trader.get_stats,
        get_prices_fn=get_prices_snapshot,
        mode=mode_label,
    )

    flask_app.run(host="0.0.0.0", port=config.PORT)


if __name__ == "__main__":
    main()
