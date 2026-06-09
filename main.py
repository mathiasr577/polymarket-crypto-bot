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


def is_trading_hours() -> bool:
    h = datetime.now(timezone.utc).hour
    return config.TRADING_START_UTC <= h < config.TRADING_END_UTC


def is_drawdown_ok(trader) -> bool:
    stats = trader.get_stats()
    drawdown = (stats["initial_balance"] - stats["balance"]) / stats["initial_balance"]
    return drawdown < config.DRAWDOWN_LIMIT


def resolve_markets(trader):
    """Check open positions and resolve any that have closed."""
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

            closed = m.get("closed") or m.get("resolved") or False
            if not closed:
                # Check if token price moved to take profit (>0.85)
                _check_take_profit(trader, pos, m)
                continue

            outcome = _determine_outcome(m)
            if outcome:
                trader.resolve_trade(market_id, outcome)

        except Exception as e:
            logger.error(f"Resolve check error {market_id}: {e}")


def _check_take_profit(trader, pos, m):
    """
    If the token we hold is now priced at 0.85+, treat as win and close.
    This captures profit before market officially resolves.
    """
    try:
        outcome_prices = m.get("outcomePrices")
        if isinstance(outcome_prices, str):
            import json
            outcome_prices = json.loads(outcome_prices)
        if not outcome_prices:
            return

        side = pos["side"]
        outcomes = m.get("outcomes")
        if isinstance(outcomes, str):
            import json
            outcomes = json.loads(outcomes)
        if not outcomes:
            return

        for i, outcome in enumerate(outcomes):
            if outcome.strip().upper() == side and i < len(outcome_prices):
                price = float(outcome_prices[i])
                if price >= 0.85:
                    logger.info(f"Take profit: {side} token at {price:.2f}")
                    trader.resolve_trade(pos["market_id"], side)
                    return
                elif price <= 0.15:
                    # Stop loss — it's basically lost
                    loser = "YES" if side == "NO" else "NO"
                    trader.resolve_trade(pos["market_id"], loser)
    except Exception as e:
        logger.debug(f"Take profit check error: {e}")


def _determine_outcome(m: dict) -> str | None:
    import json

    outcomes = m.get("outcomes")
    if isinstance(outcomes, str):
        try:
            outcomes = json.loads(outcomes)
        except Exception:
            return None

    resolved = m.get("resolvedOutcome") or m.get("resolved_outcome")
    if resolved is not None:
        idx = int(resolved)
        if outcomes and idx < len(outcomes):
            return outcomes[idx].strip().upper()

    outcome_prices = m.get("outcomePrices")
    if isinstance(outcome_prices, str):
        try:
            outcome_prices = json.loads(outcome_prices)
        except Exception:
            return None

    if outcome_prices and outcomes:
        for i, price in enumerate(outcome_prices):
            if float(price) >= 0.99 and i < len(outcomes):
                return outcomes[i].strip().upper()

    return None


def trading_loop():
    scanner = get_scanner()
    feed = get_feed()
    trader = get_trader()

    logger.info("Trading loop started — warming up 90s...")
    time.sleep(90)

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

    resolve_markets(trader)

    markets = scanner.get_markets()
    if not markets:
        logger.debug("No crypto markets found yet")
        return

    logger.info(f"Evaluating {len(markets)} markets, balance={trader.balance:.2f}")

    # Track markets we already have positions in
    open_market_ids = set(trader.open_positions.keys())

    for market in markets[:20]:  # top 20 by volume
        market_id = market["id"]

        if market_id in open_market_ids:
            continue

        # Max 1 position per asset at a time
        asset = market["asset"]
        asset_positions = [
            p for p in trader.open_positions.values()
            if p.get("asset") == asset
        ]
        if len(asset_positions) >= 2:
            continue

        indicators = feed.get_indicators(asset)
        if indicators is None:
            continue

        signal = generate_signal(indicators, market)
        if signal["blocked"]:
            logger.info(f"Blocked [{market['title'][:40]}]: {signal['block_reason']}")
            continue

        side = signal["side"]
        token_id = signal["token_id"]
        entry_price = signal["entry_price"]
        confidence = signal["confidence"]

        win_rate = trader.get_win_rate(50)
        size = kelly_size(trader.balance, win_rate, confidence)

        opened = trader.open_trade(
            market_id=market_id,
            title=market["title"],
            asset=asset,
            side=side,
            size=size,
            price=entry_price,
            confidence=confidence,
            reasons=signal["reasons"],
            indicators=indicators,
        )

        if opened:
            logger.info(
                f"✅ TRADE: {side} ${size:.2f} [{confidence}] "
                f"'{market['title'][:50]}' @ {entry_price:.2f}"
            )


def get_prices_snapshot():
    feed = get_feed()
    result = {}
    for asset in ["bitcoin", "ethereum"]:
        result[asset] = feed.get_indicators(asset)
    return result


def main():
    mode_label = "📄 PAPER TRADING" if config.PAPER_TRADING else "🔴 LIVE TRADING"
    logger.info(f"Starting bot — mode: {mode_label}")

    start_feed()
    start_scanner()

    trader = get_trader()

    t = threading.Thread(target=trading_loop, daemon=True)
    t.start()

    flask_app = create_dashboard(
        get_stats_fn=trader.get_stats,
        get_prices_fn=get_prices_snapshot,
        mode=mode_label,
    )

    flask_app.run(host="0.0.0.0", port=config.PORT)


if __name__ == "__main__":
    main()