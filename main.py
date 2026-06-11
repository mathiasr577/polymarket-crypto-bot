import logging
import threading
import time
import requests
import json
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


def is_drawdown_ok(trader) -> bool:
    stats = trader.get_stats()
    drawdown = (stats["initial_balance"] - stats["balance"]) / stats["initial_balance"]
    return drawdown < config.DRAWDOWN_LIMIT


def resolve_expired(trader):
    open_positions = list(trader.open_positions.values())
    if not open_positions:
        return
    for pos in open_positions:
        market_id = pos["market_id"]
        try:
            r = requests.get(f"{config.GAMMA_API}/markets/{market_id}", timeout=8)
            if r.status_code != 200:
                continue
            m = r.json()
            if not (m.get("closed") or m.get("resolved")):
                continue
            outcome = _determine_outcome(m)
            if outcome:
                trader.resolve_trade(market_id, outcome)
                logger.info(f"Resolved {market_id}: {outcome}")
        except Exception as e:
            logger.debug(f"Resolve error {market_id}: {e}")


def _determine_outcome(m: dict) -> str | None:
    outcomes = m.get("outcomes")
    if isinstance(outcomes, str):
        try:
            outcomes = json.loads(outcomes)
        except Exception:
            return None

    resolved = m.get("resolvedOutcome") or m.get("resolved_outcome")
    if resolved is not None:
        try:
            idx = int(resolved)
            if outcomes and idx < len(outcomes):
                return outcomes[idx].strip().upper()
        except Exception:
            pass

    outcome_prices = m.get("outcomePrices")
    if isinstance(outcome_prices, str):
        try:
            outcome_prices = json.loads(outcome_prices)
        except Exception:
            return None

    if outcome_prices and outcomes:
        for i, price in enumerate(outcome_prices):
            try:
                if float(price) >= 0.99 and i < len(outcomes):
                    return outcomes[i].strip().upper()
            except Exception:
                pass
    return None


def trading_loop():
    scanner = get_scanner()
    feed = get_feed()
    trader = get_trader()

    logger.info("Trading loop started — warming up 60s...")
    time.sleep(60)

    while True:
        try:
            _tick(scanner, feed, trader)
        except Exception as e:
            logger.error(f"Tick error: {e}")
        time.sleep(10)  # Check every 10s — need fast response for T-10s entry


def _tick(scanner, feed, trader):
    if not is_drawdown_ok(trader):
        logger.warning("Drawdown limit reached — paused")
        return

    resolve_expired(trader)

    markets = scanner.get_markets()
    if not markets:
        return

    open_market_ids = set(trader.open_positions.keys())

    for market in markets:
        market_id = market["id"]
        if not market_id or market_id in open_market_ids:
            continue

        seconds_left = market.get("seconds_left", 300)

        # Only evaluate markets in entry window (T-10s to T-60s)
        if seconds_left > 65 or seconds_left < 8:
            continue

        asset = market["asset"]

        # Max 1 position per asset
        asset_open = [p for p in trader.open_positions.values() if p.get("asset") == asset]
        if asset_open:
            continue

        indicators = feed.get_indicators(asset)
        if indicators is None:
            logger.info(f"No indicators for {asset} yet")
            continue

        # Get ref_price from price feed (most accurate - captured at window open)
        ref_from_feed = feed.get_current_window_ref(asset)
        if ref_from_feed:
            market["ref_price"] = ref_from_feed

        if not market.get("ref_price"):
            logger.info(f"No ref_price for {asset} market — skipping")
            continue

        signal = generate_signal(indicators, market)

        if signal["blocked"]:
            logger.info(f"Blocked [{asset.upper()} T-{seconds_left:.0f}s]: {signal['block_reason']}")
            continue

        side = signal["side"]
        entry_price = signal["entry_price"]
        confidence = signal["confidence"]

        win_rate = trader.get_win_rate(50)
        size = kelly_size(trader.balance, win_rate, confidence)

        opened = trader.open_trade(
            market_id=str(market_id),
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
                f"✅ TRADE: {side} {asset.upper()} ${size:.2f} "
                f"[{confidence}] @ {entry_price:.2f} | T-{seconds_left:.0f}s | "
                f"{signal['reasons']}"
            )


def get_prices_snapshot():
    feed = get_feed()
    result = {}
    for asset in ["bitcoin", "ethereum"]:
        result[asset] = feed.get_indicators(asset)
    return result


def main():
    mode_label = "📄 PAPER TRADING" if config.PAPER_TRADING else "🔴 LIVE TRADING"
    logger.info(f"Starting — mode: {mode_label}")

    start_feed()
    start_scanner()

    trader = get_trader()

    t = threading.Thread(target=trading_loop, daemon=True)
    t.start()

    flask_app = create_dashboard(
        get_stats_fn=trader.get_stats,
        get_prices_fn=get_prices_snapshot,
        get_markets_fn=get_scanner().get_markets,
        mode=mode_label,
    )

    flask_app.run(host="0.0.0.0", port=config.PORT)


if __name__ == "__main__":
    main()