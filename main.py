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
from signal_engine import generate_signal, kelly_size, ENTRY_WINDOW_START, ENTRY_WINDOW_END

# Delta mínimo del activo correlacionado para contar como confirmación cruzada
CROSS_ASSET_MIN_DELTA = 0.0003
from paper_trader import get_trader
from dashboard import create_dashboard

# Live trader imports
if not config.PAPER_TRADING:
    from live_trader import get_live_trader


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


def resolve_live_expired(live_trader):
    open_positions = list(live_trader.open_positions.values())
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
                live_trader.resolve_trade(market_id, outcome)
        except Exception as e:
            logger.debug(f"Live resolve error {market_id}: {e}")


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
    paper = get_trader()
    live = get_live_trader() if not config.PAPER_TRADING else None

    logger.info(f"Trading loop started — mode: {'LIVE' if live else 'PAPER'} — warming up 60s...")
    time.sleep(60)

    while True:
        try:
            _tick(scanner, feed, paper, live)
        except Exception as e:
            logger.error(f"Tick error: {e}")
        time.sleep(10)


def _cross_asset_confirm(feed, asset: str) -> str | None:
    """BTC y ETH suelen moverse juntos en ventanas de 5 min. Si el otro
    activo también se está moviendo en una dirección clara en su ventana
    actual, devuelve esa dirección como confirmación barata."""
    other = "ethereum" if asset == "bitcoin" else "bitcoin"
    other_ind = feed.get_indicators(other)
    other_ref = feed.get_current_window_ref(other)
    if not other_ind or not other_ref:
        return None

    other_price = other_ind.get("price")
    if not other_price:
        return None

    other_delta = (other_price - other_ref) / other_ref
    if other_delta >= CROSS_ASSET_MIN_DELTA:
        return "UP"
    if other_delta <= -CROSS_ASSET_MIN_DELTA:
        return "DOWN"
    return None


def _tick(scanner, feed, paper, live):
    resolve_expired(paper)

    if live:
        resolve_live_expired(live)
        if live.get_stats()["done"]:
            stats = live.get_stats()
            logger.info(
                f"🏁 LIVE TEST COMPLETE: "
                f"{stats['wins']}/{stats['completed']} wins | "
                f"PnL={stats['total_pnl']:+.2f} | "
                f"WinRate={stats['win_rate']:.1f}%"
            )

    # Only trade during market hours: 9AM-6PM ET (1PM-10PM UTC)
    hour_utc = datetime.now(timezone.utc).hour
    if not (config.TRADING_START_UTC <= hour_utc < config.TRADING_END_UTC):
        return

    markets = scanner.get_markets()
    if not markets:
        return

    paper_open = set(paper.open_positions.keys())
    live_open = set(live.open_positions.keys()) if live else set()

    for market in markets:
        market_id = market["id"]
        if not market_id:
            continue

        seconds_left = market.get("seconds_left", 300)
        if seconds_left > ENTRY_WINDOW_START or seconds_left < ENTRY_WINDOW_END:
            continue

        asset = market["asset"]

        indicators = feed.get_indicators(asset)
        if indicators is None:
            continue

        ref_from_feed = feed.get_current_window_ref(asset)
        if ref_from_feed:
            market["ref_price"] = ref_from_feed

        if not market.get("ref_price"):
            continue

        indicators["vol_per_sqrt_sec"] = feed.get_volatility_per_sqrt_sec(asset)
        indicators["cross_asset_confirm"] = _cross_asset_confirm(feed, asset)

        signal = generate_signal(indicators, market)
        if signal["blocked"]:
            logger.info(f"Blocked [{asset.upper()} T-{seconds_left:.0f}s]: {signal['block_reason']}")
            continue

        side = signal["side"]
        token_id = signal["token_id"]
        entry_price = signal["entry_price"]
        confidence = signal["confidence"]

        # Paper trade (always, regardless of hours)
        if market_id not in paper_open:
            paper_asset_open = [p for p in paper.open_positions.values() if p.get("asset") == asset]
            if not paper_asset_open:
                win_rate = paper.get_win_rate(50)
                size = kelly_size(paper.balance, win_rate, confidence)
                paper.open_trade(
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

        # Live trade — only during trading hours
        if live and market_id not in live_open and live.can_trade():
            live_asset_open = [p for p in live.open_positions.values() if p.get("asset") == asset]
            if not live_asset_open:
                logger.info(f"🔴 Attempting LIVE trade: {side} {asset.upper()} @ {entry_price:.2f}")
                live.open_trade(
                    market_id=str(market_id),
                    title=market["title"],
                    asset=asset,
                    side=side,
                    price=entry_price,
                    token_id=token_id,
                    reasons=signal["reasons"],
                    indicators=indicators,
                    tokens=market["tokens"],
                )


def get_prices_snapshot():
    feed = get_feed()
    result = {}
    for asset in ["bitcoin", "ethereum"]:
        result[asset] = feed.get_indicators(asset)
    return result


def get_combined_stats():
    paper_stats = get_trader().get_stats()
    if not config.PAPER_TRADING:
        live_stats = get_live_trader().get_stats()
        paper_stats["live"] = live_stats
    return paper_stats


def main():
    mode_label = "🔴 LIVE TRADING" if not config.PAPER_TRADING else "📄 PAPER TRADING"
    logger.info(f"Starting — mode: {mode_label}")

    if not config.PAPER_TRADING:
        logger.info("⚠️  LIVE TRADING ACTIVE — Real money will be used")

    start_feed()
    start_scanner()

    t = threading.Thread(target=trading_loop, daemon=True)
    t.start()

    flask_app = create_dashboard(
        get_stats_fn=get_combined_stats,
        get_prices_fn=get_prices_snapshot,
        get_markets_fn=get_scanner().get_markets,
        mode=mode_label,
    )

    flask_app.run(host="0.0.0.0", port=config.PORT)


if __name__ == "__main__":
    main()