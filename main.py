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
from paper_trader import get_trader
from dashboard import create_dashboard

# Live trader imports
if not config.PAPER_TRADING:
    from live_trader import get_live_trader

# Shadow-mode: registra Chainlink TWAP/spot en paralelo, sin tocar trading.
if config.SHADOW_MODE_ENABLED:
    from chainlink_feed import start_chainlink_feed, get_chainlink_feed
    from shadow_logger import get_shadow_logger

# Modelo v2 (TWAP + bandas de precio) — paper trading separado, nunca toca
# plata real. Depende del feed de Chainlink, así que solo corre si
# SHADOW_MODE_ENABLED también está prendido.
if config.SHADOW_MODE_ENABLED:
    from signal_engine_v2 import generate_signal_v2, ENTRY_WINDOW_START as V2_ENTRY_START, ENTRY_WINDOW_END as V2_ENTRY_END
    from paper_trader_v2 import get_trader_v2

# Order flow de Bybit (trades agresivos reales BTCUSDT/ETHUSDT) — shadow-only,
# no toca ninguna decisión de trading todavía. Ver order_flow_feed.py.
if config.SHADOW_MODE_ENABLED:
    from order_flow_feed import start_order_flow_feed, get_order_flow_feed

# Delta mínimo del activo correlacionado para contar como confirmación cruzada
CROSS_ASSET_MIN_DELTA = 0.0003


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
    shadow = get_shadow_logger() if config.SHADOW_MODE_ENABLED else None
    chainlink = get_chainlink_feed() if config.SHADOW_MODE_ENABLED else None
    paper_v2 = get_trader_v2() if config.SHADOW_MODE_ENABLED else None
    order_flow = get_order_flow_feed() if config.SHADOW_MODE_ENABLED else None

    logger.info(f"Trading loop started — mode: {'LIVE' if live else 'PAPER'} — warming up 60s...")
    time.sleep(60)

    while True:
        try:
            _tick(scanner, feed, paper, live, shadow, chainlink, paper_v2, order_flow)
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


def _tick(scanner, feed, paper, live, shadow=None, chainlink=None, paper_v2=None, order_flow=None):
    resolve_expired(paper)

    if paper_v2:
        resolve_expired(paper_v2)  # misma función genérica — solo necesita open_positions/resolve_trade

    if shadow and chainlink:
        try:
            shadow.resolve_pending(chainlink, feed)
        except Exception as e:
            logger.debug(f"Shadow resolve_pending error: {e}")

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

    # Live trading solo corre 9AM-6PM ET (1PM-10PM UTC) — probado y
    # descartado el trading 24/7 en vivo por baja liquidez de madrugada.
    # Paper trading no arriesga plata real, así que corre siempre: da más
    # datos para el win_rate que alimenta el Kelly sizing de paper, y deja
    # ver cómo se comportaría la señal fuera de ese horario.
    hour_utc = datetime.now(timezone.utc).hour
    trading_hours = config.TRADING_START_UTC <= hour_utc < config.TRADING_END_UTC

    markets = scanner.get_markets()
    if not markets:
        return

    paper_open = set(paper.open_positions.keys())
    live_open = set(live.open_positions.keys()) if live else set()
    paper_v2_open = set(paper_v2.open_positions.keys()) if paper_v2 else set()

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
        indicators["trend_drift_per_sec"] = feed.get_trend_drift_per_sec(asset)
        indicators["trend_drift_long_per_sec"] = feed.get_trend_drift_per_sec_long(asset)
        indicators["side_recent_win_rate"] = {
            "UP": paper.get_win_rate_by_side("UP", 20),
            "DOWN": paper.get_win_rate_by_side("DOWN", 20),
        }

        signal = generate_signal(indicators, market)

        if shadow and chainlink:
            try:
                window_ts = int(time.time() // 300) * 300
                shadow.log_decision(market, indicators, signal, chainlink, window_ts, market.get("ref_price"), order_flow_feed=order_flow)
            except Exception as e:
                logger.debug(f"Shadow log_decision error: {e}")

        # Modelo v2 (TWAP + bandas de precio) — independiente de v1, se
        # evalúa siempre, aunque v1 esté bloqueado. Nunca toca plata real,
        # solo paper_v2 (ver paper_trader_v2.py).
        if paper_v2 and chainlink and market_id not in paper_v2_open:
            try:
                window_ts = int(time.time() // 300) * 300
                snap = chainlink.get_snapshot(asset)
                w60 = chainlink.get_window_twap(asset, 60, window_ts)
                snap["twap60_open"] = w60.get("open")
                signal_v2 = generate_signal_v2(snap, market)
                if not signal_v2["blocked"]:
                    v2_asset_open = [p for p in paper_v2.open_positions.values() if p.get("asset") == asset]
                    if not v2_asset_open:
                        paper_v2.open_trade(
                            market_id=str(market_id),
                            title=market["title"],
                            asset=asset,
                            side=signal_v2["side"],
                            price=signal_v2["entry_price"],
                            band=signal_v2["band"],
                            reasons=signal_v2["reasons"],
                        )
            except Exception as e:
                logger.debug(f"Signal v2 error [{asset}]: {e}")

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
        if trading_hours and live and market_id not in live_open and live.can_trade():
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
    """En modo live, el dashboard debe mostrar como stats PRINCIPALES las de
    live_trader (plata real) — antes se devolvía paper_stats como base y las
    de live quedaban anidadas en stats["live"], que el template nunca lee.
    Resultado: el dashboard mostraba el P&L/win-rate/trades de paper (miles
    de dólares simulados) bajo el banner "LIVE TRADING", sin ningún indicio
    de que esos números no eran reales. Ahora paper queda anidado y
    claramente etiquetado en su propia sección."""
    paper_stats = get_trader().get_stats()
    if not config.PAPER_TRADING:
        base = get_live_trader().get_stats()
        base["paper"] = paper_stats
    else:
        base = paper_stats

    if config.SHADOW_MODE_ENABLED:
        base["paper_v2"] = get_trader_v2().get_stats()
    return base


def main():
    mode_label = "🔴 LIVE TRADING" if not config.PAPER_TRADING else "📄 PAPER TRADING"
    logger.info(f"Starting — mode: {mode_label}")

    if not config.PAPER_TRADING:
        logger.info("⚠️  LIVE TRADING ACTIVE — Real money will be used")

    start_feed()
    start_scanner()
    if config.SHADOW_MODE_ENABLED:
        start_chainlink_feed()
        start_order_flow_feed()

    t = threading.Thread(target=trading_loop, daemon=True)
    t.start()

    flask_app = create_dashboard(
        get_stats_fn=get_combined_stats,
        get_prices_fn=get_prices_snapshot,
        get_markets_fn=get_scanner().get_markets,
        mode=mode_label,
        get_shadow_stats_fn=(get_shadow_logger().get_validation_stats if config.SHADOW_MODE_ENABLED else None),
        get_shadow_backtest_fn=(get_shadow_logger().get_decision_time_backtest if config.SHADOW_MODE_ENABLED else None),
        get_shadow_calibration_fn=(get_shadow_logger().get_calibration_report if config.SHADOW_MODE_ENABLED else None),
        get_arb_stats_fn=get_scanner().get_arb_stats,
        get_shadow_calib_curve_fn=(get_shadow_logger().get_probability_calibration if config.SHADOW_MODE_ENABLED else None),
        get_shadow_weekday_fn=(get_shadow_logger().get_weekday_weekend_report if config.SHADOW_MODE_ENABLED else None),
        get_shadow_lead_fn=(get_shadow_logger().get_lead_signal_report if config.SHADOW_MODE_ENABLED else None),
        get_shadow_v2_sim_fn=(get_shadow_logger().simulate_v2_policy if config.SHADOW_MODE_ENABLED else None),
        get_shadow_model_comparison_fn=(get_shadow_logger().get_model_comparison if config.SHADOW_MODE_ENABLED else None),
        get_shadow_magnitude_fn=(get_shadow_logger().get_magnitude_and_agreement_report if config.SHADOW_MODE_ENABLED else None),
        get_shadow_filtered_sim_fn=(get_shadow_logger().simulate_v2_filtered_policy if config.SHADOW_MODE_ENABLED else None),
        get_shadow_favorite_detail_fn=(get_shadow_logger().get_favorite_band_detail if config.SHADOW_MODE_ENABLED else None),
        get_shadow_hourly_fn=(get_shadow_logger().get_hourly_report if config.SHADOW_MODE_ENABLED else None),
        get_shadow_favorite_ext_fn=(get_shadow_logger().simulate_favorite_extension if config.SHADOW_MODE_ENABLED else None),
        get_shadow_v2_weekday_fn=(get_shadow_logger().get_v2_weekday_report if config.SHADOW_MODE_ENABLED else None),
    )

    flask_app.run(host="0.0.0.0", port=config.PORT)


if __name__ == "__main__":
    main()