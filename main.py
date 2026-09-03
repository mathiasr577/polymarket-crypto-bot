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

# Live trader imports — importar el módulo no alcanza para arriesgar plata
# real: cada uno además necesita su propio LIVE_V1_ENABLED/LIVE_V2_ENABLED
# explícito (ver config.py) antes de que trading_loop() lo instancie de verdad.
if not config.PAPER_TRADING:
    from live_trader import get_live_trader
    from live_trader_v2 import get_live_trader_v2

# Shadow-mode: registra Chainlink TWAP/spot en paralelo, sin tocar trading.
if config.SHADOW_MODE_ENABLED:
    from chainlink_feed import start_chainlink_feed, get_chainlink_feed
    from shadow_logger import get_shadow_logger

# Modelo v2 (TWAP + bandas de precio) — paper trading separado, nunca toca
# plata real. Depende del feed de Chainlink, así que solo corre si
# SHADOW_MODE_ENABLED también está prendido.
if config.SHADOW_MODE_ENABLED:
    from signal_engine_v2 import generate_signal_v2, _count_confirmations, ENTRY_WINDOW_START as V2_ENTRY_START, ENTRY_WINDOW_END as V2_ENTRY_END
    from paper_trader_v2 import get_trader_v2

# Order flow de Bybit (trades agresivos reales BTCUSDT/ETHUSDT) — shadow-only,
# no toca ninguna decisión de trading todavía. Ver order_flow_feed.py.
if config.SHADOW_MODE_ENABLED:
    from order_flow_feed import start_order_flow_feed, get_order_flow_feed

# Lean de Kalshi (KXBTC15M/KXETH15M) — shadow-only, no toca ninguna decisión
# de trading todavía. Ver kalshi_feed.py — candidato a 4ta confirmación,
# pendiente de validar en nuestro propio contexto antes de contar.
if config.SHADOW_MODE_ENABLED:
    from kalshi_feed import start_kalshi_feed, get_kalshi_feed

# Libro de órdenes real de Polymarket — shadow-only, alimenta la
# descomposición de ejecución de favorite (delay de 250ms/spread/fee).
# Ver polymarket_book_feed.py.
if config.SHADOW_MODE_ENABLED:
    from polymarket_book_feed import start_book_feed, get_book_feed

# Bot chico y separado, "presión sola" — plata real chica, completamente
# aparte del bot principal (que sigue pausado). Ver pressure_bot.py.
from pressure_bot import PRESSURE_ENABLED, get_pressure_bot

# Otro bot chico y separado, "fade al favorito extremo" — ver
# fade_favorite_bot.py. Apagado hasta que el usuario decida prenderlo.
from fade_favorite_bot import FADE_ENABLED, get_fade_bot

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
    live = get_live_trader() if (not config.PAPER_TRADING and config.LIVE_V1_ENABLED) else None
    live_v2 = get_live_trader_v2() if (not config.PAPER_TRADING and config.LIVE_V2_ENABLED) else None
    shadow = get_shadow_logger() if config.SHADOW_MODE_ENABLED else None
    chainlink = get_chainlink_feed() if config.SHADOW_MODE_ENABLED else None
    paper_v2 = get_trader_v2() if config.SHADOW_MODE_ENABLED else None
    order_flow = get_order_flow_feed() if config.SHADOW_MODE_ENABLED else None
    kalshi = get_kalshi_feed() if config.SHADOW_MODE_ENABLED else None
    book_feed = get_book_feed() if config.SHADOW_MODE_ENABLED else None
    pressure_bot = get_pressure_bot() if PRESSURE_ENABLED else None
    fade_bot = get_fade_bot() if FADE_ENABLED else None

    mode_desc = "LIVE-v1" if live else ("LIVE-v2" if live_v2 else "PAPER")
    if live and live_v2:
        mode_desc = "LIVE-v1+v2"
    logger.info(f"Trading loop started — mode: {mode_desc} — warming up 60s...")
    time.sleep(60)

    while True:
        try:
            _tick(scanner, feed, paper, live, shadow, chainlink, paper_v2, order_flow, live_v2, kalshi, book_feed, pressure_bot, fade_bot)
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


def _tick(scanner, feed, paper, live, shadow=None, chainlink=None, paper_v2=None, order_flow=None, live_v2=None, kalshi=None, book_feed=None, pressure_bot=None, fade_bot=None):
    resolve_expired(paper)

    if paper_v2:
        resolve_expired(paper_v2)  # misma función genérica — solo necesita open_positions/resolve_trade

    if pressure_bot:
        resolve_expired(pressure_bot)  # misma función genérica

    if fade_bot:
        resolve_expired(fade_bot)  # misma función genérica

    if shadow and chainlink:
        try:
            shadow.resolve_pending(chainlink, feed)
        except Exception as e:
            logger.debug(f"Shadow resolve_pending error: {e}")
        try:
            shadow.resolve_pending_execution()
        except Exception as e:
            logger.debug(f"Shadow resolve_pending_execution error: {e}")

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

    if live_v2:
        resolve_live_expired(live_v2)  # misma función genérica

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
    live_v2_open = set(live_v2.open_positions.keys()) if live_v2 else set()

    # Tokens de los mercados que van a evaluarse esta vuelta (los que pasan
    # el filtro de ventana de entrada más abajo) — se juntan acá para
    # suscribir el book_feed a exactamente esos, así el libro ya está
    # llegando cuando (si) dispara una señal de favorite. Ver
    # polymarket_book_feed.py.
    book_tokens = set()

    for market in markets:
        market_id = market["id"]
        if not market_id:
            continue

        seconds_left = market.get("seconds_left", 300)
        if seconds_left > ENTRY_WINDOW_START or seconds_left < ENTRY_WINDOW_END:
            continue

        asset = market["asset"]

        if book_feed:
            tokens_dict = market.get("tokens") or {}
            for tid in tokens_dict.values():
                if tid:
                    book_tokens.add(tid)

        if fade_bot:
            # Mismo horario que pressure_bot — ver comentario de más abajo.
            # No depende de chainlink/TWAP para nada, solo del precio de
            # mercado directo, así que se evalúa acá sin esperar ese bloque.
            _hour_utc = datetime.now(timezone.utc).hour
            if config.TRADING_START_UTC <= _hour_utc < config.TRADING_END_UTC:
                try:
                    fade_bot.evaluate(market)
                except Exception as e:
                    logger.debug(f"fade_bot.evaluate error [{asset}]: {e}")

        # v2 (TWAP + bandas de precio) y el shadow-logger NO dependen del
        # feed de Kraken para nada — evaluarlos acá, ANTES de los checks de
        # Kraken de más abajo. Antes estaban después de esos checks, así
        # que un hipo momentáneo de Kraken (rate limit, red) dejaba ciego
        # de paso a v2 y al shadow-logger, aunque ninguno de los dos use
        # ese feed — justo lo contrario del punto de haber migrado a TWAP.
        # Encontrado en revisión de código, 17-ago-2026.
        window_ts = int(time.time() // 300) * 300

        if chainlink and (paper_v2 or live_v2):
            try:
                snap = chainlink.get_snapshot(asset)
                w60 = chainlink.get_window_twap(asset, 60, window_ts)
                snap["twap60_open"] = w60.get("open")
                if order_flow:
                    snap["ofi_15s"] = order_flow.get_ofi(asset, 15).get("ofi")
                snap["pressure_integral"] = chainlink.get_pressure(asset, window_ts).get("integral")

                if pressure_bot:
                    # Mismo horario que el resto de plata real (9AM-6PM ET) —
                    # se calcula acá porque este bloque corre ANTES de donde
                    # se calcula trading_hours más abajo (ver comentario de
                    # arriba). Encontrado el 1-sep-2026 al revisar el código
                    # con el usuario: pressure_bot había quedado corriendo
                    # 24/7 sin querer, incluida la madrugada donde ya
                    # medimos que la liquidez es mucho peor.
                    _hour_utc = datetime.now(timezone.utc).hour
                    _pressure_trading_hours = config.TRADING_START_UTC <= _hour_utc < config.TRADING_END_UTC
                    if _pressure_trading_hours:
                        try:
                            pressure_bot.evaluate(market, snap["pressure_integral"])
                        except Exception as e:
                            logger.debug(f"pressure_bot.evaluate error [{asset}]: {e}")

                signal_v2 = generate_signal_v2(snap, market)

                if shadow:
                    try:
                        shadow.log_price_tick(market, snap.get("twap60_open"), snap.get("twap60_now"),
                                               market.get("seconds_left"))
                    except Exception as e:
                        logger.debug(f"log_price_tick error [{asset}]: {e}")

                    if book_feed and not signal_v2["blocked"] and signal_v2["band"] == "favorite":
                        try:
                            shadow.log_execution_snapshot(
                                market, signal_v2["entry_price"], signal_v2["side"],
                                signal_v2["token_id"], book_feed,
                            )
                        except Exception as e:
                            logger.debug(f"log_execution_snapshot error [{asset}]: {e}")

                if not signal_v2["blocked"]:
                    if paper_v2 and market_id not in paper_v2_open:
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
                                stake_multiplier=signal_v2.get("stake_multiplier", 1.0),
                            )

                    # Live v2 — solo si LIVE_V2_ENABLED, y mismo horario que
                    # v1 (9AM-6PM ET, probado y necesario por liquidez).
                    # Bandas en config.LIVE_V2_DISABLED_BANDS quedan pausadas
                    # SOLO acá (paper_v2 arriba no se toca, sigue juntando
                    # datos de esa banda para poder investigar y reactivarla).
                    if (trading_hours and live_v2 and market_id not in live_v2_open
                            and signal_v2["band"] not in config.LIVE_V2_DISABLED_BANDS
                            and live_v2.can_trade()):
                        v2_live_asset_open = [p for p in live_v2.open_positions.values() if p.get("asset") == asset]
                        if not v2_live_asset_open:
                            logger.info(
                                f"🔴 Attempting LIVE v2 trade: {signal_v2['side']} {asset.upper()} "
                                f"band={signal_v2['band']} @ {signal_v2['entry_price']:.2f}"
                            )
                            live_v2.open_trade(
                                market_id=str(market_id),
                                title=market["title"],
                                asset=asset,
                                side=signal_v2["side"],
                                price=signal_v2["entry_price"],
                                token_id=signal_v2["token_id"],
                                band=signal_v2["band"],
                                reasons=signal_v2["reasons"],
                                tokens=market["tokens"],
                                stake_multiplier=signal_v2.get("stake_multiplier", 1.0),
                            )
            except Exception as e:
                logger.debug(f"Signal v2 error [{asset}]: {e}")

        # -- A partir de acá, todo lo que sigue SÍ depende de Kraken (v1) --

        indicators = feed.get_indicators(asset)
        if indicators is None:
            # Igual logueamos shadow con lo que hay (TWAP/OFI/presión),
            # aunque falten los campos de Kraken — mejor un hueco parcial
            # (data_gap ya lo marca) que perder la fila entera.
            if shadow and chainlink:
                try:
                    shadow.log_decision(market, {}, {}, chainlink, window_ts, market.get("ref_price"), order_flow_feed=order_flow, kalshi_feed=kalshi)
                except Exception as e:
                    logger.debug(f"Shadow log_decision (sin Kraken) error: {e}")
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
                shadow.log_decision(market, indicators, signal, chainlink, window_ts, market.get("ref_price"), order_flow_feed=order_flow, kalshi_feed=kalshi)
            except Exception as e:
                logger.debug(f"Shadow log_decision error: {e}")

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

    if book_feed:
        try:
            book_feed.set_tokens(book_tokens)
        except Exception as e:
            logger.debug(f"book_feed.set_tokens error: {e}")


BOOK_SNAPSHOT_INTERVAL_SEC = 3
BOOK_SNAPSHOT_RESOLVE_EVERY_N = 10  # cada ~30s, no en cada foto — ahorra pegarle a Gamma de más


def _book_snapshot_loop(scanner, chainlink, order_flow, book_feed, shadow):
    """Loop propio, más rápido que trading_loop's ~10s — ver diseño del
    backtest de maker (1-sep-2026, conversación) y shadow_book_snapshots
    en shadow_logger.py. Corre para TODOS los mercados en ventana de
    entrada, no solo favorite ni solo los que terminan tradeándose —
    la gracia es tener la trayectoria completa del libro real + qué
    hubiera dicho la señal en cada instante, para poder simular después
    cotizar-y-cancelar en vez de cruzar el spread. Solo lectura/logging,
    no toca ninguna decisión de trading."""
    tick_n = 0
    trade_since_ts = {}  # token_id -> último ts drenado, ver shadow.log_trades
    while True:
        try:
            tick_n += 1
            markets = scanner.get_markets()
            for market in markets or []:
                seconds_left = market.get("seconds_left", 300)
                if seconds_left > V2_ENTRY_START or seconds_left < V2_ENTRY_END:
                    continue
                asset = market["asset"]

                # Tape de trades — independiente del feed de Chainlink, así
                # que va afuera del try/twap60 de abajo (no se quiere perder
                # esto solo porque falte data de TWAP en un tick puntual).
                try:
                    trade_since_ts = shadow.log_trades(market, book_feed, trade_since_ts)
                except Exception as e:
                    logger.debug(f"log_trades error [{asset}]: {e}")

                try:
                    snap = chainlink.get_snapshot(asset)
                    window_ts = int(time.time() // 300) * 300
                    w60 = chainlink.get_window_twap(asset, 60, window_ts)
                    twap60_open = w60.get("open")
                    twap60_now = snap.get("twap60_now")
                    if twap60_open is None or twap60_now is None or not twap60_open:
                        continue
                    diff = twap60_now - twap60_open
                    if diff == 0:
                        continue
                    side = "UP" if diff > 0 else "DOWN"

                    cs = dict(snap)
                    cs["twap60_now"] = twap60_now
                    if order_flow:
                        cs["ofi_15s"] = order_flow.get_ofi(asset, 15).get("ofi")
                    cs["pressure_integral"] = chainlink.get_pressure(asset, window_ts).get("integral")
                    confirmations, _ = _count_confirmations(cs, side)

                    shadow.log_book_snapshot(market, twap60_open, twap60_now, confirmations, side, book_feed)
                except Exception as e:
                    logger.debug(f"_book_snapshot_loop error [{asset}]: {e}")

            if tick_n % BOOK_SNAPSHOT_RESOLVE_EVERY_N == 0:
                try:
                    shadow.resolve_pending_book_snapshots()
                except Exception as e:
                    logger.debug(f"resolve_pending_book_snapshots error: {e}")
        except Exception as e:
            logger.error(f"_book_snapshot_loop tick error: {e}")

        time.sleep(BOOK_SNAPSHOT_INTERVAL_SEC)


def get_prices_snapshot():
    feed = get_feed()
    result = {}
    for asset in ["bitcoin", "ethereum"]:
        result[asset] = feed.get_indicators(asset)
    return result


def get_combined_stats():
    """En modo live, el dashboard debe mostrar como stats PRINCIPALES las de
    quien esté arriesgando plata real de verdad — antes se devolvía
    paper_stats como base y las de live quedaban anidadas en stats["live"],
    que el template nunca lee. Resultado: el dashboard mostraba el
    P&L/win-rate/trades de paper (miles de dólares simulados) bajo el
    banner "LIVE TRADING", sin ningún indicio de que esos números no eran
    reales. Ahora paper queda anidado y claramente etiquetado.

    Con LIVE_V1_ENABLED/LIVE_V2_ENABLED, "plata real de verdad" ya no es
    simplemente "not PAPER_TRADING" — puede haber ninguno, uno, o los dos
    modelos arriesgando plata real a la vez. v2 tiene prioridad como base
    si está activo (es el modelo que se validó y hacia el que se migró),
    con v1 anidado si también corre; si solo corre v1, v1 es la base
    (compatibilidad con el comportamiento de antes)."""
    paper_stats = get_trader().get_stats()
    live_v1_active = (not config.PAPER_TRADING) and config.LIVE_V1_ENABLED
    live_v2_active = (not config.PAPER_TRADING) and config.LIVE_V2_ENABLED

    if live_v2_active:
        base = get_live_trader_v2().get_stats()
        base["paper"] = paper_stats
        if live_v1_active:
            base["live_v1"] = get_live_trader().get_stats()
    elif live_v1_active:
        base = get_live_trader().get_stats()
        base["paper"] = paper_stats
    else:
        base = paper_stats

    if config.SHADOW_MODE_ENABLED:
        base["paper_v2"] = get_trader_v2().get_stats()
    if PRESSURE_ENABLED:
        base["pressure_bot"] = get_pressure_bot().get_stats()
    if FADE_ENABLED:
        base["fade_bot"] = get_fade_bot().get_stats()
    return base


def main():
    live_v1_active = (not config.PAPER_TRADING) and config.LIVE_V1_ENABLED
    live_v2_active = (not config.PAPER_TRADING) and config.LIVE_V2_ENABLED
    any_live = live_v1_active or live_v2_active

    if live_v1_active and live_v2_active:
        mode_label = "🔴 LIVE TRADING (v1+v2)"
    elif live_v2_active:
        mode_label = "🔴 LIVE TRADING (v2)"
    elif live_v1_active:
        mode_label = "🔴 LIVE TRADING (v1)"
    elif not config.PAPER_TRADING:
        # PAPER_TRADING=false pero ningún LIVE_V*_ENABLED prendido — no
        # arriesga plata real igual, PAPER_TRADING solo ya no alcanza.
        mode_label = "📄 PAPER TRADING (PAPER_TRADING=false pero sin LIVE_V1/V2_ENABLED)"
    else:
        mode_label = "📄 PAPER TRADING"

    logger.info(f"Starting — mode: {mode_label}")

    if any_live:
        logger.info(
            f"⚠️  LIVE TRADING ACTIVE — Real money will be used "
            f"(v1={live_v1_active}, v2={live_v2_active})"
        )

    start_feed()
    start_scanner()
    if config.SHADOW_MODE_ENABLED:
        start_chainlink_feed()
        start_order_flow_feed()
        start_kalshi_feed()
        start_book_feed()

    t = threading.Thread(target=trading_loop, daemon=True)
    t.start()

    if config.SHADOW_MODE_ENABLED:
        t2 = threading.Thread(
            target=_book_snapshot_loop,
            args=(get_scanner(), get_chainlink_feed(), get_order_flow_feed(), get_book_feed(), get_shadow_logger()),
            daemon=True,
        )
        t2.start()

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
        get_shadow_lead_agreement_fn=(get_shadow_logger().get_lead_agreement_report if config.SHADOW_MODE_ENABLED else None),
        get_shadow_ofi_fn=(get_shadow_logger().get_ofi_report if config.SHADOW_MODE_ENABLED else None),
        get_shadow_pressure_fn=(get_shadow_logger().get_pressure_report if config.SHADOW_MODE_ENABLED else None),
        get_shadow_confirmation_fn=(get_shadow_logger().get_confirmation_report if config.SHADOW_MODE_ENABLED else None),
        get_shadow_sizing_fn=(get_shadow_logger().simulate_confirmation_weighted_sizing if config.SHADOW_MODE_ENABLED else None),
    )

    flask_app.run(host="0.0.0.0", port=config.PORT)


if __name__ == "__main__":
    main()