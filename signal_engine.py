"""
Estrategia basada en el build guide real:
- Entrar a T-10 segundos antes del cierre
- Señal principal: delta del precio vs precio de referencia del mercado
- Si BTC ya bajó X% desde el inicio → DOWN casi seguro
- Si BTC ya subió X% desde el inicio → UP casi seguro
- Si neutral → no apostar
"""
import logging

logger = logging.getLogger(__name__)

# Thresholds basados en backtests del build guide
DELTA_STRONG = 0.0010   # 0.10% → señal fuerte, entrar
DELTA_MEDIUM = 0.0005   # 0.05% → señal media, entrar si otros confirman
ENTRY_WINDOW_START = 60  # Entrar entre T-60s y T-10s del cierre
ENTRY_WINDOW_END = 10    # No entrar si quedan menos de 10s

def generate_signal(indicators: dict, market: dict) -> dict:
    result = {
        "side": None,
        "confidence": None,
        "score": 0,
        "reasons": [],
        "blocked": False,
        "block_reason": "",
        "token_id": None,
        "entry_price": None,
    }

    if indicators is None:
        result["blocked"] = True
        result["block_reason"] = "No indicators yet"
        return result

    seconds_left = market.get("seconds_left", 300)

    # ── Ventana de entrada: entre T-60s y T-10s ──────────────────────
    if seconds_left > ENTRY_WINDOW_START:
        result["blocked"] = True
        result["block_reason"] = f"Too early: {seconds_left:.0f}s left (wait until T-60s)"
        return result

    if seconds_left < ENTRY_WINDOW_END:
        result["blocked"] = True
        result["block_reason"] = f"Too late: {seconds_left:.0f}s left"
        return result

    # ── Factor 1 (PRINCIPAL): Delta precio actual vs referencia ───────
    current_price = indicators.get("price")
    ref_price = market.get("ref_price")

    if not current_price or not ref_price or ref_price == 0:
        result["blocked"] = True
        result["block_reason"] = "No reference price available"
        return result

    delta = (current_price - ref_price) / ref_price
    abs_delta = abs(delta)

    votes = {"UP": 0, "DOWN": 0}
    reasons = {"UP": [], "DOWN": []}

    if abs_delta >= DELTA_STRONG:
        # Señal fuerte — el precio ya se movió significativamente
        if delta > 0:
            votes["UP"] += 3
            reasons["UP"].append(f"Price +{delta*100:.3f}% above ref ${ref_price:,.0f} → UP locked")
        else:
            votes["DOWN"] += 3
            reasons["DOWN"].append(f"Price {delta*100:.3f}% below ref ${ref_price:,.0f} → DOWN locked")

    elif abs_delta >= DELTA_MEDIUM:
        # Señal media — necesita confirmación
        if delta > 0:
            votes["UP"] += 2
            reasons["UP"].append(f"Price +{delta*100:.3f}% above ref → leaning UP")
        else:
            votes["DOWN"] += 2
            reasons["DOWN"].append(f"Price {delta*100:.3f}% below ref → leaning DOWN")
    else:
        # Delta muy pequeño — mercado muy incierto, no apostar
        result["blocked"] = True
        result["block_reason"] = f"Delta too small: {delta*100:.4f}% (need ±{DELTA_MEDIUM*100:.2f}%)"
        return result

    # ── Factor 2: Momentum últimos 2 minutos ─────────────────────────
    momentum = indicators.get("momentum", "neutral")
    pct_2min = indicators.get("pct_2min", 0)

    if momentum == "up" and pct_2min > 0 and delta > 0:
        votes["UP"] += 1
        reasons["UP"].append(f"Momentum confirms UP (+{pct_2min*100:.3f}%)")
    elif momentum == "down" and pct_2min < 0 and delta < 0:
        votes["DOWN"] += 1
        reasons["DOWN"].append(f"Momentum confirms DOWN ({pct_2min*100:.3f}%)")
    elif (momentum == "up" and delta < 0) or (momentum == "down" and delta > 0):
        # Momentum contradice el delta — reducir confianza
        if votes["UP"] > 0:
            votes["UP"] = max(0, votes["UP"] - 1)
        if votes["DOWN"] > 0:
            votes["DOWN"] = max(0, votes["DOWN"] - 1)

    # ── Factor 3: Token price — evitar tokens muy caros ──────────────
    up_price = market.get("up_price", 0.5)
    down_price = market.get("down_price", 0.5)

    # Filtro de precio: entre 0.25 y 0.80 solamente
    # < 0.25 = sin liquidez, orden nunca se llena
    # > 0.80 = poco upside
    if votes["UP"] >= votes["DOWN"]:
        if up_price > 0.80:
            result["blocked"] = True
            result["block_reason"] = f"UP token too expensive: {up_price:.2f}"
            return result
        if up_price < 0.25:
            result["blocked"] = True
            result["block_reason"] = f"UP token no liquidity: {up_price:.2f}"
            return result
    else:
        if down_price > 0.80:
            result["blocked"] = True
            result["block_reason"] = f"DOWN token too expensive: {down_price:.2f}"
            return result
        if down_price < 0.25:
            result["blocked"] = True
            result["block_reason"] = f"DOWN token no liquidity: {down_price:.2f}"
            return result

    # ── Evaluar ───────────────────────────────────────────────────────
    up_score = votes["UP"]
    down_score = votes["DOWN"]
    best_side = "UP" if up_score >= down_score else "DOWN"
    best_score = max(up_score, down_score)

    if best_score < 2:
        result["blocked"] = True
        result["block_reason"] = f"Signal too weak: score={best_score}"
        return result

    token_id = market["tokens"].get(best_side)
    entry_price = up_price if best_side == "UP" else down_price

    result["side"] = best_side
    result["score"] = best_score
    result["confidence"] = "HIGH" if best_score >= 3 else "MEDIUM"
    result["reasons"] = reasons[best_side]
    result["token_id"] = token_id
    result["entry_price"] = entry_price

    logger.info(
        f"✅ Signal: {best_side} score={best_score} delta={delta*100:.3f}% "
        f"ref=${ref_price:,.0f} now=${current_price:,.0f} "
        f"@ {entry_price:.2f} T-{seconds_left:.0f}s"
    )
    return result


def kelly_size(balance: float, win_rate: float, confidence: str) -> float:
    from config import MAX_POSITION_PCT, MAX_POSITION_PCT_KELLY, MIN_TRADE_USD

    # Sanity check balance
    if balance <= 0 or balance > 100000:
        return MIN_TRADE_USD

    if win_rate < 0.52:
        win_rate = 0.55
    kelly_f = win_rate - (1 - win_rate)
    kelly_f = max(0.02, min(kelly_f, 0.5))

    multiplier = 1.5 if confidence == "HIGH" else 1.0
    size = balance * MAX_POSITION_PCT * kelly_f * multiplier
    size = min(size, balance * MAX_POSITION_PCT_KELLY)
    size = min(size, 25.0)  # Hard cap: max 5 per trade
    size = max(size, MIN_TRADE_USD)
    return round(size, 2)