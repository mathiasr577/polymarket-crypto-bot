"""
Estrategia basada en delta del precio vs precio de referencia del mercado.
- Ventana de entrada: T-45s a T-8s
- Rango de precio: 0.38 a 0.58 para ambos lados
- Delta mínimo: 0.08%
"""
import logging

logger = logging.getLogger(__name__)

DELTA_STRONG = 0.0015   # 0.15% → señal fuerte
DELTA_MEDIUM = 0.0008   # 0.08% → señal mínima aceptable
ENTRY_WINDOW_START = 45
ENTRY_WINDOW_END = 8

MIN_PRICE = 0.38
MAX_PRICE = 0.58

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

    if seconds_left > ENTRY_WINDOW_START:
        result["blocked"] = True
        result["block_reason"] = f"Too early: {seconds_left:.0f}s left (wait until T-45s)"
        return result

    if seconds_left < ENTRY_WINDOW_END:
        result["blocked"] = True
        result["block_reason"] = f"Too late: {seconds_left:.0f}s left"
        return result

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
        if delta > 0:
            votes["UP"] += 3
            reasons["UP"].append(f"Price +{delta*100:.3f}% above ref ${ref_price:,.0f} → UP locked")
        else:
            votes["DOWN"] += 3
            reasons["DOWN"].append(f"Price {delta*100:.3f}% below ref ${ref_price:,.0f} → DOWN locked")
    elif abs_delta >= DELTA_MEDIUM:
        if delta > 0:
            votes["UP"] += 2
            reasons["UP"].append(f"Price +{delta*100:.3f}% above ref → leaning UP")
        else:
            votes["DOWN"] += 2
            reasons["DOWN"].append(f"Price {delta*100:.3f}% below ref → leaning DOWN")
    else:
        result["blocked"] = True
        result["block_reason"] = f"Delta too small: {delta*100:.4f}% (need ±{DELTA_MEDIUM*100:.2f}%)"
        return result

    momentum = indicators.get("momentum", "neutral")
    pct_2min = indicators.get("pct_2min", 0)

    if momentum == "up" and pct_2min > 0 and delta > 0:
        votes["UP"] += 1
        reasons["UP"].append(f"Momentum confirms UP (+{pct_2min*100:.3f}%)")
    elif momentum == "down" and pct_2min < 0 and delta < 0:
        votes["DOWN"] += 1
        reasons["DOWN"].append(f"Momentum confirms DOWN ({pct_2min*100:.3f}%)")
    elif (momentum == "up" and delta < 0) or (momentum == "down" and delta > 0):
        if votes["UP"] > 0:
            votes["UP"] = max(0, votes["UP"] - 1)
        if votes["DOWN"] > 0:
            votes["DOWN"] = max(0, votes["DOWN"] - 1)

    up_price = market.get("up_price", 0.5)
    down_price = market.get("down_price", 0.5)

    best_side = "UP" if votes["UP"] >= votes["DOWN"] else "DOWN"
    token_price = up_price if best_side == "UP" else down_price

    if token_price > MAX_PRICE:
        result["blocked"] = True
        result["block_reason"] = f"{best_side} token too expensive: {token_price:.2f} (max {MAX_PRICE})"
        return result
    if token_price < MIN_PRICE:
        result["blocked"] = True
        result["block_reason"] = f"{best_side} token too cheap: {token_price:.2f} (min {MIN_PRICE})"
        return result

    best_score = max(votes["UP"], votes["DOWN"])

    if best_score < 2:
        result["blocked"] = True
        result["block_reason"] = f"Signal too weak: score={best_score}"
        return result

    token_id = market["tokens"].get(best_side)
    entry_price = token_price

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
    if balance <= 0 or balance > 100000:
        return MIN_TRADE_USD
    if win_rate < 0.52:
        win_rate = 0.55
    kelly_f = win_rate - (1 - win_rate)
    kelly_f = max(0.02, min(kelly_f, 0.5))
    multiplier = 1.5 if confidence == "HIGH" else 1.0
    size = balance * MAX_POSITION_PCT * kelly_f * multiplier
    size = min(size, balance * MAX_POSITION_PCT_KELLY)
    size = min(size, 25.0)
    size = max(size, MIN_TRADE_USD)
    return round(size, 2)