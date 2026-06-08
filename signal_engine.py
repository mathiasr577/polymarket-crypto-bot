import logging
from config import (
    RSI_OVERSOLD, RSI_OVERBOUGHT, MAX_VOLATILITY_PCT, MIN_INDICATORS
)

logger = logging.getLogger(__name__)

def generate_signal(indicators: dict) -> dict:
    """
    Returns:
    {
        "side": "UP" | "DOWN" | None,
        "confidence": "HIGH" | "MEDIUM" | None,
        "score": int (0-3),
        "reasons": [str],
        "blocked": bool,
        "block_reason": str
    }
    """
    result = {
        "side": None,
        "confidence": None,
        "score": 0,
        "reasons": [],
        "blocked": False,
        "block_reason": "",
    }

    if indicators is None:
        result["blocked"] = True
        result["block_reason"] = "No indicators yet (warming up)"
        return result

    vol = indicators.get("volatility", 0)
    if vol > MAX_VOLATILITY_PCT:
        result["blocked"] = True
        result["block_reason"] = f"Volatility too high: {vol*100:.2f}%"
        return result

    votes = {"UP": 0, "DOWN": 0}
    reasons = {"UP": [], "DOWN": []}

    # --- RSI ---
    rsi = indicators.get("rsi")
    if rsi is not None:
        if rsi < RSI_OVERSOLD:
            votes["UP"] += 1
            reasons["UP"].append(f"RSI={rsi:.1f} < {RSI_OVERSOLD} (oversold)")
        elif rsi > RSI_OVERBOUGHT:
            votes["DOWN"] += 1
            reasons["DOWN"].append(f"RSI={rsi:.1f} > {RSI_OVERBOUGHT} (overbought)")

    # --- EMA Cross ---
    ema9 = indicators.get("ema9")
    ema21 = indicators.get("ema21")
    ema9_prev = indicators.get("ema9_prev")
    ema21_prev = indicators.get("ema21_prev")
    if all(v is not None for v in [ema9, ema21, ema9_prev, ema21_prev]):
        was_below = ema9_prev < ema21_prev
        is_above = ema9 > ema21
        was_above = ema9_prev > ema21_prev
        is_below = ema9 < ema21

        if was_below and is_above:
            votes["UP"] += 1
            reasons["UP"].append(f"EMA9 crossed above EMA21")
        elif was_above and is_below:
            votes["DOWN"] += 1
            reasons["DOWN"].append(f"EMA9 crossed below EMA21")

    # --- Momentum ---
    momentum = indicators.get("momentum", "neutral")
    if momentum == "down":
        # 3 consecutive red candles → reversal → BUY UP
        votes["UP"] += 1
        reasons["UP"].append("3 red candles → reversal signal UP")
    elif momentum == "up":
        # 3 consecutive green candles → reversal → BUY DOWN
        votes["DOWN"] += 1
        reasons["DOWN"].append("3 green candles → reversal signal DOWN")

    # --- Evaluate ---
    up_score = votes["UP"]
    down_score = votes["DOWN"]
    best_side = "UP" if up_score >= down_score else "DOWN"
    best_score = max(up_score, down_score)
    conflict = up_score > 0 and down_score > 0

    if conflict:
        result["blocked"] = True
        result["block_reason"] = f"Conflicting signals: UP={up_score} DOWN={down_score}"
        return result

    if best_score < MIN_INDICATORS:
        result["blocked"] = True
        result["block_reason"] = f"Only {best_score} indicator(s) aligned (need {MIN_INDICATORS})"
        return result

    result["side"] = best_side
    result["score"] = best_score
    result["reasons"] = reasons[best_side]
    result["confidence"] = "HIGH" if best_score >= 3 else "MEDIUM"

    logger.info(f"Signal: {best_side} | {result['confidence']} | {result['reasons']}")
    return result


def kelly_size(balance: float, win_rate: float, confidence: str) -> float:
    """
    Kelly multiplier based on win_rate.
    confidence HIGH → up to 1.5x, MEDIUM → 1.0x
    """
    from config import MAX_POSITION_PCT, MAX_POSITION_PCT_KELLY, MIN_TRADE_USD

    # Kelly fraction: f = W - (1-W)/R where R = avg win/avg loss (assume 1:1 binary)
    if win_rate <= 0:
        win_rate = 0.5
    kelly_f = win_rate - (1 - win_rate)  # simplified for 1:1 payoff
    kelly_f = max(0.01, min(kelly_f, 1.0))

    base = balance * MAX_POSITION_PCT
    multiplier = 1.5 if confidence == "HIGH" else 1.0
    size = base * kelly_f * multiplier
    size = min(size, balance * MAX_POSITION_PCT_KELLY)
    size = max(size, MIN_TRADE_USD)
    return round(size, 2)
