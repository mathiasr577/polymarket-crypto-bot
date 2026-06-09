import logging
from config import RSI_OVERSOLD, RSI_OVERBOUGHT, MAX_VOLATILITY_PCT, MIN_INDICATORS

logger = logging.getLogger(__name__)

def generate_signal(indicators: dict, market: dict) -> dict:
    """
    For price markets like "Will BTC be above $65k by Friday?"
    
    Logic:
    - market["direction"] = "up" means YES token wins if price goes UP
    - We use technicals to decide if price is likely going UP or DOWN
    - Match technical direction with market direction to pick YES or NO

    Returns:
    {
        "side": "YES" | "NO" | None,
        "confidence": "HIGH" | "MEDIUM" | None,
        "score": int,
        "reasons": [str],
        "blocked": bool,
        "block_reason": str,
        "token_id": str,
        "entry_price": float,
    }
    """
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
        result["block_reason"] = "No indicators yet (warming up)"
        return result

    # Skip if too volatile
    vol = indicators.get("volatility", 0)
    if vol > MAX_VOLATILITY_PCT:
        result["blocked"] = True
        result["block_reason"] = f"Volatility too high: {vol*100:.2f}%"
        return result

    # Skip if token prices are too extreme (already priced in)
    yes_price = market.get("yes_price", 0.5)
    no_price = market.get("no_price", 0.5)
    if yes_price > 0.85 or yes_price < 0.15:
        result["blocked"] = True
        result["block_reason"] = f"Market already decided: YES={yes_price:.2f}"
        return result

    # Check distance from price target
    current_price = indicators.get("price")
    price_target = market.get("price_target")
    market_direction = market.get("direction", "up")

    distance_signal = None
    if current_price and price_target:
        distance_pct = (price_target - current_price) / current_price
        if market_direction == "up":
            # YES wins if price goes UP to target
            if distance_pct < -0.05:
                # Already 5%+ above target → YES very likely
                distance_signal = "up"
                result["reasons"].append(f"Price ${current_price:,.0f} already above target ${price_target:,.0f}")
            elif distance_pct > 0.15:
                # More than 15% away → very unlikely → NO
                distance_signal = "down"
                result["reasons"].append(f"Target ${price_target:,.0f} is {distance_pct*100:.1f}% away → unlikely")
        else:
            # YES wins if price goes DOWN
            if distance_pct > 0.05:
                distance_signal = "down"
                result["reasons"].append(f"Price ${current_price:,.0f} already below target ${price_target:,.0f}")

    # Technical signals
    votes = {"up": 0, "down": 0}
    reasons = {"up": [], "down": []}

    # RSI
    rsi = indicators.get("rsi")
    if rsi is not None:
        if rsi < RSI_OVERSOLD:
            votes["up"] += 1
            reasons["up"].append(f"RSI={rsi:.1f} oversold → bounce likely")
        elif rsi > RSI_OVERBOUGHT:
            votes["down"] += 1
            reasons["down"].append(f"RSI={rsi:.1f} overbought → pullback likely")

    # EMA Cross
    ema9 = indicators.get("ema9")
    ema21 = indicators.get("ema21")
    ema9_prev = indicators.get("ema9_prev")
    ema21_prev = indicators.get("ema21_prev")
    if all(v is not None for v in [ema9, ema21, ema9_prev, ema21_prev]):
        if ema9_prev < ema21_prev and ema9 > ema21:
            votes["up"] += 1
            reasons["up"].append("EMA9 crossed above EMA21 → bullish")
        elif ema9_prev > ema21_prev and ema9 < ema21:
            votes["down"] += 1
            reasons["down"].append("EMA9 crossed below EMA21 → bearish")
        elif ema9 > ema21:
            votes["up"] += 1
            reasons["up"].append(f"EMA9 > EMA21 → uptrend")
        else:
            votes["down"] += 1
            reasons["down"].append(f"EMA9 < EMA21 → downtrend")

    # Momentum
    momentum = indicators.get("momentum", "neutral")
    if momentum == "down":
        votes["up"] += 1
        reasons["up"].append("3 red candles → reversal UP likely")
    elif momentum == "up":
        votes["down"] += 1
        reasons["down"].append("3 green candles → reversal DOWN likely")

    # Distance signal counts as one vote
    if distance_signal:
        votes[distance_signal] += 1

    # Evaluate
    up_score = votes["up"]
    down_score = votes["down"]
    conflict = up_score > 0 and down_score > 0 and abs(up_score - down_score) <= 1

    if conflict:
        result["blocked"] = True
        result["block_reason"] = f"Conflicting signals UP={up_score} DOWN={down_score}"
        return result

    best_direction = "up" if up_score >= down_score else "down"
    best_score = max(up_score, down_score)

    if best_score < MIN_INDICATORS:
        result["blocked"] = True
        result["block_reason"] = f"Only {best_score} indicator(s) aligned (need {MIN_INDICATORS})"
        return result

    # Map technical direction → YES or NO based on market direction
    if market_direction == "up":
        side = "YES" if best_direction == "up" else "NO"
    else:
        side = "YES" if best_direction == "down" else "NO"

    token_id = market["tokens"].get(side)
    entry_price = yes_price if side == "YES" else no_price

    result["side"] = side
    result["score"] = best_score
    result["confidence"] = "HIGH" if best_score >= 3 else "MEDIUM"
    result["reasons"] = reasons[best_direction]
    result["token_id"] = token_id
    result["entry_price"] = entry_price

    logger.info(
        f"Signal: {side} on '{market['title'][:50]}' "
        f"@ {entry_price:.2f} | {result['confidence']} | {result['reasons']}"
    )
    return result


def kelly_size(balance: float, win_rate: float, confidence: str) -> float:
    from config import MAX_POSITION_PCT, MAX_POSITION_PCT_KELLY, MIN_TRADE_USD

    if win_rate <= 0:
        win_rate = 0.5
    kelly_f = win_rate - (1 - win_rate)
    kelly_f = max(0.01, min(kelly_f, 1.0))

    base = balance * MAX_POSITION_PCT
    multiplier = 1.5 if confidence == "HIGH" else 1.0
    size = base * kelly_f * multiplier
    size = min(size, balance * MAX_POSITION_PCT_KELLY)
    size = max(size, MIN_TRADE_USD)
    return round(size, 2)