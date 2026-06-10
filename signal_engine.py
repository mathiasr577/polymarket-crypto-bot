import logging
from config import RSI_OVERSOLD, RSI_OVERBOUGHT, MAX_VOLATILITY_PCT

logger = logging.getLogger(__name__)

def generate_signal(indicators: dict, market: dict) -> dict:
    """
    For BTC/ETH Up or Down 5min markets.
    Simple: use RSI + EMA + Momentum to predict UP or DOWN.
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
        result["block_reason"] = "Warming up — no indicators yet"
        return result

    # Don't trade if less than 60 seconds left
    seconds_left = market.get("seconds_left", 300)
    if seconds_left < 60:
        result["blocked"] = True
        result["block_reason"] = f"Too close to expiry: {seconds_left:.0f}s left"
        return result

    # Don't trade if too volatile
    vol = indicators.get("volatility", 0)
    if vol > MAX_VOLATILITY_PCT:
        result["blocked"] = True
        result["block_reason"] = f"Too volatile: {vol*100:.2f}%"
        return result

    votes = {"UP": 0, "DOWN": 0}
    reasons = {"UP": [], "DOWN": []}

    # RSI
    rsi = indicators.get("rsi")
    if rsi is not None:
        if rsi < RSI_OVERSOLD:
            votes["UP"] += 1
            reasons["UP"].append(f"RSI={rsi:.1f} oversold")
        elif rsi > RSI_OVERBOUGHT:
            votes["DOWN"] += 1
            reasons["DOWN"].append(f"RSI={rsi:.1f} overbought")
        elif rsi < 45:
            votes["DOWN"] += 1
            reasons["DOWN"].append(f"RSI={rsi:.1f} bearish zone")
        elif rsi > 55:
            votes["UP"] += 1
            reasons["UP"].append(f"RSI={rsi:.1f} bullish zone")

    # EMA trend
    ema9 = indicators.get("ema9")
    ema21 = indicators.get("ema21")
    if ema9 and ema21:
        if ema9 > ema21:
            votes["UP"] += 1
            reasons["UP"].append("EMA9 > EMA21 uptrend")
        else:
            votes["DOWN"] += 1
            reasons["DOWN"].append("EMA9 < EMA21 downtrend")

    # Momentum
    momentum = indicators.get("momentum", "neutral")
    if momentum == "up":
        votes["UP"] += 1
        reasons["UP"].append("3 green candles momentum")
    elif momentum == "down":
        votes["DOWN"] += 1
        reasons["DOWN"].append("3 red candles momentum")

    # Evaluate
    up_score = votes["UP"]
    down_score = votes["DOWN"]

    if up_score == down_score:
        result["blocked"] = True
        result["block_reason"] = "Signals tied — no clear direction"
        return result

    best_side = "UP" if up_score > down_score else "DOWN"
    best_score = max(up_score, down_score)

    token_id = market["tokens"].get(best_side)
    entry_price = market["up_price"] if best_side == "UP" else market["down_price"]

    result["side"] = best_side
    result["score"] = best_score
    result["confidence"] = "HIGH" if best_score >= 3 else "MEDIUM" if best_score == 2 else "LOW"
    result["reasons"] = reasons[best_side]
    result["token_id"] = token_id
    result["entry_price"] = entry_price

    logger.info(
        f"Signal: {best_side} {market['asset'].upper()} "
        f"@ {entry_price:.2f} | {result['confidence']} | {result['reasons']}"
    )
    return result


def kelly_size(balance: float, win_rate: float, confidence: str) -> float:
    from config import MAX_POSITION_PCT, MAX_POSITION_PCT_KELLY, MIN_TRADE_USD

    if win_rate <= 0.5:
        win_rate = 0.52  # slight edge assumed
    kelly_f = win_rate - (1 - win_rate)
    kelly_f = max(0.01, min(kelly_f, 0.5))

    base = balance * MAX_POSITION_PCT
    multiplier = 1.5 if confidence == "HIGH" else 1.0 if confidence == "MEDIUM" else 0.5
    size = base * kelly_f * multiplier
    size = min(size, balance * MAX_POSITION_PCT_KELLY)
    size = max(size, MIN_TRADE_USD)
    return round(size, 2)