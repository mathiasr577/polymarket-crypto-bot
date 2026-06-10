import logging
from config import MAX_VOLATILITY_PCT

logger = logging.getLogger(__name__)

def generate_signal(indicators: dict, market: dict) -> dict:
    """
    Strategy for BTC/ETH Up or Down 5min markets.
    
    Priority factors:
    1. Market edge: current price vs reference price (Polymarket data)
    2. Order flow: buy vs sell pressure on Binance
    3. Short-term momentum: last 2-3 minutes
    4. RSI short (6 period)
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
        result["block_reason"] = "Warming up"
        return result

    seconds_left = market.get("seconds_left", 300)
    if seconds_left < 60:
        result["blocked"] = True
        result["block_reason"] = f"Too close to expiry: {seconds_left:.0f}s"
        return result

    if seconds_left > 270:
        result["blocked"] = True
        result["block_reason"] = "Too early — wait until 30s into the market"
        return result

    vol = indicators.get("volatility", 0)
    if vol > MAX_VOLATILITY_PCT * 3:  # more lenient for 5min
        result["blocked"] = True
        result["block_reason"] = f"Extreme volatility: {vol*100:.2f}%"
        return result

    votes = {"UP": 0, "DOWN": 0}
    reasons = {"UP": [], "DOWN": []}

    # ── Factor 1: Market edge (most important) ──────────────────────────
    # Compare current price vs market reference price
    current_price = indicators.get("price")
    ref_price = market.get("ref_price")  # price at market open
    up_price = market.get("up_price", 0.5)
    down_price = market.get("down_price", 0.5)

    if ref_price and current_price:
        price_diff_pct = (current_price - ref_price) / ref_price

        if price_diff_pct > 0.001:
            # Price already UP vs reference → UP token is expensive, DOWN is cheap
            # If we think price keeps going up → UP
            # If we think reversal → DOWN
            # The market already shows this: down_price will be low
            if down_price < 0.2:
                # DOWN is very cheap → maybe worth a reversal bet
                votes["DOWN"] += 2
                reasons["DOWN"].append(f"DOWN token cheap ({down_price:.2f}) — reversal play")
            else:
                votes["UP"] += 1
                reasons["UP"].append(f"Price up {price_diff_pct*100:.2f}% vs ref → momentum UP")

        elif price_diff_pct < -0.001:
            # Price already DOWN vs reference
            if up_price < 0.2:
                votes["UP"] += 2
                reasons["UP"].append(f"UP token cheap ({up_price:.2f}) — reversal play")
            else:
                votes["DOWN"] += 1
                reasons["DOWN"].append(f"Price down {price_diff_pct*100:.2f}% vs ref → momentum DOWN")

    # ── Factor 2: Token price edge ──────────────────────────────────────
    # Only trade when there's clear mispricing
    # Best bet: token priced 30-45¢ (market uncertain, but we have an edge)
    if up_price < 0.45 and up_price > 0.15:
        votes["UP"] += 1
        reasons["UP"].append(f"UP token underpriced at {up_price:.2f}")
    elif down_price < 0.45 and down_price > 0.15:
        votes["DOWN"] += 1
        reasons["DOWN"].append(f"DOWN token underpriced at {down_price:.2f}")

    # ── Factor 3: Order flow (buy/sell pressure) ────────────────────────
    buy_ratio = indicators.get("buy_ratio", 0.5)
    if buy_ratio > 0.60:
        votes["UP"] += 2
        reasons["UP"].append(f"Buy pressure {buy_ratio*100:.0f}% → bullish flow")
    elif buy_ratio > 0.55:
        votes["UP"] += 1
        reasons["UP"].append(f"Slight buy pressure {buy_ratio*100:.0f}%")
    elif buy_ratio < 0.40:
        votes["DOWN"] += 2
        reasons["DOWN"].append(f"Sell pressure {buy_ratio*100:.0f}% → bearish flow")
    elif buy_ratio < 0.45:
        votes["DOWN"] += 1
        reasons["DOWN"].append(f"Slight sell pressure {buy_ratio*100:.0f}%")

    # ── Factor 4: Short-term momentum (last 2 min) ──────────────────────
    momentum = indicators.get("momentum", "neutral")
    pct_2min = indicators.get("pct_2min", 0)

    if momentum == "up" and pct_2min > 0:
        votes["UP"] += 1
        reasons["UP"].append(f"Momentum UP +{pct_2min*100:.2f}% last 2min")
    elif momentum == "down" and pct_2min < 0:
        votes["DOWN"] += 1
        reasons["DOWN"].append(f"Momentum DOWN {pct_2min*100:.2f}% last 2min")

    # ── Factor 5: RSI short (6 period) ──────────────────────────────────
    rsi = indicators.get("rsi", 50)
    if rsi > 70:
        votes["DOWN"] += 1
        reasons["DOWN"].append(f"RSI(6)={rsi:.0f} overbought")
    elif rsi < 30:
        votes["UP"] += 1
        reasons["UP"].append(f"RSI(6)={rsi:.0f} oversold")

    # ── Evaluate ─────────────────────────────────────────────────────────
    up_score = votes["UP"]
    down_score = votes["DOWN"]

    # Need clear majority
    if up_score == down_score:
        result["blocked"] = True
        result["block_reason"] = f"Tied signals UP={up_score} DOWN={down_score}"
        return result

    if max(up_score, down_score) < 2:
        result["blocked"] = True
        result["block_reason"] = f"Signal too weak (need 2+)"
        return result

    best_side = "UP" if up_score > down_score else "DOWN"
    best_score = max(up_score, down_score)

    token_id = market["tokens"].get(best_side)
    entry_price = market["up_price"] if best_side == "UP" else market["down_price"]

    # Don't bet on tokens already priced > 80¢ (little upside)
    if entry_price > 0.80:
        result["blocked"] = True
        result["block_reason"] = f"Token already expensive: {entry_price:.2f}"
        return result

    result["side"] = best_side
    result["score"] = best_score
    result["confidence"] = "HIGH" if best_score >= 4 else "MEDIUM" if best_score >= 2 else "LOW"
    result["reasons"] = reasons[best_side]
    result["token_id"] = token_id
    result["entry_price"] = entry_price

    logger.info(
        f"✅ Signal: {best_side} {market['asset'].upper()} "
        f"score={best_score} @ {entry_price:.2f} | {result['reasons']}"
    )
    return result


def kelly_size(balance: float, win_rate: float, confidence: str) -> float:
    from config import MAX_POSITION_PCT, MAX_POSITION_PCT_KELLY, MIN_TRADE_USD

    if win_rate < 0.52:
        win_rate = 0.52
    kelly_f = win_rate - (1 - win_rate)
    kelly_f = max(0.01, min(kelly_f, 0.4))

    multiplier = 1.5 if confidence == "HIGH" else 1.0 if confidence == "MEDIUM" else 0.5
    size = balance * MAX_POSITION_PCT * kelly_f * multiplier
    size = min(size, balance * MAX_POSITION_PCT_KELLY)
    size = max(size, MIN_TRADE_USD)
    return round(size, 2)