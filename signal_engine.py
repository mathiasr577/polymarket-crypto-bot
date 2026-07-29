"""
Estrategia: modelo de probabilidad ajustado por tiempo y volatilidad realizada.

En vez de un delta fijo + banda de precio fija, esto estima P(gana) con una
aproximación de movimiento browniano:

    z = delta_acumulado / (sigma_realizada * sqrt(tiempo_restante))
    p_modelo = CDF_normal(z)

y opera cuando esa probabilidad supera el breakeven implícito por el precio
del token (ajustado por el fee del 7%) más un margen de seguridad. Esto
reemplaza la banda de precio fija (que rechazaba señales fuertes por caras Y
señales tempranas por no tener delta suficiente) por un filtro de edge que
funciona a cualquier precio dentro de los límites de liquidez/ejecución.

- Ventana de entrada: T-120s a T-8s (antes T-180s; calibrado con 3 días de
  trading real: el bucket T-150/180s concentraba ~la mitad del volumen y
  explicaba casi toda la pérdida neta del período)
- Precio mínimo: 0.45 (antes 0.25; por debajo de 0.45 el win rate real fue
  5-48%, muy por debajo del breakeven)
- Filtro real: p_modelo >= max(breakeven(precio) + EDGE_MARGIN, MIN_MODEL_PROB)
"""
import math
import logging

logger = logging.getLogger(__name__)

ENTRY_WINDOW_START = 120
ENTRY_WINDOW_END = 8

MIN_PRICE = 0.45
MAX_PRICE = 0.80

PLATFORM_FEE = 0.07
EDGE_MARGIN = 0.10          # puntos de probabilidad exigidos por encima del breakeven
MIN_MODEL_PROB = 0.62       # piso absoluto: nunca operar si el modelo apenas roza 50/50,
                             # aunque el precio barato haga que el breakeven sea aún más bajo
                             # (el modelo es una aproximación gaussiana con pocas muestras de
                             # volatilidad — este piso evita operar ruido solo porque las
                             # cuotas son generosas)
MOMENTUM_BONUS = 0.02
CROSS_ASSET_BONUS = 0.015


def _norm_cdf(z: float) -> float:
    return 0.5 * (1 + math.erf(z / math.sqrt(2)))


def _breakeven(price: float) -> float:
    """Probabilidad de ganar necesaria para EV=0 a este precio, neto del fee."""
    return price / (price + (1 - PLATFORM_FEE) * (1 - price))


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
        "model_prob": None,
    }

    if indicators is None:
        result["blocked"] = True
        result["block_reason"] = "No indicators yet"
        return result

    seconds_left = market.get("seconds_left", 300)

    if seconds_left > ENTRY_WINDOW_START:
        result["blocked"] = True
        result["block_reason"] = f"Too early: {seconds_left:.0f}s left (wait until T-{ENTRY_WINDOW_START}s)"
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

    sigma = indicators.get("vol_per_sqrt_sec")
    if not sigma or sigma <= 0:
        result["blocked"] = True
        result["block_reason"] = "Not enough volatility data yet"
        return result

    sigma_remaining = sigma * math.sqrt(max(seconds_left, 1))
    z = delta / sigma_remaining
    p_up = _norm_cdf(z)

    best_side = "UP" if p_up >= 0.5 else "DOWN"
    p_model = p_up if best_side == "UP" else (1 - p_up)

    reasons = [
        f"z={z:+.2f} delta={delta*100:+.3f}% sigma_rem={sigma_remaining*100:.3f}% "
        f"-> model P({best_side})={p_model*100:.1f}%"
    ]

    momentum = indicators.get("momentum", "neutral")
    if (momentum == "up" and best_side == "UP") or (momentum == "down" and best_side == "DOWN"):
        p_model = min(0.97, p_model + MOMENTUM_BONUS)
        reasons.append(f"Momentum confirms {best_side} (+{MOMENTUM_BONUS*100:.1f}pp)")
    elif (momentum == "up" and best_side == "DOWN") or (momentum == "down" and best_side == "UP"):
        p_model = max(0.5, p_model - MOMENTUM_BONUS)
        reasons.append(f"Momentum contradicts {best_side} (-{MOMENTUM_BONUS*100:.1f}pp)")

    cross_confirm = indicators.get("cross_asset_confirm")
    if cross_confirm == best_side:
        p_model = min(0.97, p_model + CROSS_ASSET_BONUS)
        reasons.append(f"Other asset also leans {best_side} (+{CROSS_ASSET_BONUS*100:.1f}pp)")

    up_price = market.get("up_price", 0.5)
    down_price = market.get("down_price", 0.5)
    token_price = up_price if best_side == "UP" else down_price

    if token_price > MAX_PRICE:
        result["blocked"] = True
        result["block_reason"] = f"{best_side} token too expensive: {token_price:.2f} (max {MAX_PRICE})"
        return result
    if token_price < MIN_PRICE:
        result["blocked"] = True
        result["block_reason"] = f"{best_side} token too cheap: {token_price:.2f} (min {MIN_PRICE})"
        return result

    breakeven = _breakeven(token_price)
    required = max(breakeven + EDGE_MARGIN, MIN_MODEL_PROB)

    if p_model < required:
        result["blocked"] = True
        result["block_reason"] = (
            f"Edge insufficient: model={p_model*100:.1f}% "
            f"breakeven={breakeven*100:.1f}% need={required*100:.1f}%"
        )
        return result

    edge = p_model - breakeven
    token_id = market["tokens"].get(best_side)

    result["side"] = best_side
    result["score"] = round(edge * 100, 2)
    result["confidence"] = "HIGH" if edge >= 2 * EDGE_MARGIN else "MEDIUM"
    result["reasons"] = reasons
    result["token_id"] = token_id
    result["entry_price"] = token_price
    result["model_prob"] = p_model

    logger.info(
        f"✅ Signal: {best_side} model={p_model*100:.1f}% breakeven={breakeven*100:.1f}% "
        f"edge={edge*100:.1f}pp @ {token_price:.2f} T-{seconds_left:.0f}s "
        f"ref=${ref_price:,.0f} now=${current_price:,.0f}"
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