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

- Ventana de entrada: T-120s a T-55s (antes T-120s a T-8s; calibrado con 3
  días de trading real bajo el modelo con drift/EWMA — ver más abajo)
- Precio mínimo: 0.45 (antes 0.25; por debajo de 0.45 el win rate real fue
  5-48%, muy por debajo del breakeven)
- Filtro real: p_modelo >= max(breakeven(precio) + EDGE_MARGIN, MIN_MODEL_PROB)

ENTRY_WINDOW_END subió de 8 a 55: order_executor.py deja la orden límite
abierta hasta 55s esperando que se llene (time.sleep(55)) antes de
cancelarla. Si se genera una señal con menos de 55s restantes y la orden
no se llena al instante, el código intenta esperar más tiempo del que le
queda al mercado — cualquier fill que ocurra ahí pasa en los segundos más
caóticos y con menos liquidez antes del cierre. En los 3 días de datos
reales (1-3 agosto) el PnL promedio por trade mejora de forma monótona a
medida que se recorta esta cola, con pico exactamente en 55s (+0.562 vs
+0.235 en el corte original de 8s) y empeora de nuevo pasados los 55s —
coincide con la constante del ejecutor, no es ruido de muestra.

El modelo browniano puro no tiene drift: asume que el precio futuro está
centrado en el precio actual. Eso lo deja ciego a tendencias sostenidas
(ej. una racha alcista de 15 min), sobre todo justo al abrir cada ventana
de 5 min, cuando ref_price recién se resetea y el delta acumulado es ~0
aunque la tendencia previa siga en curso. Para corregir esto, se estima
un drift reciente (regresión de precio sobre los últimos ~12 min) y se
suma como término adicional al z-score:

    z = delta/(sigma*sqrt(t)) + DRIFT_WEIGHT * clip(drift*sqrt(t)/sigma, ±MAX_DRIFT_Z)

DRIFT_WEIGHT y MAX_DRIFT_Z están para no confiar el 100% en esta señal:
a diferencia de los demás parámetros de este archivo, este ajuste no se
pudo validar contra trades reales (no hay historial de precio tick a tick
en los exports de Polymarket, solo resultados de mercado) — son un punto
de partida conservador, no un valor calibrado.

El drift corto (12 min) tampoco alcanza cuando la tendencia viene sostenida
por horas: el 8 y 9 de agosto las apuestas DOWN perdieron sistemáticamente
(25% de win rate ambos días) contra una tendencia alcista que el horizonte
corto no podía ver. Se agregó un segundo término con horizonte largo
(~90 min, DRIFT_WEIGHT_LONG/MAX_DRIFT_Z_LONG) que se suma de la misma forma.
"""
import math
import logging

logger = logging.getLogger(__name__)

ENTRY_WINDOW_START = 120
ENTRY_WINDOW_END = 55

MIN_PRICE = 0.45
MAX_PRICE = 0.80

PLATFORM_FEE = 0.07
EDGE_MARGIN = 0.10          # puntos de probabilidad exigidos por encima del breakeven
MIN_MODEL_PROB = 0.62       # piso absoluto: nunca operar si el modelo apenas roza 50/50,
                             # aunque el precio barato haga que el breakeven sea aún más bajo
                             # (el modelo es una aproximación gaussiana con pocas muestras de
                             # volatilidad — este piso evita operar ruido solo porque las
                             # cuotas son generosas)
CROSS_ASSET_BONUS = 0.015

DRIFT_WEIGHT = 0.5   # cuánto se confía el ajuste de tendencia corto (0=ignorar, 1=full)
MAX_DRIFT_Z = 1.0    # tope al aporte del drift corto al z-score, para que no pueda
                      # por sí solo voltear una señal fuerte en contra

# Drift de horizonte largo (~90 min, ver price_feed.py). Agregado el 10 ago:
# el horizonte corto (12 min) no alcanza a detectar una tendencia sostenida
# de horas — el 8 y 9 de agosto las apuestas DOWN perdieron sistemáticamente
# contra una tendencia alcista que venía de mucho antes de esos 12 min.
# Peso y tope más generosos que el corto porque una tendencia confirmada por
# 90 min de datos es evidencia más sólida que un vistazo de 12 min — pero
# sigue siendo un ajuste sin validar contra trades reales todavía.
DRIFT_WEIGHT_LONG = 0.6
MAX_DRIFT_Z_LONG = 1.5

# Freno por lado basado en resultados reales recientes (no en el precio):
# complementa al drift — si un lado viene perdiendo mucho más de lo que
# debería, exige más edge en vez de confiar en que el modelo de precio ya
# lo está compensando. Requiere una muestra mínima antes de aplicarse para
# no reaccionar a 2-3 trades de ruido.
SIDE_DAMPENER_MIN_TRADES = 15
SIDE_DAMPENER_THRESHOLD = 0.50
SIDE_DAMPENER_MAX_PENALTY = 0.15
SIDE_DAMPENER_SCALE = 0.6


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
        "z": None,  # expuesto para el shadow-mode logger (comparar contra el modelo TWAP)
        "raw_side": None,  # hacia dónde se inclina el modelo AUNQUE termine bloqueado —
                            # "side" solo se llena si pasa todos los filtros (precio, edge),
                            # así que por sí solo no sirve para comparar contra una señal
                            # cruda (TWAP delta) sobre la misma población de mercados.
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

    drift_note = ""
    drift = indicators.get("trend_drift_per_sec")
    if drift is not None:
        raw_drift_z = drift * math.sqrt(max(seconds_left, 1)) / sigma
        drift_z = max(-MAX_DRIFT_Z, min(MAX_DRIFT_Z, raw_drift_z)) * DRIFT_WEIGHT
        z += drift_z
        drift_note = f" drift_z={drift_z:+.2f}"

    drift_long = indicators.get("trend_drift_long_per_sec")
    if drift_long is not None:
        raw_drift_long_z = drift_long * math.sqrt(max(seconds_left, 1)) / sigma
        drift_long_z = max(-MAX_DRIFT_Z_LONG, min(MAX_DRIFT_Z_LONG, raw_drift_long_z)) * DRIFT_WEIGHT_LONG
        z += drift_long_z
        drift_note += f" drift_long_z={drift_long_z:+.2f}"

    p_up = _norm_cdf(z)
    result["z"] = z

    best_side = "UP" if p_up >= 0.5 else "DOWN"
    p_model = p_up if best_side == "UP" else (1 - p_up)
    result["raw_side"] = best_side

    reasons = [
        f"z={z:+.2f}{drift_note} delta={delta*100:+.3f}% sigma_rem={sigma_remaining*100:.3f}% "
        f"-> model P({best_side})={p_model*100:.1f}%"
    ]

    cross_confirm = indicators.get("cross_asset_confirm")
    if cross_confirm == best_side:
        p_model = min(0.97, p_model + CROSS_ASSET_BONUS)
        reasons.append(f"Other asset also leans {best_side} (+{CROSS_ASSET_BONUS*100:.1f}pp)")

    side_stats = (indicators.get("side_recent_win_rate") or {}).get(best_side)
    if side_stats:
        recent_wr, n_samples = side_stats
        if (
            n_samples >= SIDE_DAMPENER_MIN_TRADES
            and recent_wr is not None
            and recent_wr < SIDE_DAMPENER_THRESHOLD
        ):
            penalty = min(SIDE_DAMPENER_MAX_PENALTY, (SIDE_DAMPENER_THRESHOLD - recent_wr) * SIDE_DAMPENER_SCALE)
            p_model = max(0.50, p_model - penalty)
            reasons.append(
                f"{best_side} recent win rate {recent_wr*100:.0f}% (n={n_samples}) -> penalty -{penalty*100:.1f}pp"
            )

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