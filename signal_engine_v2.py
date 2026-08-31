"""
Motor de señal v2 — basado en TWAP de Chainlink en vez de spot de Kraken.

Construido a partir del backtest sobre datos reales recolectados por
shadow_logger.py (ver conversación, 13-15 ago 2026): el modelo viejo
(signal_engine.py, Kraken + z-score + drift + dampener) está mal calibrado
justo en su rango de trading configurado (precio 0.55-0.85: se cree
86-95% seguro, acierta 56-77% real en los datos). En vez de intentar
arreglar esa fórmula de confianza teórica con la poca muestra que hay por
banda, este motor usa una política más simple y menos propensa a
sobreajuste: seguir la dirección cruda del TWAP de 60s (¿subió o bajó
desde la apertura de la ventana de 5 min?) y tradear SOLO en las bandas de
precio que el backtest ya mostró que superan el breakeven con margen real
— evitando por completo la zona 0.55-0.85 donde ni TWAP ni el modelo viejo
funcionan hoy.

No usa ninguna fórmula de probabilidad teórica (ni campana de Gauss ni
nada) — la política ES la calibración. Deliberado: con ~550-1000 muestras
repartidas en 6 bandas de precio, ajustar una curva de probabilidad
continua se sobreajusta fácil; una política binaria simple por banda es
más robusta con la muestra que hay hoy.

Referencia de la evidencia detrás de estos números — ver en el dashboard:
/api/shadow-v2-sim y /api/shadow-model-comparison.
"""
import logging

logger = logging.getLogger(__name__)

# Banda barata: sin filtro de magnitud, ~51% de acierto real (183 muestras)
# — pero ese promedio escondía que 3 de cada 4 señales (las de movimiento
# de TWAP60 más chico) rendían ~41%, peor que moneda al aire, y solo el
# cuartil de movimientos más GRANDES rendía 78%. Confirmado en
# /api/shadow-magnitude y /api/shadow-filtered-sim (15-ago-2026): exigir un
# delta relativo mínimo sube el acierto real de 51%→61% mientras conserva
# casi toda la plata total (+$219 vs +$228 sin filtro, con menos de la
# mitad de los trades) — mucho más margen de seguridad contra costos de
# ejecución reales que el backtest no modela. MIN_REL_DELTA_CHEAP es ese
# umbral (el "laxo" del backtest, no el más estricto — ese preserva más
# plata total aunque tenga menos acierto por trade).
#
# Banda favorita: ~96% de acierto, margen fino (+$0.16/trade, ~3.2% de
# retorno) pero confirmado estable en 3 mediciones independientes seguidas
# (n=210→385→393). El usuario marcó algo importante mirando los números:
# tratar 0.85-1.00 como una sola banda escondía que el breakeven sube de
# ~88% a ~98% adentro de ese rango. Sub-dividiéndola (/api/shadow-
# favorite-detail, 15-ago-2026) se ve que 0.85-0.97 le gana al breakeven
# con margen real en todos los tramos (+$61 de $63 totales), pero
# 0.97-0.99 está literalmente empatada con su propio breakeven (98.1%
# real vs 98.14% necesario) — arriesgar $5 para ganar $0.09 en 104 casos,
# solo +$2.10 total. FAVORITE_MAX corta esa cola sin filo; 0.99-1.00 tiene
# muestra insuficiente (n=3) para confiar en cualquier sentido.
#
# FAVORITE_MIN bajado de 0.85 a 0.75 (16-ago-2026): con más datos (1837
# mercados resueltos), la banda 0.75-0.85 empezó a ganarle al breakeven
# también (84% real vs 81% necesario) — confirmado con backtest de plata
# real, no solo el % de acierto: +$52.27 en 254 trades, $0.206/trade en
# promedio (mejor que el promedio de la banda 0.85-0.97, $0.19/trade). Ver
# /api/shadow-favorite-extension.
#
# FAVORITE_MIN subido de 0.75 a 0.80 (29-ago-2026): favorite se comprimió
# ~90% en $/trade en los últimos 3 días reales (dos días seguidos en rojo
# con plata real), pero NO parejo — /api/shadow-favorite-recency-subband
# mostró que el tramo 0.75-0.80 específicamente se dio vuelta (72.8%
# reciente vs 82.7% histórico, -$0.35/trade reciente vs +$0.27 histórico,
# n=92) mientras 0.80-0.97 seguía sano (~$0.058/trade reciente sacando
# ese tramo). Justo el tramo pegado al límite con mid_confirmed, que sí
# exige 2+ confirmaciones — este no exigía ninguna. Se saca ese tramo en
# vez de pausar toda la banda; 0.93-0.95 también mostró debilidad
# reciente pero con muestra más chica (n=126) y sin una frontera natural
# para recortarlo limpio — queda para seguir vigilando, no se tocó.
# CHEAP_MIN agregado (31-ago-2026): cheap nunca tuvo piso de precio, solo
# techo — cualquier precio de 0 a 0.55 calificaba. Revisando paper_v2 por
# rango de precio en la última semana (n=451): el tramo 0.00-0.20 es el
# ÚNICO con avg_pnl NEGATIVO (-$3.93/trade, 10.0% acierto, n=20) — todos
# los demás (0.20-0.35: +$3.65/n=47, 0.35-0.45: +$4.52/n=59, 0.45-0.55:
# +$6.81/n=325, la gran mayoría del volumen) son rentables pese a acierto
# más bajo que el resto de la banda, porque el pago compensa. Con precio
# tan bajo el mercado ya está prácticamente seguro del lado contrario —
# apostarle en contra ahí no tiene edge real, solo parece "barato". Se
# corta ese tramo específico, se deja el resto de la banda intacto.
CHEAP_MIN = 0.20
CHEAP_MAX = 0.55
FAVORITE_MIN = 0.80
FAVORITE_MAX = 0.97
MIN_REL_DELTA_CHEAP = 0.0001
TRADE_FAVORITE_BAND = True

# Zona media 0.55-0.75 (17-ago-2026): TWAP60 solo NO le gana al breakeven
# acá (ver conversación), pero se encontró que tres señales adicionales
# (D_lead = spot vs TWAP60 ahora; OFI de Bybit = compras/ventas agresivas
# reales, no order book inferido; presión TWAP = integral en el tiempo de
# spot-TWAP60) son mediocres SOLAS pero excelentes CONFIRMANDO la
# dirección de TWAP60 (86-89% de acierto cuando coinciden, vs 80% de TWAP60
# solo — ver /api/shadow-lead-agreement, /api/shadow-ofi,
# /api/shadow-pressure). Contando cuántas de las 3 coinciden con TWAP60 en
# ESTA zona específica (/api/shadow-confirmation): 0 confirmaciones pierde
# (-$73.58, 106 trades), 2+ gana con margen real (+$28.84 a 69.5% con 2,
# +$44.36 a 78.6% con 3 — el mejor retorno por trade de todo el dataset).
# MID_ZONE_MIN_CONFIRMATIONS exige al menos 2 de 3.
MID_ZONE_MIN = 0.55
MID_ZONE_MAX = 0.75
MID_ZONE_MIN_CONFIRMATIONS = 2
TRADE_MID_ZONE = True

# Sizing por confirmaciones (19-ago-2026): get_confirmation_report() mostró
# que más confirmaciones predicen mejor calidad TAMBIÉN dentro de las
# bandas que ya tradeamos (66.8% con 0, 92.3% con 3) — no solo sirve para
# decidir SI tradear la zona media. Confirmado con backtest de plata real
# en /api/shadow-sizing: escalar el tamaño 0.5x por confirmación casi
# duplica la plata total vs. apostar siempre lo mismo ($2735 vs $1384 en
# 2154 trades). Se eligió el multiplicador MODERADO (0.5x) sobre uno más
# agresivo (1x, que backtesteaba incluso mejor) a propósito: un backtest
# siempre se va a ver mejor escalando más fuerte hacia el subconjunto que
# ya sabemos que rinde mejor — eso no lo hace la elección más sensata para
# arrancar, sobre todo con la ejecución real todavía sin probar. Empieza
# aplicándose solo en paper (paper_trader_v2.py), no en plata real todavía.
STAKE_MULTIPLIER_PER_CONFIRMATION = 0.5

ENTRY_WINDOW_START = 120   # mismos límites de tiempo que v1 — los datos de
ENTRY_WINDOW_END = 55      # shadow_logger se recolectaron en esta ventana,
                            # así que el backtest solo es válido si v2 opera
                            # en la misma ventana.


def _count_confirmations(chainlink_snapshot: dict, twap_side: str) -> tuple[int, str]:
    """Cuenta cuántas de {D_lead, OFI 15s, presión TWAP} coinciden con la
    dirección de TWAP60. chainlink_snapshot debe traer chainlink_spot,
    twap60_now, ofi_15s y pressure_integral (armado en main.py)."""
    count = 0
    parts = []

    spot = chainlink_snapshot.get("chainlink_spot")
    twap60_now = chainlink_snapshot.get("twap60_now")
    if spot is not None and twap60_now is not None:
        lead_diff = spot - twap60_now
        if lead_diff != 0:
            lead_side = "UP" if lead_diff > 0 else "DOWN"
            if lead_side == twap_side:
                count += 1
                parts.append("lead✓")
            else:
                parts.append("lead✗")

    ofi = chainlink_snapshot.get("ofi_15s")
    if ofi is not None and ofi != 0:
        ofi_side = "UP" if ofi > 0 else "DOWN"
        if ofi_side == twap_side:
            count += 1
            parts.append("ofi✓")
        else:
            parts.append("ofi✗")

    pressure = chainlink_snapshot.get("pressure_integral")
    if pressure is not None and pressure != 0:
        pressure_side = "UP" if pressure > 0 else "DOWN"
        if pressure_side == twap_side:
            count += 1
            parts.append("pressure✓")
        else:
            parts.append("pressure✗")

    return count, " ".join(parts) if parts else "sin datos de confirmación"


def generate_signal_v2(chainlink_snapshot: dict, market: dict) -> dict:
    """chainlink_snapshot: dict con twap60_open (apertura de la ventana
    actual, de ChainlinkFeed.get_window_twap), twap60_now/chainlink_spot/
    feed_connected (de ChainlinkFeed.get_snapshot), y opcionalmente
    ofi_15s (de OrderFlowFeed.get_ofi) y pressure_integral (de
    ChainlinkFeed.get_pressure) — se arma en main.py antes de llamar acá,
    igual que se arma `indicators` para el modelo viejo."""
    result = {
        "side": None,
        "confidence": None,
        "reasons": [],
        "blocked": False,
        "block_reason": "",
        "token_id": None,
        "entry_price": None,
        "band": None,
        "confirmations": None,
        "stake_multiplier": 1.0,
    }

    seconds_left = market.get("seconds_left", 300)
    if seconds_left > ENTRY_WINDOW_START:
        result["blocked"] = True
        result["block_reason"] = f"Too early: {seconds_left:.0f}s left"
        return result
    if seconds_left < ENTRY_WINDOW_END:
        result["blocked"] = True
        result["block_reason"] = f"Too late: {seconds_left:.0f}s left"
        return result

    if not chainlink_snapshot.get("feed_connected"):
        result["blocked"] = True
        result["block_reason"] = "Chainlink feed not connected"
        return result

    twap60_open = chainlink_snapshot.get("twap60_open")
    twap60_now = chainlink_snapshot.get("twap60_now")
    if twap60_open is None or twap60_now is None:
        result["blocked"] = True
        result["block_reason"] = "No TWAP60 data yet for this window"
        return result

    diff = twap60_now - twap60_open
    if diff == 0:
        result["blocked"] = True
        result["block_reason"] = "TWAP60 flat, no direction"
        return result

    side = "UP" if diff > 0 else "DOWN"

    up_price = market.get("up_price")
    down_price = market.get("down_price")
    price = up_price if side == "UP" else down_price
    if price is None:
        result["blocked"] = True
        result["block_reason"] = "No price available"
        return result

    # Se cuenta siempre, no solo para la zona media — sirve para sizing en
    # cualquier banda (ver STAKE_MULTIPLIER_PER_CONFIRMATION arriba).
    confirmations, confirm_detail = _count_confirmations(chainlink_snapshot, side)

    if price < CHEAP_MIN:
        result["blocked"] = True
        result["block_reason"] = (
            f"Price {price:.2f} below CHEAP_MIN ({CHEAP_MIN}) — tramo con avg_pnl "
            f"negativo en datos reales, ver comentario junto a CHEAP_MIN"
        )
        return result
    elif price < CHEAP_MAX:
        band = "cheap"
        rel_delta = abs(diff / twap60_open) if twap60_open else 0
        if rel_delta < MIN_REL_DELTA_CHEAP:
            result["blocked"] = True
            result["block_reason"] = (
                f"TWAP60 move too small in cheap band: rel_delta={rel_delta:.6f} "
                f"< {MIN_REL_DELTA_CHEAP} (ver /api/shadow-filtered-sim)"
            )
            return result
    elif FAVORITE_MIN <= price < FAVORITE_MAX:
        band = "favorite"
    elif price >= FAVORITE_MAX:
        result["blocked"] = True
        result["block_reason"] = (
            f"Price {price:.2f} in favorite band's dead-weight tail "
            f"(>={FAVORITE_MAX}, breakeven ~empatado, ver /api/shadow-favorite-detail)"
        )
        return result
    elif MID_ZONE_MIN <= price < MID_ZONE_MAX:
        band = "mid_confirmed"
        if not TRADE_MID_ZONE:
            result["blocked"] = True
            result["block_reason"] = "Mid zone (0.55-0.75) deshabilitada"
            return result
        if confirmations < MID_ZONE_MIN_CONFIRMATIONS:
            result["blocked"] = True
            result["block_reason"] = (
                f"Mid zone necesita {MID_ZONE_MIN_CONFIRMATIONS}+ confirmaciones "
                f"(lead/ofi/pressure de acuerdo con TWAP60), tiene {confirmations} "
                f"({confirm_detail}) — ver /api/shadow-confirmation"
            )
            return result
    else:
        result["blocked"] = True
        result["block_reason"] = f"Price {price:.2f} in excluded band (sin ventaja probada)"
        return result

    if band == "favorite" and not TRADE_FAVORITE_BAND:
        result["blocked"] = True
        result["block_reason"] = "Favorite band deshabilitada hasta confirmar margen con más datos"
        return result

    token_id = market["tokens"].get(side)
    result["side"] = side
    result["confidence"] = "HIGH" if band in ("cheap", "mid_confirmed") else "MEDIUM"
    result["confirmations"] = confirmations
    result["stake_multiplier"] = round(1 + STAKE_MULTIPLIER_PER_CONFIRMATION * confirmations, 2)
    reason = (
        f"TWAP60 delta={diff:+.4f} -> {side}, price={price:.2f} band={band} "
        f"confirmations={confirmations} ({confirm_detail}) stake_x={result['stake_multiplier']}"
    )
    result["reasons"] = [reason]
    result["token_id"] = token_id
    result["entry_price"] = price
    result["band"] = band

    logger.info(
        f"✅ Signal v2: {side} band={band} @ {price:.2f} T-{seconds_left:.0f}s "
        f"TWAP60 diff={diff:+.4f} ({twap60_open:.2f} -> {twap60_now:.2f})"
    )
    return result
