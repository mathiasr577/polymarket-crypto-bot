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
# /api/shadow-favorite-extension. 0.55-0.65 y 0.65-0.75 siguen sin ganarle
# al breakeven (62% vs 62%, y 70% vs 71%) — esos quedan afuera todavía.
CHEAP_MAX = 0.55
FAVORITE_MIN = 0.75
FAVORITE_MAX = 0.97
MIN_REL_DELTA_CHEAP = 0.0001
TRADE_FAVORITE_BAND = True

ENTRY_WINDOW_START = 120   # mismos límites de tiempo que v1 — los datos de
ENTRY_WINDOW_END = 55      # shadow_logger se recolectaron en esta ventana,
                            # así que el backtest solo es válido si v2 opera
                            # en la misma ventana.


def generate_signal_v2(chainlink_snapshot: dict, market: dict) -> dict:
    """chainlink_snapshot: dict con twap60_open (apertura de la ventana
    actual, de ChainlinkFeed.get_window_twap) y twap60_now/feed_connected
    (de ChainlinkFeed.get_snapshot) — se arma en main.py antes de llamar
    acá, igual que se arma `indicators` para el modelo viejo."""
    result = {
        "side": None,
        "confidence": None,
        "reasons": [],
        "blocked": False,
        "block_reason": "",
        "token_id": None,
        "entry_price": None,
        "band": None,
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

    if price < CHEAP_MAX:
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
    else:
        result["blocked"] = True
        result["block_reason"] = f"Price {price:.2f} in excluded band (0.55-0.75, sin ventaja probada)"
        return result

    if band == "favorite" and not TRADE_FAVORITE_BAND:
        result["blocked"] = True
        result["block_reason"] = "Favorite band deshabilitada hasta confirmar margen con más datos"
        return result

    token_id = market["tokens"].get(side)
    result["side"] = side
    result["confidence"] = "HIGH" if band == "cheap" else "MEDIUM"
    result["reasons"] = [f"TWAP60 delta={diff:+.4f} -> {side}, price={price:.2f} band={band}"]
    result["token_id"] = token_id
    result["entry_price"] = price
    result["band"] = band

    logger.info(
        f"✅ Signal v2: {side} band={band} @ {price:.2f} T-{seconds_left:.0f}s "
        f"TWAP60 diff={diff:+.4f} ({twap60_open:.2f} -> {twap60_now:.2f})"
    )
    return result
