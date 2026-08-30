import os

# Polymarket
PRIVATE_KEY = os.environ.get("PRIVATE_KEY", "")
FUNDER = os.environ.get("FUNDER", "0x5fa918d6752074476dCfa68ae5618fC70Bc49945")
CHAIN_ID = 137
CLOB_HOST = "https://clob.polymarket.com"

# DB
DATABASE_URL = os.environ.get("DATABASE_URL", "")

# Trading mode
PAPER_TRADING = os.environ.get("PAPER_TRADING", "true").lower() == "true"
PAPER_BALANCE = 500.0

# Risk params
MAX_POSITION_PCT = 0.05       # 5% base
MAX_POSITION_PCT_KELLY = 0.10 # 10% max with Kelly
MIN_TRADE_USD = 5.0
MAX_SIMULTANEOUS = 5
# Bajado de 0.25 a 0.20 (30-ago-2026) tras el peor día de plata real a la
# fecha: -$98.52 en solo 19 trades (~2 horas), confirmado con datos reales
# (no ejecución rota — paper_v2, con muestra ~10x más grande, degradó
# igual de fuerte en esa misma ventana horaria antes de volver a lo normal
# después de las 11 AM ET: riesgo de cola real de la estrategia, no un
# bug). Reconstruyendo trade por trade lo que pasó hoy: con 0.20 el freno
# hubiera cortado 2 trades antes (ahorrando ~$15 de los ~$98). No se pudo
# backtestear contra los 8 días anteriores de plata real con certeza —
# live_state_v2 solo guarda el día ACTUAL, no quedó registro del drawdown
# % que tuvo cada día pasado (ninguno llegó a pausarse, pero no sé qué tan
# cerca estuvo el peor, ej. 27-ago -$47.06). Se agrega live_day_history_v2
# en live_trader_v2.py para tener esos datos la próxima vez. 0.20 es un
# punto medio razonable, no un número optimizado contra historial real.
DRAWDOWN_LIMIT = 0.20         # pause new live trades for the day at -20% of the day's starting balance
TRADING_START_UTC = 13        # 9AM ET = 1PM UTC
TRADING_END_UTC = 22          # 6PM ET = 10PM UTC
MAX_VOLATILITY_PCT = 0.005    # 0.5%

# Signal
MIN_INDICATORS = 1
RSI_OVERSOLD = 30
RSI_OVERBOUGHT = 70

# Market scanner
GAMMA_API = "https://gamma-api.polymarket.com"
DATA_API = "https://data-api.polymarket.com"
COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"
SCAN_INTERVAL = 30          # seconds
PRICE_INTERVAL = 30         # seconds
MARKET_WINDOW_MIN = 10      # only markets resolving within 10 min

PORT = int(os.environ.get("PORT", 5000))

# Shadow-mode: registra Chainlink TWAP (30s y 60s) + spot en paralelo al
# trading real, sin tocar ninguna decisión. Kill switch por si el feed RTDS
# da problemas en producción — no afecta paper/live si se apaga.
SHADOW_MODE_ENABLED = os.environ.get("SHADOW_MODE_ENABLED", "true").lower() == "true"

# Frenos explícitos para plata real, independientes de PAPER_TRADING.
# PAPER_TRADING=false por sí solo YA NO alcanza para que ningún modelo
# arriesgue plata real — cada uno necesita además su propio flag en true.
# Por qué: v1 es el modelo viejo que confirmamos que pierde plata — no
# queremos que se reactive solo por accidente al apagar el modo paper para
# probar v2. Los dos arrancan en false por defecto (nada cambia con este
# deploy hasta que se toquen las variables de entorno en Railway a mano).
LIVE_V1_ENABLED = os.environ.get("LIVE_V1_ENABLED", "false").lower() == "true"
LIVE_V2_ENABLED = os.environ.get("LIVE_V2_ENABLED", "false").lower() == "true"

# Pausa por banda para plata real de v2, sin tocar paper/shadow (que siguen
# corriendo esa banda para no perder el diagnóstico en curso). Formato:
# nombres de banda separados por coma en la env var, vacío = nada pausado.
#
# Historial: 26-ago-2026 se pausó "cheap" (11-36% real vs 68% en shadow
# varios días seguidos). 28-ago-2026 se sumó "mid_confirmed" tras un día
# de -$55.67 en esa banda con plata real.
#
# 28-ago-2026 (reactivación): con /api/shadow-band-recency ya cubriendo
# las 3 bandas (antes solo cheap/favorite), los últimos 3 días de shadow-
# logging (población completa, mucho más grande que lo poco que se
# tradeó en vivo) muestran las dos bandas SANAS, no degradadas:
#   cheap: 69.3% reciente vs 68.3% histórico (n=319) — consistente.
#   mid:   79.0% reciente vs 72.9% histórico (n=195) — mejor, no peor.
# Esto indica que las pérdidas puntuales en vivo (barata varios días,
# media el 27-ago) fueron ruido de muestra chica (lo que efectivamente
# se tradeó en vivo es una fracción mínima de lo que shadow evalúa), no
# una señal real rompiéndose — se reactivan las dos. Favorite, en
# cambio, sí mostró compresión real (0.122->0.054 $/trade reciente,
# n=779) — se deja activa pero vigilada, no se pausó por seguir positiva.
#
# 29-ago-2026: la compresión de favorite se profundizó y confirmó con
# muestra todavía más grande — $/trade reciente cayó a $0.013 (n=837,
# vs $0.116 histórico), el acierto casi no cambió (89.7% vs 90.9%) así
# que no es que empiece a perder, es que el margen se evaporó un ~90%.
# Coincide con dos días seguidos en rojo con plata real (-$24.78 el 28,
# -$45.95 el 29). A diferencia de cheap/mid, acá SÍ hay evidencia real y
# de muestra grande de degradación, no ruido — se pausa también.
#
# 30-ago-2026 (fix real, no solo pausa): shadow-logging por sub-banda fina
# (get_favorite_recency_by_subband) localizó la degradación: no es toda la
# banda favorite, se concentra en [0.75-0.80) — reciente -$0.346/trade vs
# +$0.273 histórico (n=92), justo pegado al límite con mid_confirmed pero
# sin su filtro de 2+ confirmaciones. El resto de la banda (0.80-0.97)
# sigue sano salvo un tramo más chico y menos claro en 0.93-0.95 (n=126,
# sin frontera natural para cortar, se deja en observación). Se sube
# FAVORITE_MIN de 0.75 a 0.80 en signal_engine_v2.py para excluir el tramo
# roto de raíz (en vez de dejar la banda entera parada sin arreglar nada),
# y se reactiva favorite en plata real ya recortada.
LIVE_V2_DISABLED_BANDS = set(
    b.strip() for b in os.environ.get("LIVE_V2_DISABLED_BANDS", "").split(",") if b.strip()
)