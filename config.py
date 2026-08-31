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
# Bajado de 0.25 a 0.20 el 30-ago-2026 (ver commit e159587) tras el peor
# día de plata real a la fecha (-$98.52 en 19 trades), y revertido a 0.25
# el mismo día a pedido explícito del usuario: decisión de tolerancia al
# riesgo, no un hallazgo nuevo — se confirmó que fue un evento de cola
# real y no un bug (ver ese commit), así que se prefiere no recortar el
# margen del día por un evento que se está tratando como no-recurrente.
# El tamaño de apuesta SÍ se bajó ($7->$6, $13->$11 en live_trader_v2.py)
# y ese ajuste se mantiene. live_day_history_v2 sigue registrando cada
# día para la próxima vez que haga falta revisar este número con datos.
DRAWDOWN_LIMIT = 0.25         # pause new live trades for the day at -25% of the day's starting balance
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
#
# 31-ago-2026 (pausa otra vez, con evidencia distinta y más fuerte que
# antes — consultado con una IA externa, verificado cada punto antes de
# actuar): tres señales independientes apuntan al mismo lado.
#   1) Plata real: -$23.41 en 432 trades (88.4% acierto, margen tan fino
#      que ni ganando casi siempre alcanza a compensar).
#   2) Se probó un EV-gate (logit(precio_mercado) + señales de lead/OFI/
#      presión/magnitud, walk-forward real, testeado SOLO contra los
#      últimos 3 días nunca vistos por el modelo) — no le ganó al precio
#      crudo de Polymarket out-of-sample (Brier 0.0880 vs 0.0882,
#      prácticamente idéntico). El mercado ya está bien calibrado ahí.
#   3) Gross/net edge por bucket de precio, separando reciente de
#      histórico con la fórmula de fee CORRECTA: histórico +2.44% neto
#      por dólar apostado (real), reciente (últimos 5 días) cayó a
#      +0.41% — technically positivo pero tan fino que fricciones reales
#      de ejecución (Polymarket confirma un "taker delay" de 250ms en
#      estos mercados exactos, itode:true verificado) pueden borrarlo.
# Conclusión: no es "no hay edge", es que el que queda es demasiado fino
# para sobrevivir como taker. Se pausa de plata real otra vez — shadow y
# paper siguen corriendo para investigar maker-only (post-only, sin fee
# de taker, con posible rebate) antes de volver a poner capital real acá.
#
# 31-ago-2026 (cierre de la línea de "alpha informacional" con las 4
# señales actuales — magnitud TWAP60, lead, OFI, presión): probado con
# regresión logística (offset=logit(precio), 1 split walk-forward) Y con
# GBDT (mismas features + precio/segundos-restantes/asset, 3 folds
# temporales walk-forward, n=1857 agregado out-of-sample) — en AMBOS
# casos el Brier score empata con el precio crudo de Polymarket, y el
# ranking por quintiles de edge predicho sale desordenado (el quintil
# "mejor predicho" no es el de mejor resultado real, ni con el modelo no
# lineal aparece la interacción tipo "OFI importa solo si lead cruza tal
# umbral"). Con dos modelos de distinta familia y folds distintos dando
# el mismo resultado negativo, se cierra esta línea (no se seguirá
# iterando con más modelos sobre las mismas 4 features — eso sería
# data snooping) hasta que haya información genuinamente nueva (ej.
# Kalshi validado, o la descomposición de ejecución de los 250ms de
# taker delay que se investiga a continuación).
LIVE_V2_DISABLED_BANDS = set(
    b.strip() for b in os.environ.get("LIVE_V2_DISABLED_BANDS", "favorite").split(",") if b.strip()
)