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
# corriendo esa banda para no perder el diagnóstico en curso). 26-ago-2026:
# la banda barata viene rindiendo muy por debajo de lo que muestra
# shadow-logging (68% en toda la población vs 11-36% real en lo que
# efectivamente tradeamos, varios días seguidos) — se pausa en plata real
# hasta entender por qué, sin esperar a tener la respuesta antes de frenar
# la pérdida. Formato: nombres de banda separados por coma en la env var
# (ej. "cheap,mid_confirmed"), vacío = nada pausado.
LIVE_V2_DISABLED_BANDS = set(
    b.strip() for b in os.environ.get("LIVE_V2_DISABLED_BANDS", "cheap").split(",") if b.strip()
)