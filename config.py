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
DRAWDOWN_LIMIT = 0.20         # pause at -20%
TRADING_START_UTC = 0
TRADING_END_UTC = 24
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