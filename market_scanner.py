import requests
import logging
import time
import re
import threading
from datetime import datetime, timezone, timedelta
from config import GAMMA_API, SCAN_INTERVAL

logger = logging.getLogger(__name__)

# Keywords que matchean mercados crypto de precio
CRYPTO_PATTERNS = {
    "bitcoin": [
        "bitcoin", "btc",
    ],
    "ethereum": [
        "ethereum", "eth",
    ],
}

# Palabras que indican que es un mercado de precio (no política, no adoption)
PRICE_KEYWORDS = [
    "above", "below", "over", "under", "reach", "hit",
    "exceed", "higher", "lower", "price", "end of",
    "close above", "close below", "by end", "eoy",
    "$", "k by", "usd",
]

class MarketScanner:
    def __init__(self):
        self.active_markets = []
        self._lock = threading.Lock()
        self._running = False
        self._thread = None

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        logger.info("MarketScanner started")

    def stop(self):
        self._running = False

    def _loop(self):
        while self._running:
            try:
                self._scan()
            except Exception as e:
                logger.error(f"MarketScanner error: {e}")
            time.sleep(SCAN_INTERVAL)

    def _scan(self):
        found = []
        offset = 0
        limit = 100

        while True:
            try:
                r = requests.get(
                    f"{GAMMA_API}/markets",
                    params={
                        "active": "true",
                        "closed": "false",
                        "limit": limit,
                        "offset": offset,
                    },
                    timeout=15,
                )
                if r.status_code != 200:
                    logger.warning(f"Gamma API status {r.status_code}")
                    break

                markets = r.json()
                if isinstance(markets, dict):
                    markets = markets.get("markets", markets.get("data", []))

                if not markets:
                    break

                for m in markets:
                    parsed = self._parse_market(m)
                    if parsed:
                        found.append(parsed)

                if len(markets) < limit:
                    break
                offset += limit

            except Exception as e:
                logger.error(f"Scan page error: {e}")
                break

        # Sort by volume
        found.sort(key=lambda x: x["volume24hr"], reverse=True)

        with self._lock:
            self.active_markets = found

        logger.info(f"MarketScanner: {len(found)} crypto price markets found")

    def _parse_market(self, m: dict) -> dict | None:
        title = (m.get("question") or m.get("title") or "").strip()
        title_lower = title.lower()

        # Must be crypto
        asset = None
        for a, keywords in CRYPTO_PATTERNS.items():
            if any(k in title_lower for k in keywords):
                asset = a
                break
        if not asset:
            return None

        # Must be a price market
        if not any(k in title_lower for k in PRICE_KEYWORDS):
            return None

        # Must have Yes/No outcomes
        outcomes = m.get("outcomes")
        if isinstance(outcomes, str):
            import json
            try:
                outcomes = json.loads(outcomes)
            except Exception:
                return None

        if not outcomes or len(outcomes) < 2:
            return None

        outcome_names = [o.strip().upper() for o in outcomes]
        if "YES" not in outcome_names and "NO" not in outcome_names:
            return None

        # Get token IDs
        token_ids = m.get("clobTokenIds") or m.get("clob_token_ids")
        if isinstance(token_ids, str):
            import json
            try:
                token_ids = json.loads(token_ids)
            except Exception:
                return None

        if not token_ids or len(token_ids) < 2:
            return None

        # Map YES/NO to token IDs
        tokens = {}
        for i, outcome in enumerate(outcomes):
            key = outcome.strip().upper()
            if i < len(token_ids):
                tokens[key] = str(token_ids[i])

        if "YES" not in tokens or "NO" not in tokens:
            return None

        # Parse prices
        outcome_prices = m.get("outcomePrices")
        if isinstance(outcome_prices, str):
            import json
            try:
                outcome_prices = json.loads(outcome_prices)
            except Exception:
                outcome_prices = None

        yes_price = 0.5
        no_price = 0.5
        if outcome_prices and len(outcome_prices) >= 2:
            try:
                yes_price = float(outcome_prices[0])
                no_price = float(outcome_prices[1])
            except Exception:
                pass

        # Parse end date
        end_dt = self._parse_dt(
            m.get("endDateIso") or m.get("endDate") or ""
        )

        # Extract price target from title
        price_target = self._extract_price_target(title)

        # Direction: "above/over/hit" → YES means price goes up
        direction = self._extract_direction(title_lower)

        volume24hr = float(m.get("volume24hr") or 0)

        return {
            "id": m.get("id"),
            "title": title,
            "asset": asset,
            "tokens": tokens,
            "yes_price": yes_price,
            "no_price": no_price,
            "end_dt": end_dt,
            "price_target": price_target,
            "direction": direction,  # "up" or "down"
            "volume24hr": volume24hr,
            "condition_id": m.get("conditionId") or m.get("condition_id"),
        }

    def _extract_price_target(self, title: str) -> float | None:
        """Extract dollar amount from title like 'BTC above $65,000'"""
        # Match patterns like $65,000 or $65k or 65000
        patterns = [
            r'\$([0-9,]+(?:\.[0-9]+)?)[kK]',   # $65k
            r'\$([0-9,]+(?:\.[0-9]+)?)',          # $65,000
            r'([0-9,]+(?:\.[0-9]+)?)[kK]\s',     # 65k
        ]
        for pat in patterns:
            m = re.search(pat, title)
            if m:
                val = m.group(1).replace(',', '')
                try:
                    num = float(val)
                    if 'k' in pat.lower():
                        num *= 1000
                    if num > 100:  # filter out small numbers that aren't prices
                        return num
                except Exception:
                    pass
        return None

    def _extract_direction(self, title_lower: str) -> str:
        """
        Returns 'up' if YES means price goes UP (above/over/hit/reach)
        Returns 'down' if YES means price goes DOWN (below/under/dip/drop)
        """
        down_words = ["below", "under", "lower", "drop", "fall", "dip", "crash", "decline"]
        up_words = ["above", "over", "hit", "reach", "exceed", "higher", "at least", "surpass"]

        for w in down_words:
            if w in title_lower:
                return "down"
        for w in up_words:
            if w in title_lower:
                return "up"
        return "up"  # default

    def _parse_dt(self, s: str):
        if not s:
            return None
        try:
            s = s.replace("Z", "+00:00")
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            return None

    def get_markets(self):
        with self._lock:
            return list(self.active_markets)

    def get_markets_by_asset(self, asset: str):
        with self._lock:
            return [m for m in self.active_markets if m["asset"] == asset]


_scanner = MarketScanner()

def start_scanner():
    _scanner.start()

def get_scanner():
    return _scanner