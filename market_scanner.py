import requests
import logging
import time
import json
import threading
from datetime import datetime, timezone
from config import GAMMA_API, SCAN_INTERVAL

logger = logging.getLogger(__name__)

ASSETS = {
    "btc": "bitcoin",
    "eth": "ethereum",
}

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
        now_ts = int(time.time())
        current = (now_ts // 300) * 300
        windows = [current - 300, current, current + 300, current + 600]

        found = []
        for prefix, asset in ASSETS.items():
            for ts in windows:
                slug = f"{prefix}-updown-5m-{ts}"
                market = self._fetch(slug, asset)
                if market:
                    found.append(market)

        with self._lock:
            self.active_markets = found

        if found:
            logger.info(
                f"MarketScanner: {len(found)} markets | " +
                " | ".join(
                    f"{m['asset'].upper()} T-{m['seconds_left']:.0f}s UP={m['up_price']:.2f} DOWN={m['down_price']:.2f}"
                    for m in found
                )
            )

    def _fetch(self, slug: str, asset: str) -> dict | None:
        try:
            r = requests.get(
                f"{GAMMA_API}/events",
                params={"slug": slug},
                timeout=10,
            )
            if r.status_code != 200:
                return None

            data = r.json()
            if not data:
                return None

            event = data[0] if isinstance(data, list) else data
            markets = event.get("markets", [])
            if not markets:
                return None

            m = markets[0]

            if m.get("closed"):
                return None

            outcomes = self._parse_json(m.get("outcomes"))
            token_ids = self._parse_json(m.get("clobTokenIds") or m.get("clob_token_ids"))

            if not outcomes or not token_ids or len(outcomes) < 2:
                return None

            tokens = {}
            for i, o in enumerate(outcomes):
                k = o.strip().upper()
                if i < len(token_ids):
                    tokens[k] = str(token_ids[i])

            if "UP" not in tokens or "DOWN" not in tokens:
                return None

            # Use bestAsk for UP and DOWN — this is the real current price
            # bestAsk = price you pay to buy that token right now
            best_ask_up = float(m.get("bestAsk", 0.5) or 0.5)
            best_bid_up = float(m.get("bestBid", 0.5) or 0.5)

            # outcomePrices gives [up_price, down_price] as last trade prices
            prices = self._parse_json(m.get("outcomePrices")) or ["0.5", "0.5"]
            up_price_last = 0.5
            down_price_last = 0.5
            for i, o in enumerate(outcomes):
                k = o.strip().upper()
                if k == "UP" and i < len(prices):
                    up_price_last = float(prices[i])
                elif k == "DOWN" and i < len(prices):
                    down_price_last = float(prices[i])

            # Use bestAsk as the real price to pay for UP token
            # If bestAsk not available, fall back to outcomePrices
            up_price = best_ask_up if best_ask_up > 0.01 else up_price_last
            down_price = round(1.0 - up_price, 4)  # UP + DOWN = 1.0

            end_dt = self._parse_dt(
                m.get("endDate") or m.get("endDateIso") or event.get("endDate") or ""
            )
            now = datetime.now(timezone.utc)
            if not end_dt:
                return None

            seconds_left = (end_dt - now).total_seconds()

            if seconds_left < -30:
                return None

            ref_price = self._get_ref_price(m, event, asset)

            return {
                "id": str(m.get("id", "")),
                "slug": slug,
                "title": event.get("title") or slug,
                "asset": asset,
                "tokens": tokens,
                "up_price": up_price,
                "down_price": down_price,
                "ref_price": ref_price,
                "end_dt": end_dt,
                "seconds_left": seconds_left,
            }

        except Exception as e:
            logger.debug(f"Fetch {slug}: {e}")
            return None

    def _get_ref_price(self, market: dict, event: dict, asset: str) -> float | None:
        import re
        desc = market.get("description") or ""
        patterns = [r'\$([0-9,]+\.?[0-9]*)', r'([0-9,]+\.?[0-9]*)\s*USD']
        for pat in patterns:
            matches = re.findall(pat, desc)
            for match in matches:
                val = float(match.replace(",", ""))
                if asset == "bitcoin" and 10000 < val < 500000:
                    return val
                elif asset == "ethereum" and 100 < val < 50000:
                    return val
        return None

    def _parse_json(self, val):
        if val is None:
            return None
        if isinstance(val, (list, dict)):
            return val
        try:
            return json.loads(val)
        except Exception:
            return None

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


_scanner = MarketScanner()

def start_scanner():
    _scanner.start()

def get_scanner():
    return _scanner