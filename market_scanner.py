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
        found = []

        # Previous, current and next 2 windows
        current = (now_ts // 300) * 300
        windows = [current - 300, current, current + 300, current + 600]

        for prefix, asset in ASSETS.items():
            for ts in windows:
                slug = f"{prefix}-updown-5m-{ts}"
                market = self._fetch(slug, asset)
                if market:
                    found.append(market)

        with self._lock:
            self.active_markets = found

        if found:
            logger.info(f"MarketScanner: {len(found)} 5min markets | " +
                       " | ".join(f"{m['asset'].upper()} {m['seconds_left']:.0f}s" for m in found))
        else:
            logger.info("MarketScanner: 0 markets found — checking slugs...")

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
                logger.info(f"Empty response for {slug}")
                return None

            event = data[0] if isinstance(data, list) else data
            markets = event.get("markets", [])
            if not markets:
                logger.info(f"No markets in event for {slug}")
                return None
            logger.info(f"Found event {slug}: {len(markets)} markets, active={event.get('active')}, closed={event.get('closed')}")

            m = markets[0]

            if m.get("closed"):
                return None

            # Outcomes and tokens
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

            # Prices
            prices = self._parse_json(m.get("outcomePrices")) or ["0.5", "0.5"]
            up_price = 0.5
            down_price = 0.5
            for i, o in enumerate(outcomes):
                k = o.strip().upper()
                if k == "UP" and i < len(prices):
                    up_price = float(prices[i])
                elif k == "DOWN" and i < len(prices):
                    down_price = float(prices[i])

            # End time
            # Try market endDate first, then event endDate
            end_str = m.get("endDate") or m.get("endDateIso") or event.get("endDate") or ""
            end_dt = self._parse_dt(end_str)
            now = datetime.now(timezone.utc)
            if not end_dt:
                logger.info(f"No end_dt for {slug}")
                return None
            seconds_remaining = (end_dt - now).total_seconds()
            if seconds_remaining < -30:
                logger.info(f"Expired {slug}: end_dt={end_dt}, now={now}, diff={seconds_remaining:.0f}s")
                return None

            seconds_left = (end_dt - now).total_seconds()

            # Reference price from market description/title
            # Polymarket stores it as "Price To Beat" — we get it from the event
            ref_price = self._extract_ref_price(event, m)

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

    def _extract_ref_price(self, event: dict, market: dict) -> float | None:
        """
        Try to extract the reference price (Price To Beat) from market data.
        Polymarket stores this in the description or as startPrice.
        """
        try:
            # Try startPrice field
            sp = market.get("startPrice") or event.get("startPrice")
            if sp:
                return float(sp)

            # Try parsing from description
            desc = market.get("question") or event.get("title") or ""
            import re
            # Look for patterns like "$61,451.30" or "61451.30"
            matches = re.findall(r'\$?([\d,]+\.?\d*)', desc)
            for m in matches:
                val = float(m.replace(",", ""))
                if 1000 < val < 500000:  # reasonable BTC/ETH price
                    return val
        except Exception:
            pass
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