import requests
import logging
import time
import json
import threading
from datetime import datetime, timezone, timedelta
from config import GAMMA_API, SCAN_INTERVAL

logger = logging.getLogger(__name__)

# Assets to trade and their slug prefixes
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

        # Generate next 3 upcoming 5-minute windows
        current_window = (now_ts // 300) * 300
        windows = [current_window, current_window + 300, current_window + 600]

        for asset_prefix, asset_name in ASSETS.items():
            for window_ts in windows:
                slug = f"{asset_prefix}-updown-5m-{window_ts}"
                market = self._fetch_event(slug, asset_name)
                if market:
                    found.append(market)

        with self._lock:
            self.active_markets = found

        if found:
            logger.info(f"MarketScanner: {len(found)} 5min markets found")
        else:
            logger.debug("No 5min markets found yet")

    def _fetch_event(self, slug: str, asset: str) -> dict | None:
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

            # Get the main market (usually first one)
            m = markets[0]

            # Parse outcomes and token IDs
            outcomes = m.get("outcomes")
            if isinstance(outcomes, str):
                try:
                    outcomes = json.loads(outcomes)
                except Exception:
                    return None

            token_ids = m.get("clobTokenIds") or m.get("clob_token_ids")
            if isinstance(token_ids, str):
                try:
                    token_ids = json.loads(token_ids)
                except Exception:
                    return None

            if not outcomes or not token_ids or len(outcomes) < 2:
                return None

            # Map Up/Down to token IDs
            tokens = {}
            for i, outcome in enumerate(outcomes):
                key = outcome.strip().upper()
                if i < len(token_ids):
                    tokens[key] = str(token_ids[i])

            if "UP" not in tokens or "DOWN" not in tokens:
                return None

            # Parse prices
            outcome_prices = m.get("outcomePrices")
            if isinstance(outcome_prices, str):
                try:
                    outcome_prices = json.loads(outcome_prices)
                except Exception:
                    outcome_prices = None

            up_price = 0.5
            down_price = 0.5
            if outcome_prices and len(outcome_prices) >= 2:
                try:
                    # Find which index is UP
                    for i, o in enumerate(outcomes):
                        if o.strip().upper() == "UP":
                            up_price = float(outcome_prices[i])
                        elif o.strip().upper() == "DOWN":
                            down_price = float(outcome_prices[i])
                except Exception:
                    pass

            # Parse end time
            end_dt = self._parse_dt(
                m.get("endDateIso") or m.get("endDate") or
                event.get("endDate") or ""
            )

            # Skip if already resolved or not active
            if m.get("closed") or not m.get("active", True):
                return None

            # Skip if end time is in the past
            now = datetime.now(timezone.utc)
            if end_dt and end_dt < now:
                return None

            # Time remaining
            seconds_left = (end_dt - now).total_seconds() if end_dt else 300

            return {
                "id": m.get("id"),
                "slug": slug,
                "title": event.get("title") or m.get("question") or slug,
                "asset": asset,
                "tokens": tokens,
                "up_price": up_price,
                "down_price": down_price,
                "end_dt": end_dt,
                "seconds_left": seconds_left,
                "condition_id": m.get("conditionId"),
            }

        except Exception as e:
            logger.debug(f"Fetch {slug}: {e}")
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