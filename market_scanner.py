import requests
import logging
import time
import threading
from datetime import datetime, timezone, timedelta
from config import GAMMA_API, MARKET_WINDOW_MIN, SCAN_INTERVAL

logger = logging.getLogger(__name__)

KEYWORDS = {
    "bitcoin": ["Bitcoin Up or Down", "BTC Up or Down"],
    "ethereum": ["Ethereum Up or Down", "ETH Up or Down"],
}

class MarketScanner:
    def __init__(self):
        self.active_markets = []  # list of dicts
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
        url = f"{GAMMA_API}/markets"
        params = {"active": "true", "limit": 100, "closed": "false"}
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        markets = r.json()
        if isinstance(markets, dict):
            markets = markets.get("markets", markets.get("data", []))

        now = datetime.now(timezone.utc)
        cutoff = now + timedelta(minutes=MARKET_WINDOW_MIN)

        found = []
        for m in markets:
            title = m.get("question") or m.get("title") or ""
            asset = self._match_asset(title)
            if not asset:
                continue

            # Parse end time
            end_dt = self._parse_dt(m.get("endDateIso") or m.get("end_date_iso") or m.get("endDate") or "")
            if end_dt is None:
                continue
            if end_dt <= now or end_dt > cutoff:
                continue

            # Extract token IDs for Up/Down outcomes
            tokens = self._extract_tokens(m)
            if not tokens:
                continue

            found.append({
                "id": m.get("id"),
                "title": title,
                "asset": asset,  # "bitcoin" or "ethereum"
                "end_dt": end_dt,
                "tokens": tokens,  # {"UP": token_id, "DOWN": token_id}
                "condition_id": m.get("conditionId") or m.get("condition_id"),
            })

        with self._lock:
            self.active_markets = found

        if found:
            logger.info(f"Scanned: {len(found)} active 5min markets found")
        else:
            logger.debug("No 5-min BTC/ETH markets in window")

    def _match_asset(self, title: str) -> str | None:
        t = title.lower()
        if "bitcoin" in t or "btc" in t:
            return "bitcoin"
        if "ethereum" in t or "eth" in t:
            return "ethereum"
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

    def _extract_tokens(self, m: dict) -> dict | None:
        """
        Returns {"UP": token_id_str, "DOWN": token_id_str} or None
        """
        outcomes = m.get("outcomes")
        token_ids = m.get("clobTokenIds") or m.get("clob_token_ids")

        if not outcomes or not token_ids:
            return None

        if isinstance(outcomes, str):
            import json
            try:
                outcomes = json.loads(outcomes)
            except Exception:
                return None

        if isinstance(token_ids, str):
            import json
            try:
                token_ids = json.loads(token_ids)
            except Exception:
                return None

        result = {}
        for i, outcome in enumerate(outcomes):
            key = outcome.strip().upper()
            if i < len(token_ids):
                if "UP" in key:
                    result["UP"] = str(token_ids[i])
                elif "DOWN" in key:
                    result["DOWN"] = str(token_ids[i])

        if "UP" in result and "DOWN" in result:
            return result
        return None

    def get_markets(self):
        with self._lock:
            return list(self.active_markets)


_scanner = MarketScanner()

def start_scanner():
    _scanner.start()

def get_scanner():
    return _scanner