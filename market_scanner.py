import requests
import logging
import time
import json
import threading
from collections import deque
from datetime import datetime, timezone
from config import GAMMA_API, SCAN_INTERVAL
from signal_engine import ENTRY_WINDOW_START

logger = logging.getLogger(__name__)

# Chequeo pasivo de arbitraje sin dirección: si UP+DOWN < ARB_GAP_THRESHOLD
# (precios reales de compra del CLOB, no el midpoint), comprando ambos lados
# se garantiza ganancia pase lo que pase, sin necesidad de predecir nada.
# 0.97 deja margen razonable bajo el fee del 7% (que en este bot se cobra
# sobre la ganancia del lado ganador, no sobre el costo total) — no es una
# ejecución real, solo mide si la oportunidad existe alguna vez en estos
# mercados antes de invertir tiempo en construir la lógica de ejecución.
ARB_GAP_THRESHOLD = 0.97

# Ventana en la que se pide el precio BUY real del order book en vez de
# confiar en outcomePrices de Gamma (que puede estar stale). Debe cubrir al
# menos toda la ventana de entrada del signal_engine, con margen.
LIVE_PRICE_WINDOW = ENTRY_WINDOW_START + 20

ASSETS = {
    "btc": "bitcoin",
    "eth": "ethereum",
}

CLOB = "https://clob.polymarket.com"

class MarketScanner:
    def __init__(self):
        self.active_markets = []
        self._lock = threading.Lock()
        self._running = False
        self._thread = None
        self.arb_checked_count = 0
        self.arb_gap_count = 0
        self.arb_gap_samples = deque(maxlen=50)

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
            
            # Dynamic interval: 2s when any market is inside the entry window, else 30s
            markets = self.active_markets
            near_close = any(0 < m.get("seconds_left", 999) < LIVE_PRICE_WINDOW for m in markets)
            interval = 2 if near_close else 30
            time.sleep(interval)

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

    def _check_arb_gap(self, asset, slug, up_price, down_price, seconds_left):
        """Registra (sin ejecutar nada) si comprar UP+DOWN garantizaría
        ganancia sin importar el resultado — ver ARB_GAP_THRESHOLD arriba."""
        self.arb_checked_count += 1
        total = up_price + down_price
        if total < ARB_GAP_THRESHOLD:
            self.arb_gap_count += 1
            sample = {
                "asset": asset, "slug": slug, "up_price": up_price,
                "down_price": down_price, "sum": round(total, 4),
                "seconds_left": round(seconds_left, 1),
                "seen_at": datetime.now(timezone.utc).isoformat(),
            }
            self.arb_gap_samples.append(sample)
            logger.info(f"💰 Posible arbitraje sin dirección: {asset.upper()} UP={up_price:.3f} DOWN={down_price:.3f} suma={total:.3f}")

    def get_arb_stats(self) -> dict:
        return {
            "checked": self.arb_checked_count,
            "gaps_found": self.arb_gap_count,
            "threshold": ARB_GAP_THRESHOLD,
            "recent_samples": list(self.arb_gap_samples),
        }

    def _get_clob_buy_price(self, token_id: str) -> float | None:
        """Get real executable BUY price from CLOB /price endpoint."""
        try:
            r = requests.get(
                f"{CLOB}/price",
                params={"token_id": token_id, "side": "BUY"},
                timeout=2.0
            )
            if r.status_code != 200:
                return None
            data = r.json()
            price = float(data.get("price", 0))
            return price if price > 0 else None
        except Exception:
            return None

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

            end_dt = self._parse_dt(
                m.get("endDate") or m.get("endDateIso") or event.get("endDate") or ""
            )
            now = datetime.now(timezone.utc)
            if not end_dt:
                return None

            seconds_left = (end_dt - now).total_seconds()

            if seconds_left < -30:
                return None

            # For markets close to entry window, get REAL prices from CLOB /price
            used_real_clob_prices = False
            if -30 < seconds_left < LIVE_PRICE_WINDOW:
                up_price = self._get_clob_buy_price(tokens["UP"])
                down_price = self._get_clob_buy_price(tokens["DOWN"])
                if up_price is None or down_price is None:
                    up_price, down_price = self._get_outcome_prices(m, outcomes)
                else:
                    used_real_clob_prices = True
            else:
                up_price, down_price = self._get_outcome_prices(m, outcomes)

            if used_real_clob_prices:
                self._check_arb_gap(asset, slug, up_price, down_price, seconds_left)

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

    def _get_outcome_prices(self, m: dict, outcomes: list) -> tuple:
        prices = self._parse_json(m.get("outcomePrices")) or ["0.5", "0.5"]
        up_price = 0.5
        down_price = 0.5
        for i, o in enumerate(outcomes):
            k = o.strip().upper()
            if k == "UP" and i < len(prices):
                up_price = float(prices[i])
            elif k == "DOWN" and i < len(prices):
                down_price = float(prices[i])
        return up_price, down_price

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