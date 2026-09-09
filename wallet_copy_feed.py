"""
Rastreo prospectivo de "wallets a copiar" — CUALQUIER mercado de
Polymarket, no solo BTC/ETH updown (pedido explícito: "no importa el
mercado, lo que necesitamos es que dé plata y haya liquidez").

Origen: el usuario ya intentó esto hace meses y falló por dos motivos
reales, medidos ahora en vez de a ojo:
  1. Para cuando su posición se llenaba, ya era tarde — sin ganancia.
  2. Muchas wallets apostaban en mercados que resuelven en días/semanas.

No hay un endpoint de "top wallets" específico para esto (probado,
404 en varios paths) y las wallets del leaderboard oficial de
Polymarket no aparecieron en una muestra del feed en vivo — en vez de
depender de un ranking externo que no tiene en cuenta si SE PUEDE
copiar de verdad, construimos nuestra propia evaluación desde cero:
por cada trade grande real, medimos qué nos hubiera costado copiarlo
con un delay realista (3s y 10s después, precio real del CLOB en ese
momento) — eso mide exactamente el problema #1 de arriba. El problema
#2 (mercados que tardan) se resuelve solo, quedando registrado
market_title/resolved_at: en el análisis, se puede filtrar directo a
"resolvió en <24h" si eso es lo que importa.

Solo lectura/logging — no ejecuta ninguna orden real todavía.
"""
import logging
import threading
import time
import requests
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

DATA_API = "https://data-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"

POLL_INTERVAL_SEC = 3
MIN_NOTIONAL_USD = 100.0   # filtro de "conviccion real", no ruido retail chico
COPY_CHECK_DELAYS_SEC = (3, 10)  # a los cuantos segundos medimos el precio de copiar
RESOLVE_POLL_EVERY_N_TICKS = 20   # cada ~60s

_HEADERS = {"User-Agent": "Mozilla/5.0"}


class WalletCopyFeed:
    def __init__(self, shadow_logger=None):
        self._running = False
        self._thread = None
        self._seen_tx = set()  # dedupe en memoria, ademas del UNIQUE de la DB
        self._pending_copy_checks = []  # [(row_id, token_id, due_ts, delay_label)]
        self.shadow = shadow_logger

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._run_forever, daemon=True)
        self._thread.start()
        logger.info("WalletCopyFeed started")

    def stop(self):
        self._running = False

    def _run_forever(self):
        tick = 0
        while self._running:
            tick += 1
            try:
                self._poll_trades()
            except Exception as e:
                logger.warning(f"WalletCopyFeed poll error: {e}")
            try:
                self._process_pending_copy_checks()
            except Exception as e:
                logger.debug(f"WalletCopyFeed copy-check error: {e}")
            if tick % RESOLVE_POLL_EVERY_N_TICKS == 0:
                try:
                    self._resolve_pending()
                except Exception as e:
                    logger.debug(f"WalletCopyFeed resolve error: {e}")
            time.sleep(POLL_INTERVAL_SEC)

    def _poll_trades(self):
        try:
            r = requests.get(f"{DATA_API}/trades", params={"limit": 100}, headers=_HEADERS, timeout=8)
            r.raise_for_status()
            trades = r.json()
        except Exception as e:
            logger.debug(f"WalletCopyFeed fetch trades error: {e}")
            return

        for t in trades:
            tx = t.get("transactionHash")
            if not tx or tx in self._seen_tx:
                continue
            size = t.get("size") or 0
            price = t.get("price") or 0
            notional = float(size) * float(price)
            if notional < MIN_NOTIONAL_USD:
                self._seen_tx.add(tx)
                continue

            self._seen_tx.add(tx)
            if len(self._seen_tx) > 20000:  # no crecer sin límite
                self._seen_tx = set(list(self._seen_tx)[-10000:])

            row_id = self._log_trade(t)
            if row_id:
                token_id = t.get("asset")
                now = time.time()
                for delay in COPY_CHECK_DELAYS_SEC:
                    self._pending_copy_checks.append((row_id, token_id, now + delay, delay))

    def _log_trade(self, t: dict):
        if not self.shadow or not self.shadow.conn:
            return None
        try:
            with self.shadow.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO wallet_copy_trades
                    (proxy_wallet, pseudonym, condition_id, token_id, market_title, market_slug,
                     side, outcome, wallet_price, wallet_size, tx_hash, exchange_ts_sec)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (tx_hash) DO NOTHING
                    RETURNING id
                """, (
                    t.get("proxyWallet"), t.get("pseudonym"), t.get("conditionId"), t.get("asset"),
                    t.get("title"), t.get("slug"), t.get("side"), t.get("outcome"),
                    t.get("price"), t.get("size"), t.get("transactionHash"), t.get("timestamp"),
                ))
                row = cur.fetchone()
                self.shadow.conn.commit()
                return row[0] if row else None
        except Exception as e:
            logger.debug(f"WalletCopyFeed log_trade error: {e}")
            try:
                self.shadow.conn.rollback()
            except Exception:
                pass
            return None

    def _process_pending_copy_checks(self):
        now = time.time()
        due = [p for p in self._pending_copy_checks if p[2] <= now]
        if not due:
            return
        self._pending_copy_checks = [p for p in self._pending_copy_checks if p[2] > now]
        for row_id, token_id, _, delay in due:
            try:
                r = requests.get(f"{CLOB}/price", params={"token_id": token_id, "side": "BUY"}, timeout=3)
                if r.status_code != 200:
                    continue
                price = float(r.json().get("price", 0))
                if price <= 0:
                    continue
                col = "copy_price_3s" if delay == 3 else "copy_price_10s"
                with self.shadow.conn.cursor() as cur:
                    cur.execute(f"""
                        UPDATE wallet_copy_trades SET {col}=%s, copy_checked_at=NOW()
                        WHERE id=%s
                    """, (price, row_id))
                self.shadow.conn.commit()
            except Exception as e:
                logger.debug(f"WalletCopyFeed copy-price error [{token_id}]: {e}")

    def _resolve_pending(self):
        if not self.shadow or not self.shadow.conn:
            return
        try:
            with self.shadow.conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT condition_id FROM wallet_copy_trades
                    WHERE resolved_at IS NULL AND condition_id IS NOT NULL
                    LIMIT 200
                """)
                pending = [row[0] for row in cur.fetchall()]
        except Exception as e:
            logger.debug(f"WalletCopyFeed resolve query error: {e}")
            return

        for cid in pending:
            try:
                r = requests.get(f"{GAMMA_API}/markets", params={"condition_ids": cid}, timeout=8)
                if r.status_code != 200:
                    continue
                markets = r.json()
                if not markets:
                    continue
                m = markets[0]
                if not m.get("closed"):
                    continue
                outcomes = m.get("outcomes")
                prices = m.get("outcomePrices")
                if isinstance(outcomes, str):
                    import json as _json
                    outcomes = _json.loads(outcomes)
                if isinstance(prices, str):
                    import json as _json
                    prices = _json.loads(prices)
                if not outcomes or not prices:
                    continue
                winner = None
                for o, p in zip(outcomes, prices):
                    if float(p) >= 0.5:
                        winner = o
                        break
                if not winner:
                    continue
                with self.shadow.conn.cursor() as cur:
                    cur.execute("""
                        SELECT id, side, outcome, wallet_price, copy_price_3s, copy_price_10s
                        FROM wallet_copy_trades WHERE condition_id=%s AND resolved_at IS NULL
                    """, (cid,))
                    rows = cur.fetchall()
                    for rid, side, outc, wp, cp3, cp10 in rows:
                        won = 1.0 if outc == winner else 0.0
                        wallet_pnl = (won - float(wp)) if wp is not None else None
                        copy_pnl_3s = (won - float(cp3)) if cp3 is not None else None
                        copy_pnl_10s = (won - float(cp10)) if cp10 is not None else None
                        cur.execute("""
                            UPDATE wallet_copy_trades
                            SET resolved_at=NOW(), actual_outcome=%s,
                                wallet_pnl_per_share=%s, copy_pnl_per_share_3s=%s, copy_pnl_per_share_10s=%s
                            WHERE id=%s
                        """, (winner, wallet_pnl, copy_pnl_3s, copy_pnl_10s, rid))
                self.shadow.conn.commit()
            except Exception as e:
                logger.debug(f"WalletCopyFeed resolve error [{cid}]: {e}")


_feed = None


def start_wallet_copy_feed(shadow_logger):
    global _feed
    _feed = WalletCopyFeed(shadow_logger)
    _feed.start()
    return _feed


def get_wallet_copy_feed():
    return _feed
