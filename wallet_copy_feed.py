"""
Rastreo prospectivo de "wallets a copiar" — CUALQUIER mercado de
Polymarket, no solo BTC/ETH updown (pedido explícito: "no importa el
mercado, lo que necesitamos es que dé plata y haya liquidez").

Origen: el usuario ya intentó esto hace meses y falló por dos motivos
reales, medidos ahora en vez de a ojo:
  1. Para cuando su posición se llenaba, ya era tarde — sin ganancia.
  2. Muchas wallets apostaban en mercados que resuelven en días/semanas.

13-sep-2026: el filtro original (cualquier trade >$100, sin importar la
wallet) mostró con ~4000 trades resueltos que, ponderado por tamaño, ES
NEGATIVO tanto para el trader original como para copiarlo (-0.04 y
-0.033/share) — las wallets grandes del feed general en su mayoría NO
son buenas, no es un problema de delay de ejecución. Se agrega un
segundo canal: wallets CURADAS del leaderboard oficial de Polymarket
(polymarket.com/leaderboard, sacado del HTML porque no hay endpoint
público — ver curated_wallets), filtradas además por actividad
reciente real (confirmado con datos: de 821 wallets del leaderboard,
varias de las "top" llevan meses sin operar, y la ganancia mostrada de
al menos una de ellas -Papeasy, $5.26M- no cuadra ni de cerca con su
volumen real operado -$8,400-, así que el número del leaderboard NO se
toma como verdad por sí solo). Para las wallets curadas SÍ se loguea
cada trade sin importar el tamaño (no tiene sentido aplicar el filtro
de "ruido retail" a alguien ya pre-seleccionado por track record real).

Tercer canal: consenso general (idea del usuario, "si mucha gente
apuesta lo mismo, algo está pasando") — mismo patrón que ya se usa en
fomo_feed.py para memecoins, aplicado acá al feed de trades de
Polymarket: cuando >=MARKET_CONSENSUS_MIN_TRADERS wallets DISTINTAS
(cualquiera, no solo curadas) compran el mismo lado del mismo mercado
dentro de MARKET_CONSENSUS_WINDOW_MINUTES y el USD combinado supera
MARKET_CONSENSUS_MIN_TOTAL_USD, se loguea el evento para poder medir
después si ese consenso predijo el resultado real.

No hay un endpoint de "top wallets" específico para esto (probado,
404 en varios paths) — el leaderboard real se sacó parseando el HTML
de la página (ver comentario en start_wallet_copy_feed sobre cómo se
generó curated_wallets, ya sembrada directo en la DB).

Solo lectura/logging — no ejecuta ninguna orden real todavía.
"""
import logging
import threading
import time
from collections import defaultdict, deque
import requests
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

DATA_API = "https://data-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"

POLL_INTERVAL_SEC = 3
MIN_NOTIONAL_USD = 100.0   # filtro de "conviccion real", no ruido retail chico (solo aplica a wallets NO curadas)
COPY_CHECK_DELAYS_SEC = (3, 10)  # a los cuantos segundos medimos el precio de copiar
RESOLVE_POLL_EVERY_N_TICKS = 20   # cada ~60s
CURATED_REFRESH_EVERY_N_TICKS = 1200  # cada ~1h, recarga la lista de wallets curadas desde la DB

# 13-sep-2026: umbrales iniciales para el consenso general, sin tuning
# todavia (misma historia que FOMO: van a necesitar ajuste con datos
# reales, esto es un punto de partida razonable, no una conclusion).
MARKET_CONSENSUS_MIN_TRADERS = 3
MARKET_CONSENSUS_MIN_TOTAL_USD = 1000.0
MARKET_CONSENSUS_WINDOW_MINUTES = 30

_HEADERS = {"User-Agent": "Mozilla/5.0"}


class WalletCopyFeed:
    def __init__(self, shadow_logger=None):
        self._running = False
        self._thread = None
        self._seen_tx = set()  # dedupe en memoria, ademas del UNIQUE de la DB
        self._pending_copy_checks = []  # [(row_id, token_id, due_ts, delay_label)]
        self.shadow = shadow_logger
        self._curated_wallets = set()  # direcciones en minuscula
        # consenso: (condition_id, outcome) -> deque[(wallet, ts_ms, usd)]
        self._buys_by_market = defaultdict(deque)
        self._consensus_seen = set()  # (condition_id, outcome) ya logueado en esta racha

    def start(self):
        self._running = True
        self._load_curated_wallets()
        self._thread = threading.Thread(target=self._run_forever, daemon=True)
        self._thread.start()
        logger.info(f"WalletCopyFeed started ({len(self._curated_wallets)} wallets curadas cargadas)")

    def stop(self):
        self._running = False

    def _load_curated_wallets(self):
        if not self.shadow or not self.shadow.conn:
            return
        try:
            with self.shadow.conn.cursor() as cur:
                cur.execute("SELECT wallet FROM curated_wallets")
                self._curated_wallets = {row[0].lower() for row in cur.fetchall()}
        except Exception as e:
            logger.debug(f"WalletCopyFeed load curated wallets error: {e}")
            self._reconnect_shadow()

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
                try:
                    self._resolve_consensus_events()
                except Exception as e:
                    logger.debug(f"WalletCopyFeed resolve consensus error: {e}")
            if tick % CURATED_REFRESH_EVERY_N_TICKS == 0:
                self._load_curated_wallets()
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
            wallet = (t.get("proxyWallet") or "").lower()
            is_curated = wallet in self._curated_wallets

            # el consenso general mira TODOS los buys de conviccion real
            # (mismo MIN_NOTIONAL_USD de siempre), no solo curadas -- la
            # idea del usuario es "mucha gente sin filtrar, mismo lado".
            if (t.get("side") or "").upper() == "BUY" and notional >= MIN_NOTIONAL_USD:
                try:
                    self._handle_consensus(t, notional)
                except Exception as e:
                    logger.debug(f"WalletCopyFeed consensus error: {e}")

            if notional < MIN_NOTIONAL_USD and not is_curated:
                self._seen_tx.add(tx)
                continue

            self._seen_tx.add(tx)
            if len(self._seen_tx) > 20000:  # no crecer sin límite
                self._seen_tx = set(list(self._seen_tx)[-10000:])

            row_id = self._log_trade(t, is_curated)
            if row_id:
                token_id = t.get("asset")
                now = time.time()
                for delay in COPY_CHECK_DELAYS_SEC:
                    self._pending_copy_checks.append((row_id, token_id, now + delay, delay))
                if is_curated:
                    self._update_curated_last_seen(wallet, t.get("timestamp"))

    def _update_curated_last_seen(self, wallet: str, ts):
        if not self.shadow or not self.shadow.conn or not ts:
            return
        try:
            with self.shadow.conn.cursor() as cur:
                cur.execute("""
                    UPDATE curated_wallets SET last_trade_ts=%s, last_checked_at=NOW()
                    WHERE wallet=%s AND (last_trade_ts IS NULL OR last_trade_ts < %s)
                """, (ts, wallet, ts))
            self.shadow.conn.commit()
        except Exception as e:
            logger.debug(f"WalletCopyFeed update curated last_seen error: {e}")
            self._reconnect_shadow()

    def _handle_consensus(self, t: dict, notional: float):
        condition_id = t.get("conditionId")
        outcome = t.get("outcome")
        wallet = t.get("proxyWallet")
        if not condition_id or not outcome or not wallet:
            return
        key = (condition_id, outcome)
        ts_ms = (t.get("timestamp") or int(time.time())) * 1000
        dq = self._buys_by_market[key]
        dq.append((wallet, ts_ms, notional))
        cutoff = ts_ms - MARKET_CONSENSUS_WINDOW_MINUTES * 60 * 1000
        while dq and dq[0][1] < cutoff:
            dq.popleft()

        unique_wallets = {w for w, _, _ in dq}
        total_usd = sum(u for _, _, u in dq)
        if len(unique_wallets) >= MARKET_CONSENSUS_MIN_TRADERS and total_usd >= MARKET_CONSENSUS_MIN_TOTAL_USD:
            if key not in self._consensus_seen:
                self._consensus_seen.add(key)
                self._log_consensus_event(t, dq, unique_wallets, total_usd)
        else:
            # mismo bug que se encontro y arreglo en fomo_feed.py: reset
            # real cuando cae por debajo del umbral, no un elif inalcanzable.
            self._consensus_seen.discard(key)

    def _reconnect_shadow(self):
        """9-sep-2026, encontrado en revisión: shadow_logger.py nunca
        reconecta su conexión sola si se cae (ni acá ni en ningún otro
        lugar del proyecto — no es un problema nuevo de este archivo, es
        sistémico). No lo toco desde afuera, pero reutilizo su propio
        _connect() ya existente para al menos intentar restaurarla acá."""
        if not self.shadow:
            return
        try:
            self.shadow._connect()
        except Exception as e:
            logger.debug(f"WalletCopyFeed reconnect shadow DB error: {e}")

    def _log_trade(self, t: dict, is_curated: bool = False):
        if not self.shadow or not self.shadow.conn:
            return None
        try:
            with self.shadow.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO wallet_copy_trades
                    (proxy_wallet, pseudonym, condition_id, token_id, market_title, market_slug,
                     side, outcome, wallet_price, wallet_size, tx_hash, exchange_ts_sec, is_curated)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (tx_hash) DO NOTHING
                    RETURNING id
                """, (
                    t.get("proxyWallet"), t.get("pseudonym"), t.get("conditionId"), t.get("asset"),
                    t.get("title"), t.get("slug"), t.get("side"), t.get("outcome"),
                    t.get("price"), t.get("size"), t.get("transactionHash"), t.get("timestamp"),
                    is_curated,
                ))
                row = cur.fetchone()
                self.shadow.conn.commit()
                return row[0] if row else None
        except Exception as e:
            logger.debug(f"WalletCopyFeed log_trade error: {e}")
            self._reconnect_shadow()
            return None

    def _log_consensus_event(self, t: dict, dq: deque, unique_wallets: set, total_usd: float):
        if not self.shadow or not self.shadow.conn:
            return
        try:
            token_id = t.get("asset")
            price_now = None
            try:
                pr = requests.get(f"{CLOB}/price", params={"token_id": token_id, "side": "BUY"}, timeout=3)
                if pr.status_code == 200:
                    price_now = float(pr.json().get("price", 0)) or None
            except Exception:
                pass
            with self.shadow.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO market_consensus_events
                    (condition_id, market_title, outcome, token_id, unique_traders, trader_wallets,
                     total_usd, window_minutes, first_ts, last_ts, price_at_event)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    t.get("conditionId"), t.get("title"), t.get("outcome"), token_id,
                    len(unique_wallets), ",".join(sorted(unique_wallets)),
                    total_usd, MARKET_CONSENSUS_WINDOW_MINUTES, dq[0][1], dq[-1][1], price_now,
                ))
            self.shadow.conn.commit()
            logger.info(
                f"🔥 Consenso Polymarket: {t.get('title')} -> {t.get('outcome')} — "
                f"{len(unique_wallets)} wallets distintas en {MARKET_CONSENSUS_WINDOW_MINUTES}min, ~${total_usd:.0f}"
            )
        except Exception as e:
            logger.debug(f"WalletCopyFeed log_consensus error: {e}")
            self._reconnect_shadow()

    def _resolve_consensus_events(self):
        """Resuelve eventos de consenso pendientes reusando la misma
        fuente confiable (CLOB /markets/{condition_id}) que ya se
        valido para wallet_copy_trades."""
        if not self.shadow or not self.shadow.conn:
            return
        try:
            with self.shadow.conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT condition_id FROM market_consensus_events
                    WHERE resolved_at IS NULL AND condition_id IS NOT NULL
                    LIMIT 100
                """)
                pending_cids = [r[0] for r in cur.fetchall()]
        except Exception as e:
            logger.debug(f"WalletCopyFeed resolve_consensus query error: {e}")
            self._reconnect_shadow()
            return

        for cid in pending_cids:
            try:
                r = requests.get(f"{CLOB}/markets/{cid}", headers=_HEADERS, timeout=8)
                if r.status_code != 200:
                    continue
                m = r.json()
                if not m.get("closed"):
                    continue
                tokens = m.get("tokens") or []
                winner = None
                for tk in tokens:
                    if tk.get("winner") is True:
                        winner = tk.get("outcome")
                        break
                with self.shadow.conn.cursor() as cur:
                    cur.execute("""
                        SELECT id, outcome FROM market_consensus_events
                        WHERE condition_id=%s AND resolved_at IS NULL
                    """, (cid,))
                    rows = cur.fetchall()
                    for rid, outc in rows:
                        won = (outc == winner) if winner else None
                        cur.execute("""
                            UPDATE market_consensus_events
                            SET resolved_at=NOW(), actual_outcome=%s, consensus_won=%s
                            WHERE id=%s
                        """, (winner, won, rid))
                self.shadow.conn.commit()
            except Exception as e:
                logger.debug(f"WalletCopyFeed resolve_consensus error [{cid}]: {e}")
                self._reconnect_shadow()

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
                self._reconnect_shadow()

    def _resolve_pending(self):
        if not self.shadow or not self.shadow.conn:
            return
        try:
            with self.shadow.conn.cursor() as cur:
                # 13-sep-2026, bug real encontrado en chequeo de salud: con
                # "ORDER BY MIN(exchange_ts_sec) ASC" a secas, el LIMIT 200
                # queda atrapado para siempre en el mismo bloque de mercados
                # viejos que tardan mucho en cerrar (o nunca cierran limpio)
                # -- medido en vivo: de los 200 mas viejos, 0 estaban cerrados
                # (152 seguian abiertos, 48 daban error), asi que CERO trades
                # se resolvian por ciclo durante horas aunque siguieran
                # entrando miles de trades nuevos detras en la cola. Ahora se
                # ordena por "hace cuanto no lo chequeamos" (tabla aparte
                # wallet_copy_market_checks), no por antiguedad del trade --
                # asi cada ciclo rota hacia adelante y con el tiempo se
                # revisan TODOS los condition_id pendientes, no siempre los
                # mismos 200 atascados.
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS wallet_copy_market_checks (
                        condition_id TEXT PRIMARY KEY,
                        last_checked_at TIMESTAMPTZ
                    )
                """)
                cur.execute("""
                    SELECT t.condition_id, MIN(t.exchange_ts_sec)
                    FROM wallet_copy_trades t
                    LEFT JOIN wallet_copy_market_checks mc ON mc.condition_id = t.condition_id
                    WHERE t.resolved_at IS NULL AND t.condition_id IS NOT NULL
                    GROUP BY t.condition_id
                    ORDER BY COALESCE(MAX(mc.last_checked_at), TIMESTAMP 'epoch') ASC
                    LIMIT 200
                """)
                pending = cur.fetchall()
                self.shadow.conn.commit()
        except Exception as e:
            logger.debug(f"WalletCopyFeed resolve query error: {e}")
            self._reconnect_shadow()
            return

        for cid, first_seen_sec in pending:
            try:
                with self.shadow.conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO wallet_copy_market_checks (condition_id, last_checked_at)
                        VALUES (%s, NOW())
                        ON CONFLICT (condition_id) DO UPDATE SET last_checked_at = NOW()
                    """, (cid,))
                self.shadow.conn.commit()

                # 9-sep-2026, bug real encontrado en revisión: si un mercado
                # nunca resuelve limpio (empatado/anulado) o desaparece de
                # la API, esa fila se quedaba en "pendiente" para siempre —
                # abandonar después de 14 días reales sin resolver.
                age_days = (time.time() - float(first_seen_sec)) / 86400 if first_seen_sec else 0
                give_up = age_days > 14

                # 10-sep-2026, bug real encontrado en revisión: gamma-api
                # /markets?condition_ids= devuelve [] para mercados ya
                # resueltos/archivados (sports, esports viejos) — por eso
                # 0 de ~2000 trades se resolvían. El CLOB /markets/{cid} sí
                # los devuelve, con un flag "winner" por token. Se usa ese.
                r = requests.get(f"{CLOB}/markets/{cid}", headers=_HEADERS, timeout=8)
                if r.status_code != 200:
                    if give_up:
                        self._abandon(cid)
                    continue
                m = r.json()
                if not m.get("closed"):
                    continue
                tokens = m.get("tokens") or []
                winner = None
                for tk in tokens:
                    if tk.get("winner") is True:
                        winner = tk.get("outcome")
                        break
                if not winner:
                    # cerrado pero sin ganador claro (empate/anulado) — no
                    # tiene sentido seguir preguntando por esto cada ciclo,
                    # se resuelve ya con outcome desconocido.
                    self._abandon(cid)
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
                    cur.execute("DELETE FROM wallet_copy_market_checks WHERE condition_id=%s", (cid,))
                self.shadow.conn.commit()
            except Exception as e:
                logger.debug(f"WalletCopyFeed resolve error [{cid}]: {e}")
                self._reconnect_shadow()

    def _abandon(self, condition_id: str):
        """Marca como resuelto con outcome desconocido (NULL) — para
        mercados que cerraron sin ganador claro, o que ya no se pueden
        encontrar en la API después de 14 días de intentarlo. Sin esto
        esas filas nunca salen de la cola de 'pendiente' (ver comentario
        en _resolve_pending)."""
        if not self.shadow or not self.shadow.conn:
            return
        try:
            with self.shadow.conn.cursor() as cur:
                cur.execute("""
                    UPDATE wallet_copy_trades SET resolved_at=NOW(), actual_outcome=NULL
                    WHERE condition_id=%s AND resolved_at IS NULL
                """, (condition_id,))
                cur.execute("DELETE FROM wallet_copy_market_checks WHERE condition_id=%s", (condition_id,))
            self.shadow.conn.commit()
        except Exception as e:
            logger.debug(f"WalletCopyFeed abandon error [{condition_id}]: {e}")


_feed = None


def start_wallet_copy_feed(shadow_logger):
    global _feed
    _feed = WalletCopyFeed(shadow_logger)
    _feed.start()
    return _feed


def get_wallet_copy_feed():
    return _feed
