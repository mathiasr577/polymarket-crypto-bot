"""
Live trader para el modelo v2 (TWAP + bandas de precio + confirmaciones) —
ejecuta órdenes REALES en Polymarket. Reusa toda la lógica de seguridad que
ya se probó y arregló a mano en live_trader.py (v1) durante semanas de
trading real: reserva síncrona + ejecución en hilo aparte para no bloquear
el tick de main.py, extracción de precio/costo real de fill
(makingAmount/takingAmount, no un campo "price" directo — confirmado contra
respuestas reales), manejo de fills de estado DESCONOCIDO (cancel_order()
no lanza excepción cuando la orden ya se llenó — se confirmó con un caso
real donde eso hizo perder de vista una posición ganadora), circuit
breaker de drawdown diario, y sizing como % del balance en vez de monto
fijo.

Dos mejoras sobre v1 (v1 nunca las tuvo, encontradas al construir esto):
1. Todo se persiste en Postgres (live_trades_v2, live_state_v2) — v1 vive
   solo en memoria, así que un restart de Railway borraba su historial y
   reseteaba el tracking de drawdown a mitad de día. Acá no.
2. Guarda la banda (cheap/mid_confirmed/favorite) de cada trade, para
   poder ver rendimiento real en vivo por banda — no solo el total.

NO se activa solo con PAPER_TRADING=false — necesita además
LIVE_V2_ENABLED=true explícito (ver config.py) como freno extra antes de
arriesgar plata real con un modelo nuevo.
"""
import json
import logging
import threading
import time
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone, timedelta

from config import DRAWDOWN_LIMIT, DATABASE_URL

logger = logging.getLogger(__name__)

MAX_LIVE_TRADES = 1_000_000  # sin techo real de trades — corre indefinido
# Subido de $5 a $7 (25-ago-2026) tras confirmar el fix de unknown_fill
# (get_order) funcionando limpio un día completo — con el balance de ese
# día (~$376-380), 5 pérdidas seguidas al TOPE ($13 c/u, ver
# LIVE_MAX_STAKE_USD) son ~$65, un 68% del margen de -25% (~$95) — deja
# aire para una mañana floja típica antes de que el circuit breaker actúe.
#
# Bajado de $7 a $6 (30-ago-2026) tras el drawdown más grande a la fecha:
# -$105 en solo ~2 horas (9:00-11:00 AM ET), breaker activado. Investigado
# a fondo cruzando cada trade real contra paper_v2 en el MISMO market_id
# exacto: 18/18 coincidieron en lado apostado y resultado, y paper_v2 en
# esa misma ventana horaria también cayó (favorite 73%/mid 25% vs ~90%/
# ~75% esperado) para volver a lo normal apenas pasó esa ventana (favorite
# 89%, mid 75% después de las 11 AM ET). O sea: NO fue un bug de ejecución
# ni mala suerte del muestreo chico de plata real — fue una ventana real
# de ~2 horas donde el modelo completo (toda la población evaluada, no
# solo lo que se tradeó en vivo) perdió más de lo normal en todas las
# bandas a la vez. Eso no se arregla ajustando umbrales de banda (no es
# degradación persistente como la de favorite el 29-ago) — es riesgo de
# cola inherente a la estrategia. Se baja el tamaño para que una ventana
# así duela menos la próxima vez, no para "arreglar" la señal.
TRADE_SIZE = 6.0
MAX_NO_FILLS_HISTORY = 20

# Mismo esquema de sizing proporcional al balance que ya validamos en v1
# (agregado el 10-ago-2026 tras ver que $5 fijo era >13% de una cuenta
# chica y el circuit breaker cortaba el día antes de juntar muestra).
TARGET_RISK_PCT = 0.08
MIN_TRADE_USD = 2.0

# Techo absoluto para el sizing por confirmaciones (24-ago-2026, subido de
# $10 a $13 el 25-ago-2026 junto con TRADE_SIZE). Bajado de $13 a $11 el
# 30-ago-2026 junto con TRADE_SIZE — ver comentario arriba. Independiente
# de TRADE_SIZE (el techo base sin multiplicador) — este es el techo de lo
# que se puede llegar a arriesgar en UN trade con el multiplicador
# aplicado, decisión explícita del usuario, no calculado.
LIVE_MAX_STAKE_USD = 11.0

# Fase temprana de plata real (20-ago-2026, revisión pre-lanzamiento con la
# otra IA): el primer día no valida el modelo — valida que la EJECUCIÓN real
# (fills, fees, slippage, latencia) se comporte como el backtest/paper
# asumieron, cosa que nunca se probó con plata real de este modelo. Se
# define por cantidad de fills reales (no por fecha/calendario) para que se
# auto-expire solo apenas se junten operaciones limpias, sin depender de que
# alguien se acuerde de aflojar un flag a mano:
#   - primeras EARLY_PHASE_FLAT_TRADES: tamaño fijo chico (aísla el efecto
#     de la ejecución del efecto del sizing proporcional/por confirmaciones)
#   - primeras EARLY_PHASE_SINGLE_POSITION_TRADES: como mucho 1 posición
#     abierta a la vez (más fácil de seguir a mano trade por trade)
#   - mientras dure la fase flat: circuit breaker en dólares absolutos, más
#     estricto que el 25% normal (~-$26 con el balance de hoy) — un bug de
#     ejecución se corta con poca plata en juego, no después de perder un
#     cuarto de la cuenta.
EARLY_PHASE_FLAT_TRADES = 20
EARLY_PHASE_FLAT_SIZE = 2.0
EARLY_PHASE_SINGLE_POSITION_TRADES = 5
EARLY_PHASE_MAX_LOSS_USD = 10.0

# Caché corta del balance real para no golpear la API en cada tick.
BALANCE_CACHE_SEC = 10

# Slippage del LIMIT order (ver order_executor.py) por banda — antes era un
# 3% fijo para cualquier precio. En la banda favorita (precio ~0.75-0.97) el
# margen esperado es de centavos por trade (~$0.19-0.21 en $5, ver
# signal_engine_v2.py), así que un 3% de slippage en ese rango de precio
# (~2.5 centavos) se come más de la mitad del edge esperado. Preferible un
# NO_FILL a convertir un trade con edge en uno sin edge (punto de la otra
# IA, correcto). Banda barata mantiene 3% (ahí el mismo % son centésimas de
# centavo, no material).
DEFAULT_SLIPPAGE_PCT = 0.03
TIGHT_SLIPPAGE_PCT = 0.015
_TIGHT_SLIPPAGE_BANDS = ("favorite", "mid_confirmed")

ET_OFFSET = timedelta(hours=-4)  # mismo offset fijo que usa el resto del código

_FILL_PRICE_FIELDS = ("price", "avgPrice", "average_price", "averagePrice", "fillPrice", "fill_price")

DDL = """
CREATE TABLE IF NOT EXISTS live_trades_v2 (
    id SERIAL PRIMARY KEY,
    market_id TEXT,
    title TEXT,
    asset TEXT,
    side TEXT,
    band TEXT,
    size FLOAT,
    price FLOAT,
    decision_price FLOAT,
    token_id TEXT,
    reasons TEXT,
    opened_at TIMESTAMPTZ DEFAULT NOW(),
    resolved_at TIMESTAMPTZ,
    outcome TEXT,
    pnl FLOAT,
    win BOOLEAN,
    order_resp TEXT,
    latency_ms INT
);

CREATE TABLE IF NOT EXISTS live_state_v2 (
    id INT PRIMARY KEY DEFAULT 1,
    day_start_balance FLOAT,
    current_trading_day DATE,
    today_pnl FLOAT DEFAULT 0,
    drawdown_paused BOOLEAN DEFAULT FALSE,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO live_state_v2 (id) VALUES (1) ON CONFLICT (id) DO NOTHING;

-- 30-ago-2026: agregada tras el peor día de plata real a la fecha
-- (-$98.52 en solo 19 trades) — para decidir si el DRAWDOWN_LIMIT debía
-- bajarse hubo que reconstruir a mano el drawdown % de cada día anterior
-- y no se pudo con certeza porque live_state_v2 solo guarda el ESTADO
-- ACTUAL (se pisa cada rollover) — no hay forma de saber qué tan cerca
-- estuvo cada día pasado del límite. Esta tabla guarda un renglón por
-- día al cerrar (rollover), para que la próxima vez que haga falta
-- ajustar el breaker haya datos reales en vez de tener que adivinar.
CREATE TABLE IF NOT EXISTS live_day_history_v2 (
    id SERIAL PRIMARY KEY,
    trading_day DATE,
    day_start_balance FLOAT,
    day_end_balance FLOAT,
    pnl FLOAT,
    trades_count INT,
    drawdown_paused BOOLEAN,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
"""


def _extract_fill_info(resp: dict, decision_price: float, intended_size: float) -> tuple[float, float]:
    """Idéntica a la de live_trader.py — ver ese archivo para el contexto
    de por qué makingAmount/takingAmount y no un campo "price" directo."""
    taking = resp.get("takingAmount")
    making = resp.get("makingAmount")
    if taking is not None and making is not None:
        try:
            shares = float(taking)
            cost = float(making)
            if shares > 0 and cost > 0:
                return cost / shares, cost
        except (TypeError, ValueError):
            pass

    for key in _FILL_PRICE_FIELDS:
        val = resp.get(key)
        if val is None:
            continue
        try:
            fv = float(val)
        except (TypeError, ValueError):
            continue
        if fv > 0:
            return fv, intended_size

    logger.warning(
        f"No pude identificar el precio/costo real de fill v2 (probé makingAmount/"
        f"takingAmount y {_FILL_PRICE_FIELDS}) — uso decisión {decision_price:.3f} "
        f"y tamaño ${intended_size:.2f}. Respuesta completa: {resp}"
    )
    return decision_price, intended_size


def _safe_float(val):
    if val is None:
        return None
    try:
        return float(val)
    except Exception:
        return None


class LiveTraderV2:
    def __init__(self):
        self._lock = threading.Lock()
        self.total_trades = 0
        self.open_positions = {}
        self.results = []
        self.attempted_markets = set()
        self.active_assets = set()
        self.no_fill_count = 0
        self.blocked_count = 0
        self.recent_no_fills = []
        self.unknown_fills = []
        self._client = None
        self.day_start_balance = None
        self.current_trading_day = None
        self.today_pnl = 0.0
        self._drawdown_paused = False
        self._cached_balance = None
        self._cached_balance_ts = 0.0
        self.conn = None
        self._init_client()
        self._connect_db()

    def _get_cached_balance(self) -> float:
        """Balance real de la cuenta, con caché corta (evita golpear la API
        de balance en cada tick de cada mercado, ya que can_trade() y
        _current_trade_size() ahora la consultan directamente en vez de usar
        la suma interna today_pnl — ver can_trade() para el porqué."""
        now = time.time()
        with self._lock:
            if self._cached_balance is not None and (now - self._cached_balance_ts) < BALANCE_CACHE_SEC:
                return self._cached_balance
        balance = self.get_balance()
        with self._lock:
            self._cached_balance = balance
            self._cached_balance_ts = now
        return balance

    def _init_client(self):
        try:
            from order_executor import get_client
            self._client = get_client()
            if self._client:
                logger.info("LiveTraderV2: CLOB client ready")
            else:
                logger.error("LiveTraderV2: No CLOB client")
        except Exception as e:
            logger.error(f"LiveTraderV2 init error: {e}")

    def _connect_db(self):
        if not DATABASE_URL:
            logger.warning("LiveTraderV2: no DATABASE_URL — sin persistencia (como v1)")
            return
        try:
            self.conn = psycopg2.connect(DATABASE_URL)
            self.conn.autocommit = True
            with self.conn.cursor() as cur:
                cur.execute(DDL)
            self._load_state()
            logger.info("LiveTraderV2 DB connected")
        except Exception as e:
            logger.error(f"LiveTraderV2 DB error: {e}")
            self.conn = None

    def _load_state(self):
        if not self.conn:
            return
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM live_state_v2 WHERE id=1")
                row = cur.fetchone()
                if row:
                    self.day_start_balance = row["day_start_balance"]
                    self.current_trading_day = row["current_trading_day"]
                    self.today_pnl = row["today_pnl"] or 0.0
                    self._drawdown_paused = row["drawdown_paused"] or False

                cur.execute("SELECT * FROM live_trades_v2 WHERE resolved_at IS NULL")
                for row in cur.fetchall():
                    self.open_positions[row["market_id"]] = dict(row)

                # 26-ago-2026: self.results (de donde sale "P&L Total"/win_rate/
                # by_band en el dashboard) NUNCA se reconstruía desde la base de
                # datos acá — solo se llenaba con lo que resolvía DESPUÉS de
                # arrancar este proceso. Cada redeploy (bastante seguidos esta
                # semana) reseteaba ese número a $0 aunque today_pnl (que sí se
                # persiste) siguiera reflejando el día real — el "P&L Total" se
                # veía roto/inconsistente hasta reconstruirse solo con las
                # operaciones nuevas de esa sesión. Se carga acá lo ya resuelto
                # HOY (mismo criterio de "día" que current_trading_day, offset
                # ET) para que el número esté completo desde el primer segundo.
                if self.current_trading_day:
                    cur.execute("""
                        SELECT * FROM live_trades_v2
                        WHERE resolved_at IS NOT NULL
                          AND (resolved_at AT TIME ZONE 'UTC' - INTERVAL '4 hours')::date = %s
                        ORDER BY resolved_at ASC
                    """, (self.current_trading_day,))
                    for row in cur.fetchall():
                        self.results.append(dict(row))

                # total_trades cuenta CUALQUIER fila insertada (fills reales,
                # se inserta solo en el camino de éxito de _execute_order) —
                # no solo las resueltas. Un restart de Railway a mitad de día
                # con posiciones todavía abiertas las hubiera dejado afuera
                # del conteo para siempre (nunca se re-suman al resolverse,
                # solo _execute_order incrementa total_trades). Encontrado en
                # la revisión final antes de ir a plata real.
                cur.execute("SELECT COUNT(*) as n FROM live_trades_v2")
                self.total_trades = cur.fetchone()["n"] or 0
            logger.info(
                f"LiveTraderV2 state restored: day_start_balance={self.day_start_balance} "
                f"today_pnl={self.today_pnl} open_positions={len(self.open_positions)} "
                f"total_trades={self.total_trades} results_loaded_today={len(self.results)}"
            )
        except Exception as e:
            logger.error(f"LiveTraderV2 load state error: {e}")

    def _save_day_state(self):
        if not self.conn:
            return
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    UPDATE live_state_v2 SET
                        day_start_balance=%s, current_trading_day=%s,
                        today_pnl=%s, drawdown_paused=%s, updated_at=NOW()
                    WHERE id=1
                """, (self.day_start_balance, self.current_trading_day, self.today_pnl, self._drawdown_paused))
        except Exception as e:
            logger.error(f"LiveTraderV2 save day state error: {e}")

    def _check_day_rollover(self):
        """Idéntica en espíritu a live_trader.py — ver ese archivo para el
        bug real que motivó el patrón de reintento en vez de aceptar 0.

        24-ago-2026: encontrado en producción que current_trading_day quedó
        pegado en 2026-08-22 durante días, con day_start_balance de ese día
        ($75.62) todavía activo mientras el balance real ya iba en ~$249 —
        el freno de -25% se estaba calculando sobre una referencia vieja.
        No se identificó con certeza por qué is_new_day nunca volvió a
        evaluar True tras el primer rollover exitoso (log de diagnóstico
        agregado abajo por si se repite). Mientras tanto, red de seguridad
        explícita: si el día guardado quedó más de 1 día calendario atrás,
        fuerza el rollover sin depender de la comparación normal."""
        today = (datetime.now(timezone.utc) + ET_OFFSET).date()
        with self._lock:
            stale_days = (today - self.current_trading_day).days if self.current_trading_day else None
            is_new_day = self.current_trading_day != today or (stale_days is not None and stale_days > 1)
            if is_new_day:
                logger.info(
                    f"LiveTraderV2 day check: stored={self.current_trading_day} today={today} "
                    f"stale_days={stale_days} is_new_day={is_new_day}"
                )
            needs_balance = is_new_day or not self.day_start_balance or self.day_start_balance <= 0
            if not needs_balance:
                return
            closing_day = closing_day_start = closing_pnl = closing_trades = closing_paused = None
            if is_new_day:
                # Capturar el resumen del día que se cierra ANTES de resetear,
                # para persistirlo abajo — ver comentario de live_day_history_v2.
                if self.current_trading_day is not None and self.day_start_balance:
                    closing_day = self.current_trading_day
                    closing_day_start = self.day_start_balance
                    closing_pnl = self.today_pnl
                    closing_trades = len(self.results)
                    closing_paused = self._drawdown_paused
                self.current_trading_day = today
                self.today_pnl = 0.0
                self._drawdown_paused = False
                # 26-ago-2026: self.results (pnl/win_rate/by_band mostrados)
                # nunca se vaciaba acá — si el proceso corre sin reiniciarse
                # de un día a otro, se pondría a sumar mezclado lo de ayer
                # con lo de hoy. El usuario pidió explícitamente que ese
                # número sea SOLO el del día, no acumulado — con los
                # despliegues frecuentes de esta semana nunca se notó
                # (cada uno ya vaciaba self.results de por sí), pero apenas
                # pase un día entero sin deploy se hubiera visto mal.
                self.results = []

        balance = self.get_balance()
        if not balance or balance <= 0:
            logger.error(
                f"LiveTraderV2: no pude obtener balance para drawdown (got {balance}) — "
                f"circuit breaker inactivo hasta que funcione; reintenta el próximo tick"
            )
            return

        with self._lock:
            self.day_start_balance = balance
        self._save_day_state()
        if closing_day is not None:
            # balance de hoy ≈ balance de cierre de ayer (no hay trades fuera
            # de horario) — aproximación, no un cierre exacto medido en el
            # instante justo de las 00:00 ET.
            self._save_day_history(closing_day, closing_day_start, balance, closing_pnl, closing_trades, closing_paused)
        logger.info(f"LiveTraderV2 trading day {today}: day_start_balance=${balance:.2f}")

    def _save_day_history(self, trading_day, day_start_balance, day_end_balance, pnl, trades_count, drawdown_paused):
        if not self.conn:
            return
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO live_day_history_v2
                        (trading_day, day_start_balance, day_end_balance, pnl, trades_count, drawdown_paused)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (trading_day, day_start_balance, day_end_balance, pnl, trades_count, drawdown_paused))
        except Exception as e:
            logger.error(f"LiveTraderV2: error guardando live_day_history_v2: {e}")

    def can_trade(self) -> bool:
        self._check_day_rollover()
        with self._lock:
            if (
                self._client is None or
                self.total_trades >= MAX_LIVE_TRADES
            ):
                return False

            max_open = 1 if self.total_trades < EARLY_PHASE_SINGLE_POSITION_TRADES else 2
            if len(self.open_positions) >= max_open:
                return False

            day_start = self.day_start_balance
            total_trades = self.total_trades
            was_paused = self._drawdown_paused

        if not day_start or day_start <= 0:
            return True  # todavía sin balance de referencia del día, no se puede evaluar

        # El freno de drawdown se ancla al BALANCE REAL de la cuenta (con
        # caché corta), no a una suma que construimos nosotros mismos
        # (today_pnl) — encontrado 22-ago-2026: dos operaciones de banda
        # barata a precio muy bajo (~0.05-0.07) se llenaron fragmentadas
        # contra liquidez delgada, tardaron en confirmarse, y terminaron en
        # UNKNOWN_FILL (cancelación ambigua) — pero en realidad SÍ se
        # habían llenado y GANARON en grande (+$94.90 y +$66.42 reales). El
        # supuesto de "peor caso" que le agregamos a UNKNOWN_FILL asumió
        # pérdida total para esas dos, y con varios UNKNOWN_FILL más en el
        # día el today_pnl interno quedó en un -29.9% completamente falso
        # — pausando el trading real mientras la cuenta real iba +$164 en
        # el día. El balance real es la fuente de verdad; es inmune a
        # cualquier error de contabilidad interna como este.
        real_balance = self._get_cached_balance()
        if not real_balance or real_balance <= 0:
            logger.error("LiveTraderV2: no pude obtener balance real para el freno de drawdown — bloqueando por seguridad")
            return False

        if total_trades < EARLY_PHASE_FLAT_TRADES:
            loss_usd = day_start - real_balance
            paused = loss_usd >= EARLY_PHASE_MAX_LOSS_USD
            reason = (
                f"EARLY-PHASE loss limit hit: balance real ${real_balance:.2f}, "
                f"day_start ${day_start:.2f} (-${loss_usd:.2f}) >= -${EARLY_PHASE_MAX_LOSS_USD:.2f} "
                f"(trade #{total_trades}, fase temprana de ejecución)"
            )
        else:
            drawdown = 1 - (real_balance / day_start)
            paused = drawdown >= DRAWDOWN_LIMIT
            reason = (
                f"drawdown limit hit: balance real ${real_balance:.2f} vs day_start "
                f"${day_start:.2f} ({-drawdown*100:.1f}%)"
            )

        with self._lock:
            self._drawdown_paused = paused
        if paused and not was_paused:
            logger.warning(f"LiveTraderV2 {reason} — pausando hasta que el balance real se recupere.")
            self._save_day_state()
        elif was_paused and not paused:
            logger.info(f"LiveTraderV2: balance real (${real_balance:.2f}) ya no está en drawdown vs day_start (${day_start:.2f}) — reanudando.")
            self._save_day_state()

        return not paused

    def record_blocked(self):
        with self._lock:
            self.blocked_count += 1

    def _current_trade_size(self, stake_multiplier: float = 1.0) -> float:
        with self._lock:
            total = self.total_trades
        if total < EARLY_PHASE_FLAT_TRADES:
            # Fase temprana ignora el multiplicador a propósito — su punto
            # era aislar la ejecución del sizing, y ya quedó atrás (más de
            # EARLY_PHASE_FLAT_TRADES fills reales hechos).
            return EARLY_PHASE_FLAT_SIZE
        # Balance real (misma caché que can_trade()), no day_start+today_pnl
        # — mismo motivo que el freno de drawdown: today_pnl puede quedar
        # mal si un fill queda UNKNOWN_FILL y en realidad sí se ejecutó.
        balance = self._get_cached_balance()
        if balance <= 0:
            return MIN_TRADE_USD
        size = balance * TARGET_RISK_PCT
        base = min(TRADE_SIZE, max(MIN_TRADE_USD, size))
        # Sizing por confirmaciones (24-ago-2026) — el mismo multiplicador
        # que ya se validó varios días en paper (STAKE_MULTIPLIER_PER_
        # CONFIRMATION en signal_engine_v2.py, 1.0x-2.5x según cuántas de
        # las 3 señales de confirmación coinciden). Acá, con plata real, se
        # le pone un techo absoluto propio (LIVE_MAX_STAKE_USD) en vez de
        # dejar que el multiplicador decida solo — 2.5x sobre el techo base
        # de $5 daría $12.50, más de lo que se quiere arriesgar por trade.
        return round(min(LIVE_MAX_STAKE_USD, max(MIN_TRADE_USD, base * stake_multiplier)), 2)

    def get_balance(self) -> float:
        try:
            from order_executor import get_balance
            return get_balance()
        except Exception as e:
            logger.error(f"LiveTraderV2 balance error: {e}")
            return 0.0

    def open_trade(self, market_id, title, asset, side, price, token_id,
                   band, reasons, tokens=None, stake_multiplier=1.0) -> bool:
        """Misma reserva-síncrona-más-ejecución-en-hilo-aparte que v1 — ver
        live_trader.py para el porqué (no bloquear el tick mientras se
        espera hasta 55s por un fill). stake_multiplier: ver
        _current_trade_size() y LIVE_MAX_STAKE_USD."""
        with self._lock:
            if self.total_trades >= MAX_LIVE_TRADES:
                return False
            if market_id in self.open_positions:
                return False
            if market_id in self.attempted_markets:
                return False
            if len(self.open_positions) >= 2:
                return False
            if asset in self.active_assets:
                logger.info(f"LiveTraderV2: skipping {asset} — order already pending for this asset")
                return False
            self.attempted_markets.add(market_id)
            self.active_assets.add(asset)

        intended_size = self._current_trade_size(stake_multiplier)

        t = threading.Thread(
            target=self._execute_order,
            args=(market_id, title, asset, side, price, token_id, band, reasons, tokens, intended_size),
            daemon=True,
        )
        t.start()
        return True

    def _execute_order(self, market_id, title, asset, side, price, token_id, band, reasons, tokens, intended_size):
        try:
            t0 = time.time()

            from order_executor import place_order

            alt_token_id = None
            if tokens:
                alt_side = "DOWN" if side == "UP" else "UP"
                alt_token_id = tokens.get(alt_side)

            slippage_pct = TIGHT_SLIPPAGE_PCT if band in _TIGHT_SLIPPAGE_BANDS else DEFAULT_SLIPPAGE_PCT
            resp = place_order(
                token_id=token_id,
                price=round(price, 2),
                size=intended_size,
                side="BUY",
                alt_token_id=alt_token_id,
                max_slippage_pct=slippage_pct,
            )

            latency = time.time() - t0

            with self._lock:
                self.active_assets.discard(asset)

            if "error" in resp:
                error_msg = resp["error"]
                logger.error(f"LiveTraderV2 order failed: {error_msg}")

                if resp.get("unknown_fill"):
                    logger.error(
                        f"⚠️⚠️ LiveTraderV2 estado de fill DESCONOCIDO: {side} {asset.upper()} "
                        f"band={band} market={market_id} order_id={resp.get('order_id')} — "
                        f"revisar manualmente en Polymarket. No se reintenta este mercado."
                    )
                    with self._lock:
                        self.unknown_fills.append({
                            "market_id": market_id, "asset": asset, "side": side, "band": band,
                            "order_id": resp.get("order_id"), "price": price,
                            "size": intended_size,
                            "time": datetime.now(timezone.utc).strftime("%H:%M UTC"),
                        })
                        # Solo informativo (today_pnl/get_stats) desde el 22-ago-2026
                        # — el freno de drawdown (can_trade) ya NO usa today_pnl,
                        # usa el balance real de la cuenta (ver can_trade()). Se
                        # había agregado el 20-ago-2026 asumiendo peor caso (pérdida
                        # total) para que el circuit breaker no quedara ciego a un
                        # fill que sí se ejecutó y perdió; pero resultó tener el
                        # problema simétrico: el 22-ago dos UNKNOWN_FILL de banda
                        # barata en realidad habían GANADO en grande (+$94.90 y
                        # +$66.42), y este supuesto generó un today_pnl falso de
                        # -29.9% que pausó el trading real mientras la cuenta real
                        # iba +$164 en el día. Se deja para mostrar en el dashboard
                        # como señal de "esto quedó incierto", ya no decide nada.
                        self.today_pnl -= intended_size
                    self._save_day_state()
                    return

                if "not filled" in error_msg or "cancelled" in error_msg:
                    self._record_no_fill(asset, side, price)
                with self._lock:
                    self.attempted_markets.discard(market_id)
                return

            status = resp.get("status", "")
            if status != "matched":
                logger.warning(f"LiveTraderV2 order not matched (status={status}) — skipping")
                self._record_no_fill(asset, side, price)
                with self._lock:
                    self.attempted_markets.discard(market_id)
                return

            fill_price, real_cost = _extract_fill_info(resp, price, intended_size)

            with self._lock:
                self.total_trades += 1
                trade = {
                    "market_id": market_id, "title": title, "asset": asset, "side": side,
                    "band": band, "size": real_cost, "price": fill_price,
                    "decision_price": price, "token_id": token_id, "reasons": json.dumps(reasons),
                    "opened_at": datetime.now(timezone.utc).isoformat(),
                    "order_resp": str(resp), "latency_ms": round(latency * 1000),
                }
                self.open_positions[market_id] = trade

            if self.conn:
                try:
                    with self.conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO live_trades_v2
                            (market_id, title, asset, side, band, size, price, decision_price,
                             token_id, reasons, order_resp, latency_ms)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """, (
                            market_id, title, asset, side, band,
                            _safe_float(real_cost), _safe_float(fill_price), _safe_float(price),
                            token_id, json.dumps(reasons), str(resp), round(latency * 1000)
                        ))
                except Exception as e:
                    logger.error(f"LiveTraderV2 insert trade error: {e}")

            logger.info(
                f"🔴 LIVE V2 TRADE: {side} {asset.upper()} band={band} ${real_cost:.2f} "
                f"@ fill={fill_price:.3f} (decision={price:.2f}) | latency={latency*1000:.0f}ms | {reasons}"
            )

        except Exception as e:
            with self._lock:
                self.active_assets.discard(asset)
                self.attempted_markets.discard(market_id)
            logger.error(f"LiveTraderV2 execute error: {e}")

    def _record_no_fill(self, asset, side, price):
        with self._lock:
            self.no_fill_count += 1
            entry = {
                "asset": asset, "side": side, "price": price,
                "time": datetime.now(timezone.utc).strftime("%H:%M UTC"),
            }
            self.recent_no_fills.insert(0, entry)
            if len(self.recent_no_fills) > MAX_NO_FILLS_HISTORY:
                self.recent_no_fills = self.recent_no_fills[:MAX_NO_FILLS_HISTORY]

    def resolve_trade(self, market_id: str, outcome: str):
        with self._lock:
            trade = self.open_positions.pop(market_id, None)
            if not trade:
                return
            side = trade["side"]
            win = (side == outcome)
            entry_price = trade["price"]
            cost = trade.get("size", TRADE_SIZE)
            if win:
                pnl = cost * ((1.0 - entry_price) / entry_price) * (1 - 0.07)
            else:
                pnl = -cost
            result = {**trade, "outcome": outcome, "win": win, "pnl": pnl}
            self.results.append(result)
            self.today_pnl += pnl

        self._save_day_state()

        if self.conn:
            try:
                with self.conn.cursor() as cur:
                    cur.execute("""
                        UPDATE live_trades_v2
                        SET resolved_at=NOW(), outcome=%s, pnl=%s, win=%s
                        WHERE market_id=%s AND resolved_at IS NULL
                    """, (outcome, _safe_float(pnl), win, market_id))
            except Exception as e:
                logger.error(f"LiveTraderV2 resolve persist error: {e}")

        logger.info(
            f"🔴 LIVE V2 RESULT: {'WIN' if win else 'LOSS'} {side} {trade['asset'].upper()} "
            f"band={trade.get('band')} | PnL={pnl:+.2f} | entry={entry_price:.2f}"
        )

    def get_stats(self) -> dict:
        with self._lock:
            wins = [r for r in self.results if r["win"]]
            losses = [r for r in self.results if not r["win"]]
            unknown_fill_assumed_loss = sum(u.get("size", 0) or 0 for u in self.unknown_fills)
            # total_pnl sumaba solo self.results (trades resueltos con fill
            # confirmado) — un unknown_fill nunca entra ahí (no sabemos si
            # se llenó), así que el P&L mostrado se veía mejor que la
            # pérdida real de la cuenta cuando había uno. today_pnl ya lo
            # asumía correctamente para el circuit breaker (ver open_trade);
            # ahora el número que se MUESTRA también lo refleja, para que no
            # contradiga el cash real de la wallet. Encontrado 21-ago-2026
            # comparando este número contra el historial real de Polymarket.
            total_pnl = sum(r["pnl"] for r in self.results) - unknown_fill_assumed_loss
            best = max((r["pnl"] for r in self.results), default=0)
            worst = min((r["pnl"] for r in self.results), default=0)

            by_asset = {}
            by_band = {}
            for r in self.results:
                a = r.get("asset", "unknown")
                by_asset.setdefault(a, {"total": 0, "wins": 0})
                by_asset[a]["total"] += 1
                if r["win"]:
                    by_asset[a]["wins"] += 1

                b = r.get("band", "unknown")
                by_band.setdefault(b, {"total": 0, "wins": 0, "pnl": 0.0, "_slip_sum": 0.0, "_slip_n": 0})
                by_band[b]["total"] += 1
                by_band[b]["pnl"] += r["pnl"]
                if r["win"]:
                    by_band[b]["wins"] += 1
                # Slippage real de ejecución: precio de fill vs precio de decisión,
                # como % del precio de decisión. Es exactamente lo que la otra IA
                # pidió trackear por banda (los centavos pesan distinto en cheap
                # vs favorite) — ya teníamos price/decision_price guardados por
                # trade, solo faltaba resumirlo.
                dp = r.get("decision_price")
                fp = r.get("price")
                if dp and fp:
                    by_band[b]["_slip_sum"] += (fp - dp) / dp
                    by_band[b]["_slip_n"] += 1

            for b, d in by_band.items():
                d["avg_slippage_pct"] = round(d["_slip_sum"] / d["_slip_n"] * 100, 3) if d["_slip_n"] else None
                del d["_slip_sum"]
                del d["_slip_n"]

            cash_balance = 0.0
            try:
                from order_executor import get_balance
                cash_balance = get_balance()
            except Exception:
                pass

            return {
                "total_trades": self.total_trades,
                "completed": len(self.results),
                "wins": len(wins),
                "losses": len(losses),
                "win_rate": len(wins) / len(self.results) * 100 if self.results else 0,
                "total_pnl": total_pnl,
                "pnl": total_pnl,
                "roi": (total_pnl / sum(r.get("size", TRADE_SIZE) for r in self.results) * 100) if self.results else 0,
                "best_trade": best,
                "worst_trade": worst,
                "open_count": len(self.open_positions),
                "open_positions": list(self.open_positions.values()),
                "recent_trades": list(reversed(self.results[-10:])),
                "max_trades": MAX_LIVE_TRADES,
                "done": self.total_trades >= MAX_LIVE_TRADES,
                "cash_balance": cash_balance,
                "no_fill_count": self.no_fill_count,
                "blocked_count": self.blocked_count,
                "recent_no_fills": list(self.recent_no_fills[:10]),
                "unknown_fills": list(self.unknown_fills),
                "unknown_fill_assumed_loss": unknown_fill_assumed_loss,
                "by_asset": by_asset,
                "by_band": by_band,
                "balance": cash_balance,
                "today_pnl": self.today_pnl,
                "day_start_balance": self.day_start_balance,
                "current_trading_day": str(self.current_trading_day),
                "drawdown_paused": self._drawdown_paused,
                "early_phase": {
                    "flat_sizing_active": self.total_trades < EARLY_PHASE_FLAT_TRADES,
                    "single_position_active": self.total_trades < EARLY_PHASE_SINGLE_POSITION_TRADES,
                    "trades_until_normal_sizing": max(0, EARLY_PHASE_FLAT_TRADES - self.total_trades),
                    "max_loss_usd": EARLY_PHASE_MAX_LOSS_USD,
                },
            }


_live_trader_v2 = LiveTraderV2()


def get_live_trader_v2():
    return _live_trader_v2
