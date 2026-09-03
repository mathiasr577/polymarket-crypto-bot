"""
Bot chico y separado — "presión sola" (1-sep-2026, pedido explícito del
usuario: algo nuevo, simple, con plata real chica, mientras el bot
principal está en pausa investigando maker-only).

Señal: twap_pressure_integral (integral en el tiempo de cuánto el spot
de Chainlink se separó del TWAP60 durante lo que va de la ventana) —
usada SOLA, sin TWAP60, sin bandas, sin confirmaciones. Es una de las 3
señales de confirmación de v2, pero acá se prueba como señal PRINCIPAL
por sí sola, algo que nunca se había hecho.

Validado antes de tocar plata real (mismo criterio de siempre):
paper backtest sobre shadow_decisions, n=10,415, partido en dos mitades
por tiempo. Umbral elegido (20) usando SOLO la mitad vieja, aplicado
después a la mitad nueva como si fuera "futuro nunca visto":
84.8% acierto, +$0.173 por cada $1 apostado, n=4489. Estable entre
mitades (la nueva dio igual o mejor que la vieja — lo opuesto del
patrón de sobreajuste que tiró abajo la idea de usar Kalshi solo).

Aviso honesto que se mantiene: el precio promedio donde entra esto
(0.76-0.88) se mete en el rango de favorite, que ya se investigó y se
encontró con edge muy fino usando TWAP60+confirmaciones. No está
resuelto por qué presión sola rinde mejor ahí — puede ser que encuentre
un subconjunto mejor de ese rango, o puede ser una particularidad de
esta muestra todavía no explicada del todo. Por eso: plata chica,
separada del capital del bot principal, con freno propio.

Completamente independiente de config.LIVE_V2_ENABLED — no toca ni
reactiva nada de la investigación principal (que sigue pausada).
"""
import logging
import os
import threading
from datetime import datetime, timezone, timedelta

import psycopg2
import psycopg2.extras

from config import DATABASE_URL

logger = logging.getLogger(__name__)

PRESSURE_ENABLED = os.environ.get("PRESSURE_BOT_ENABLED", "false").lower() == "true"

# Subido de 20 a 995 (2-sep-2026): el umbral original resultó ser casi un
# no-op — la distribución real de twap_pressure_integral tiene mediana
# 140 y percentil 90 en 2524, así que 20 dejaba pasar el 83% de TODAS
# las lecturas (ver /api/shadow-pressure-distribution). El backtest
# original (umbrales 0-40) nunca probó si la MAGNITUD de la presión
# predice algo — solo "algo de señal" vs "cero señal". Repetido con
# umbrales que sí dividen la distribución real (0/20/140/500/995/2524/
# 3920, mismo split de dos mitades por tiempo): el acierto y el $/trade
# suben limpio y ESTABLE en ambas mitades cuanto más alto el umbral —
# 995 (percentil 75) da 90.1%/90.8% de acierto y +$0.131/+$0.167 por
# dólar en la mitad vieja/nueva, con volumen todavía razonable
# (n=935-1496). 2524 (percentil 90) rinde mejor todavía (94-98%,
# +$0.22-0.33) pero con mucho menos volumen — 995 es el punto medio.
PRESSURE_THRESHOLD = 995.0

# Agregado (2-sep-2026, mismo día): el 90.1-90.8% de acierto validado
# arriba era el promedio de TODA la banda 0.50-0.99 — nunca se sub-dividió
# por precio antes de arrancar (mismo error que en su momento con
# favorite, corregido ahí, repetido acá sin querer). Los primeros trades
# reales con el umbral 995 entraron casi todos en 0.83-0.98 (mediana
# 0.94) y el día cerró en -$0.28 pese a 89.7% de acierto — la típica
# trampa de "muchas ganancias chiquitas, una pérdida se las come todas".
# Revisando por rango de precio (n=2481, señales que ya superan 995):
# <0.50 -> 88.3% acierto, +$0.857/$1; 0.50-0.75 -> 82.5%, +$0.403/$1;
# TODO lo de 0.75 para arriba -> entre -$0.024 y +$0.020/$1, ruido. El
# edge real de esta señal vive por debajo de 0.75; arriba es donde
# justo entraron casi todos los trades reales de hoy. Se corta ahí.
# 3-sep-2026: encontrado en logs que muchas órdenes fallaban con "Size X
# lower than the minimum: 5" — Polymarket exige un mínimo de 5 SHARES por
# orden (no dólares), y con TRADE_SIZE=$2, shares=$2/precio queda por
# debajo de 5 en gran parte del rango 0-0.75 (ej. a precio 0.75,
# shares=2.67). Algunas órdenes pasaban igual (parece depender del
# mercado/momento puntual, no se pudo aislar la regla exacta) pero
# muchas fallaban en silencio — el arreglo del 2-sep nunca tuvo una
# prueba limpia el 3-sep por esto. Subido a $5 para que incluso en el
# peor caso (precio justo en el techo, 0.75) haya margen cómodo por
# encima de 5 shares (6.67), no al filo.
PRESSURE_MAX_PRICE = 0.75
TRADE_SIZE = 5.0
MIN_TRADE_USD = 5.0
MAX_STAKE_USD = 5.0         # techo, por si algún día se agrega sizing — hoy TRADE_SIZE es fijo
MIN_SHARES = 5.5            # chequeo previo, margen sobre el mínimo real de Polymarket (5) —
                             # evita ni intentar una orden que sabemos que va a rebotar
DAILY_LOSS_LIMIT_USD = 20.0 # subido junto con TRADE_SIZE, misma tolerancia relativa que antes
MAX_OPEN_POSITIONS = 1      # simple a propósito

ET_OFFSET = timedelta(hours=-4)

DDL = """
CREATE TABLE IF NOT EXISTS pressure_bot_trades (
    id SERIAL PRIMARY KEY,
    market_id TEXT,
    title TEXT,
    asset TEXT,
    side TEXT,
    size FLOAT,
    price FLOAT,
    pressure_at_entry FLOAT,
    token_id TEXT,
    opened_at TIMESTAMPTZ DEFAULT NOW(),
    resolved_at TIMESTAMPTZ,
    outcome TEXT,
    pnl FLOAT,
    win BOOLEAN,
    order_resp TEXT
);

CREATE TABLE IF NOT EXISTS pressure_bot_state (
    id INT PRIMARY KEY DEFAULT 1,
    current_trading_day DATE,
    today_pnl FLOAT DEFAULT 0,
    drawdown_paused BOOLEAN DEFAULT FALSE,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
INSERT INTO pressure_bot_state (id) VALUES (1) ON CONFLICT (id) DO NOTHING;
"""


def _extract_fill_info(resp: dict, decision_price: float, intended_size: float):
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
    return decision_price, intended_size


class PressureBot:
    def __init__(self):
        self._lock = threading.Lock()
        self.total_trades = 0
        self.open_positions = {}
        self.results = []
        self.today_pnl = 0.0
        self.current_trading_day = None
        self._drawdown_paused = False
        self._client = None
        self.conn = None
        self._init_client()
        self._connect_db()

    def _init_client(self):
        try:
            from order_executor import get_client
            self._client = get_client()
            if self._client:
                logger.info("PressureBot: CLOB client ready")
        except Exception as e:
            logger.error(f"PressureBot init error: {e}")

    def _connect_db(self):
        if not DATABASE_URL:
            return
        try:
            self.conn = psycopg2.connect(DATABASE_URL)
            self.conn.autocommit = True
            with self.conn.cursor() as cur:
                cur.execute(DDL)
            self._load_state()
            logger.info("PressureBot DB connected")
        except Exception as e:
            logger.error(f"PressureBot DB error: {e}")
            self.conn = None

    def _load_state(self):
        if not self.conn:
            return
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM pressure_bot_state WHERE id=1")
                row = cur.fetchone()
                if row:
                    self.current_trading_day = row["current_trading_day"]
                    self.today_pnl = row["today_pnl"] or 0.0
                    self._drawdown_paused = row["drawdown_paused"] or False

                cur.execute("SELECT * FROM pressure_bot_trades WHERE resolved_at IS NULL")
                for row in cur.fetchall():
                    self.open_positions[row["market_id"]] = dict(row)

                cur.execute("SELECT count(*) AS n FROM pressure_bot_trades")
                self.total_trades = cur.fetchone()["n"] or 0

                if self.current_trading_day:
                    cur.execute("""
                        SELECT * FROM pressure_bot_trades
                        WHERE resolved_at IS NOT NULL
                          AND (resolved_at AT TIME ZONE 'UTC' - INTERVAL '4 hours')::date = %s
                        ORDER BY resolved_at ASC
                    """, (self.current_trading_day,))
                    for row in cur.fetchall():
                        self.results.append(dict(row))
        except Exception as e:
            logger.error(f"PressureBot load_state error: {e}")

    def _save_state(self):
        if not self.conn:
            return
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    UPDATE pressure_bot_state
                    SET current_trading_day=%s, today_pnl=%s, drawdown_paused=%s, updated_at=NOW()
                    WHERE id=1
                """, (self.current_trading_day, self.today_pnl, self._drawdown_paused))
        except Exception as e:
            logger.error(f"PressureBot save_state error: {e}")

    def _check_day_rollover(self):
        today = (datetime.now(timezone.utc) + ET_OFFSET).date()
        with self._lock:
            is_new_day = self.current_trading_day != today
            if not is_new_day:
                return
            self.current_trading_day = today
            self.today_pnl = 0.0
            self._drawdown_paused = False
            self.results = []
        self._save_state()

    def get_balance(self) -> float:
        try:
            from order_executor import get_balance
            return get_balance()
        except Exception as e:
            logger.error(f"PressureBot balance error: {e}")
            return 0.0

    def can_trade(self) -> bool:
        self._check_day_rollover()
        with self._lock:
            if self._client is None:
                return False
            if len(self.open_positions) >= MAX_OPEN_POSITIONS:
                return False
            paused = self._drawdown_paused
            today_pnl = self.today_pnl
        if paused:
            return False
        # Freno simple en dólares absolutos — a esta escala ($2/trade) no
        # hace falta nada más sofisticado que un límite fijo de pérdida.
        if today_pnl <= -DAILY_LOSS_LIMIT_USD:
            with self._lock:
                self._drawdown_paused = True
            logger.warning(f"PressureBot: freno diario activado, today_pnl=${today_pnl:.2f}")
            self._save_state()
            return False
        return True

    def evaluate(self, market: dict, pressure_integral: float):
        """Llamar por cada mercado en ventana de entrada, con la presión
        ya calculada (chainlink.get_pressure(asset, window_ts)['integral']).
        No hace nada si la señal no supera el umbral o si ya hay una
        posición en este mercado."""
        if pressure_integral is None or abs(pressure_integral) < PRESSURE_THRESHOLD:
            return
        market_id = str(market["id"])
        with self._lock:
            if market_id in self.open_positions:
                return
        if not self.can_trade():
            return

        side = "UP" if pressure_integral > 0 else "DOWN"
        price = market.get("up_price") if side == "UP" else market.get("down_price")
        tokens = market.get("tokens") or {}
        token_id = tokens.get(side)
        if not price or not token_id:
            return
        if price >= PRESSURE_MAX_PRICE:
            return
        if (TRADE_SIZE / price) < MIN_SHARES:
            return

        threading.Thread(
            target=self._execute,
            args=(market_id, market.get("title"), market["asset"], side, price, token_id, pressure_integral),
            daemon=True,
        ).start()

    def _execute(self, market_id, title, asset, side, price, token_id, pressure_integral):
        try:
            with self._lock:
                if market_id in self.open_positions:
                    return
                self.open_positions[market_id] = {"market_id": market_id, "_reserved": True}

            from order_executor import place_order
            resp = place_order(token_id=token_id, price=round(price, 2), size=TRADE_SIZE, side="BUY")

            if "error" in resp:
                with self._lock:
                    self.open_positions.pop(market_id, None)
                logger.info(f"PressureBot no fill [{asset}]: {resp['error']}")
                return

            status = resp.get("status", "")
            if status != "matched":
                with self._lock:
                    self.open_positions.pop(market_id, None)
                return

            fill_price, real_cost = _extract_fill_info(resp, price, TRADE_SIZE)
            with self._lock:
                self.total_trades += 1
                self.open_positions[market_id] = {
                    "market_id": market_id, "title": title, "asset": asset, "side": side,
                    "size": real_cost, "price": fill_price, "pressure_at_entry": pressure_integral,
                    "token_id": token_id, "opened_at": datetime.now(timezone.utc).isoformat(),
                    "order_resp": str(resp),
                }
            if self.conn:
                try:
                    with self.conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO pressure_bot_trades
                            (market_id, title, asset, side, size, price, pressure_at_entry, token_id, order_resp)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """, (market_id, title, asset, side, real_cost, fill_price, pressure_integral, token_id, str(resp)))
                except Exception as e:
                    logger.error(f"PressureBot insert error: {e}")
            logger.info(f"🟢 PressureBot ABIERTA: {side} {asset.upper()} @ {fill_price:.2f} pressure={pressure_integral:.1f}")
        except Exception as e:
            with self._lock:
                self.open_positions.pop(market_id, None)
            logger.error(f"PressureBot execute error: {e}")

    def resolve_trade(self, market_id: str, outcome: str):
        with self._lock:
            trade = self.open_positions.pop(market_id, None)
            if not trade or trade.get("_reserved"):
                return
            side = trade["side"]
            win = (side == outcome)
            entry_price = trade["price"]
            cost = trade.get("size", TRADE_SIZE)
            fee = cost * 0.07 * (1.0 - entry_price)
            if win:
                pnl = cost * ((1.0 - entry_price) / entry_price) - fee
            else:
                pnl = -cost - fee
            result = {**trade, "outcome": outcome, "win": win, "pnl": pnl}
            self.results.append(result)
            self.today_pnl += pnl
        self._save_state()
        if self.conn:
            try:
                with self.conn.cursor() as cur:
                    cur.execute("""
                        UPDATE pressure_bot_trades SET resolved_at=NOW(), outcome=%s, pnl=%s, win=%s
                        WHERE market_id=%s AND resolved_at IS NULL
                    """, (outcome, pnl, win, market_id))
            except Exception as e:
                logger.error(f"PressureBot resolve persist error: {e}")
        logger.info(f"🟢 PressureBot RESULTADO: {'GANÓ' if win else 'PERDIÓ'} {side} {trade.get('asset','').upper()} pnl={pnl:+.2f}")

    def get_stats(self) -> dict:
        with self._lock:
            wins = [r for r in self.results if r.get("win")]
            losses = [r for r in self.results if r.get("win") is False]
            total_pnl = sum(r.get("pnl", 0) or 0 for r in self.results)
            cash_balance = 0.0
            try:
                from order_executor import get_balance
                cash_balance = get_balance()
            except Exception:
                pass
            return {
                "enabled": PRESSURE_ENABLED,
                "total_trades": self.total_trades,
                "completed": len(self.results),
                "wins": len(wins),
                "losses": len(losses),
                "win_rate": len(wins) / len(self.results) * 100 if self.results else 0,
                "today_pnl": self.today_pnl,
                "total_pnl": total_pnl,
                "balance": cash_balance,
                "drawdown_paused": self._drawdown_paused,
                "open_count": len(self.open_positions),
                "recent_trades": list(reversed(self.results[-10:])),
            }


_pressure_bot = None


def get_pressure_bot():
    global _pressure_bot
    if _pressure_bot is None:
        _pressure_bot = PressureBot()
    return _pressure_bot
