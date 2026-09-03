"""
Bot chico y separado — "fade al favorito extremo" (2-sep-2026, idea del
usuario después de ver que las 5 pérdidas del pressure_bot eran todas a
precio alto, con el lado contrario pagando muchísimo si ganaba).

Idea: apostar SIEMPRE al lado improbable cada vez que el otro lado esté
a FADE_MIN_FAVORITE_PRICE o más — el pago cuando gana es enorme (~30-90x
el stake a estos precios), la pregunta es si la frecuencia real con la
que el mercado "se equivoca" alcanza para cubrir esa cantidad de
apuestas perdedoras.

Verificado con datos reales ANTES de tocar plata real (mismo criterio de
siempre, aunque el usuario haya pedido probarlo igual): con favorito
>=0.95, sobre 2,550 casos reales (20.7 días, ~123.5 oportunidades/día),
el lado improbable ganó 1.84% de las veces — el margen de pago cubriría
hasta 43.6 pérdidas por cada ganancia, pero en la práctica hicieron
falta 53.3. Resultado real: -$965.87 neto sobre $5,100 apostados
(apostando $2 fijo). Es un resultado grande (n=2550), no ruido de
muestra chica — la recomendación fue no hacerlo, el usuario decidió
probarlo igual con plata mínima.

Ajuste de tamaño explicado al usuario antes de prender esto: con ~123
oportunidades/día y 1.84% de acierto, a $2 por apuesta el resultado
ESPERADO de un día normal (no el peor caso, el esperado) ya es una
pérdida real (~-$47/día en la muestra). Se baja a $0.20 por apuesta para
que la variancia normal de esta estrategia (rachas largas de pérdidas
chicas son el comportamiento ESPERADO, no una señal de que algo está
mal) sea manejable como plata de jugar de verdad.

Apagado por defecto (FADE_BOT_ENABLED). Plan acordado: se prende
mañana SOLO si pressure_bot no da resultados positivos hoy — decisión
del usuario, no automática.
"""
import logging
import os
import threading
from datetime import datetime, timezone, timedelta

import psycopg2
import psycopg2.extras

from config import DATABASE_URL

logger = logging.getLogger(__name__)

FADE_ENABLED = os.environ.get("FADE_BOT_ENABLED", "false").lower() == "true"

FADE_MIN_FAVORITE_PRICE = 0.95  # apostar al lado contrario cuando el otro esté a esto o más
# 3-sep-2026 (arreglado ANTES de prender esto, encontrado con pressure_bot):
# Polymarket exige mínimo 5 shares por orden. El peor caso acá es cuando
# el favorito está justo en 0.95 (el lado que apostamos queda a ~0.05) —
# a $0.20 eso da 4 shares, por debajo del mínimo. Subido a $0.30 para
# que incluso en ese peor caso haya margen (6 shares).
TRADE_SIZE = 0.30
MIN_TRADE_USD = 0.30
MIN_SHARES = 5.5  # chequeo previo, margen sobre el mínimo real de Polymarket (5)
DAILY_LOSS_LIMIT_USD = 10.0     # esperar rachas largas de pérdidas es NORMAL acá, no un bug
MAX_OPEN_POSITIONS = 1

ET_OFFSET = timedelta(hours=-4)

DDL = """
CREATE TABLE IF NOT EXISTS fade_bot_trades (
    id SERIAL PRIMARY KEY,
    market_id TEXT,
    title TEXT,
    asset TEXT,
    side TEXT,
    size FLOAT,
    price FLOAT,
    favorite_price_at_entry FLOAT,
    token_id TEXT,
    opened_at TIMESTAMPTZ DEFAULT NOW(),
    resolved_at TIMESTAMPTZ,
    outcome TEXT,
    pnl FLOAT,
    win BOOLEAN,
    order_resp TEXT
);

CREATE TABLE IF NOT EXISTS fade_bot_state (
    id INT PRIMARY KEY DEFAULT 1,
    current_trading_day DATE,
    today_pnl FLOAT DEFAULT 0,
    drawdown_paused BOOLEAN DEFAULT FALSE,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
INSERT INTO fade_bot_state (id) VALUES (1) ON CONFLICT (id) DO NOTHING;
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


class FadeFavoriteBot:
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
                logger.info("FadeFavoriteBot: CLOB client ready")
        except Exception as e:
            logger.error(f"FadeFavoriteBot init error: {e}")

    def _connect_db(self):
        if not DATABASE_URL:
            return
        try:
            self.conn = psycopg2.connect(DATABASE_URL)
            self.conn.autocommit = True
            with self.conn.cursor() as cur:
                cur.execute(DDL)
            self._load_state()
            logger.info("FadeFavoriteBot DB connected")
        except Exception as e:
            logger.error(f"FadeFavoriteBot DB error: {e}")
            self.conn = None

    def _load_state(self):
        if not self.conn:
            return
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM fade_bot_state WHERE id=1")
                row = cur.fetchone()
                if row:
                    self.current_trading_day = row["current_trading_day"]
                    self.today_pnl = row["today_pnl"] or 0.0
                    self._drawdown_paused = row["drawdown_paused"] or False

                cur.execute("SELECT * FROM fade_bot_trades WHERE resolved_at IS NULL")
                for row in cur.fetchall():
                    self.open_positions[row["market_id"]] = dict(row)

                cur.execute("SELECT count(*) AS n FROM fade_bot_trades")
                self.total_trades = cur.fetchone()["n"] or 0

                if self.current_trading_day:
                    cur.execute("""
                        SELECT * FROM fade_bot_trades
                        WHERE resolved_at IS NOT NULL
                          AND (resolved_at AT TIME ZONE 'UTC' - INTERVAL '4 hours')::date = %s
                        ORDER BY resolved_at ASC
                    """, (self.current_trading_day,))
                    for row in cur.fetchall():
                        self.results.append(dict(row))
        except Exception as e:
            logger.error(f"FadeFavoriteBot load_state error: {e}")

    def _save_state(self):
        if not self.conn:
            return
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    UPDATE fade_bot_state
                    SET current_trading_day=%s, today_pnl=%s, drawdown_paused=%s, updated_at=NOW()
                    WHERE id=1
                """, (self.current_trading_day, self.today_pnl, self._drawdown_paused))
        except Exception as e:
            logger.error(f"FadeFavoriteBot save_state error: {e}")

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
            logger.error(f"FadeFavoriteBot balance error: {e}")
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
        if today_pnl <= -DAILY_LOSS_LIMIT_USD:
            with self._lock:
                self._drawdown_paused = True
            logger.warning(f"FadeFavoriteBot: freno diario activado, today_pnl=${today_pnl:.2f}")
            self._save_state()
            return False
        return True

    def evaluate(self, market: dict):
        """Llamar por cada mercado en ventana de entrada. Mira los DOS
        precios del mercado directo (no depende de ninguna señal de
        TWAP/presión/confirmaciones) — si alguno de los dos lados está
        en FADE_MIN_FAVORITE_PRICE o más, apuesta al OTRO lado."""
        up_price = market.get("up_price")
        down_price = market.get("down_price")
        if not up_price or not down_price:
            return

        if up_price >= FADE_MIN_FAVORITE_PRICE:
            side, price, fav_price = "DOWN", down_price, up_price
        elif down_price >= FADE_MIN_FAVORITE_PRICE:
            side, price, fav_price = "UP", up_price, down_price
        else:
            return

        market_id = str(market["id"])
        with self._lock:
            if market_id in self.open_positions:
                return
        if not self.can_trade():
            return

        tokens = market.get("tokens") or {}
        token_id = tokens.get(side)
        if not price or not token_id:
            return

        # Polymarket exige mínimo 5 shares por orden (encontrado en producción
        # con pressure_bot: "Size (X) lower than the minimum: 5"). Chequeo
        # previo para no intentar una orden que sabemos que va a rechazar.
        if (TRADE_SIZE / price) < MIN_SHARES:
            return

        threading.Thread(
            target=self._execute,
            args=(market_id, market.get("title"), market["asset"], side, price, token_id, fav_price),
            daemon=True,
        ).start()

    def _execute(self, market_id, title, asset, side, price, token_id, fav_price):
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
                    "size": real_cost, "price": fill_price, "favorite_price_at_entry": fav_price,
                    "token_id": token_id, "opened_at": datetime.now(timezone.utc).isoformat(),
                    "order_resp": str(resp),
                }
            if self.conn:
                try:
                    with self.conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO fade_bot_trades
                            (market_id, title, asset, side, size, price, favorite_price_at_entry, token_id, order_resp)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """, (market_id, title, asset, side, real_cost, fill_price, fav_price, token_id, str(resp)))
                except Exception as e:
                    logger.error(f"FadeFavoriteBot insert error: {e}")
            logger.info(f"🟠 FadeFavoriteBot ABIERTA: {side} {asset.upper()} @ {fill_price:.3f} (contra favorito @{fav_price:.2f})")
        except Exception as e:
            with self._lock:
                self.open_positions.pop(market_id, None)
            logger.error(f"FadeFavoriteBot execute error: {e}")

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
                        UPDATE fade_bot_trades SET resolved_at=NOW(), outcome=%s, pnl=%s, win=%s
                        WHERE market_id=%s AND resolved_at IS NULL
                    """, (outcome, pnl, win, market_id))
            except Exception as e:
                logger.error(f"FadeFavoriteBot resolve persist error: {e}")
        logger.info(f"🟠 FadeFavoriteBot RESULTADO: {'GANÓ' if win else 'perdió'} {side} {trade.get('asset','').upper()} pnl={pnl:+.2f}")

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
                "enabled": FADE_ENABLED,
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


_fade_bot = None


def get_fade_bot():
    global _fade_bot
    if _fade_bot is None:
        _fade_bot = FadeFavoriteBot()
    return _fade_bot
