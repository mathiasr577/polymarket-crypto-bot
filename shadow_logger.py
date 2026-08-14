"""
Shadow-mode logger — Fase 1 (validación de ingeniería, 20-50 mercados).

No toca NINGUNA decisión de trading (ni paper ni live). Por cada mercado que
entra en la ventana de evaluación de signal_engine.py, guarda un snapshot
estructurado de todos los precios de referencia relevantes — Kraken spot
(lo que usa el modelo viejo), Chainlink spot, y Chainlink TWAP de 30s y 60s
(apertura de la ventana y último valor visto) — más lo que el modelo viejo
decidió (z, p_model, side). Cuando el mercado resuelve, completa la fila con
el ganador real de Polymarket y el último valor visto de cada feed como
aproximación al cierre.

Objetivo de esta fase: la prueba binaria — ¿el ganador reconstruido a partir
del delta TWAP (open vs. close, 30s y 60s por separado) coincide con el
ganador real que resuelve Polymarket? Si no coincide de forma consistente,
el problema está en cómo se están leyendo timestamps/ventanas/boundaries, no
en ningún modelo predictivo — y no hay que tocar el modelo real hasta
resolver eso. Con 20-50 mercados alcanza para ver si hay un patrón de
discrepancia sistemático; recién con 100-300+ tiene sentido buscar señal.

No hay replay en RTDS (ver chainlink_feed.py) — el valor de "cierre" que se
guarda es el último valor visto en vivo dentro de la ventana de 5 min del
mercado, no una reconstrucción posterior.
"""
import logging
import threading
import time
import json
import requests
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone

from config import DATABASE_URL, GAMMA_API

logger = logging.getLogger(__name__)

DDL = """
CREATE TABLE IF NOT EXISTS shadow_decisions (
    id SERIAL PRIMARY KEY,
    market_id TEXT UNIQUE,
    asset TEXT,
    title TEXT,
    window_ts BIGINT,
    market_open_timestamp TIMESTAMPTZ,
    decision_timestamp TIMESTAMPTZ DEFAULT NOW(),
    seconds_remaining FLOAT,

    kraken_ref_open FLOAT,
    kraken_spot_now FLOAT,
    kraken_spot_close FLOAT,

    chainlink_spot_now FLOAT,
    chainlink_spot_ts BIGINT,
    chainlink_spot_close FLOAT,

    twap30_open FLOAT,
    twap30_open_ts BIGINT,
    twap30_now FLOAT,
    twap30_now_ts BIGINT,
    twap30_close FLOAT,
    twap30_close_ts BIGINT,
    twap30_window_s INT,

    twap60_open FLOAT,
    twap60_open_ts BIGINT,
    twap60_now FLOAT,
    twap60_now_ts BIGINT,
    twap60_close FLOAT,
    twap60_close_ts BIGINT,
    twap60_window_s INT,

    feed_connected BOOLEAN,
    feed_reconnect_count INT,
    feed_lag_ms FLOAT,
    data_gap BOOLEAN,

    sigma FLOAT,
    z_old FLOAT,
    p_old FLOAT,
    old_model_side TEXT,

    polymarket_up_price FLOAT,
    polymarket_down_price FLOAT,

    resolved_at TIMESTAMPTZ,
    actual_outcome TEXT,
    predicted_winner_twap30 TEXT,
    predicted_winner_twap60 TEXT,
    predicted_winner_chainlink_spot TEXT,
    predicted_winner_kraken TEXT
);
"""


def _safe(val):
    if val is None:
        return None
    try:
        return float(val)
    except Exception:
        return None


def _winner(delta):
    if delta is None:
        return None
    if delta > 0:
        return "UP"
    if delta < 0:
        return "DOWN"
    return None  # empate exacto — dejarlo explícito en vez de forzar un lado


def _determine_outcome(m: dict) -> str | None:
    """Copia deliberada de la misma función en main.py — se evita import
    cruzado para no crear un ciclo (main.py importa este módulo)."""
    outcomes = m.get("outcomes")
    if isinstance(outcomes, str):
        try:
            outcomes = json.loads(outcomes)
        except Exception:
            return None

    resolved = m.get("resolvedOutcome") or m.get("resolved_outcome")
    if resolved is not None:
        try:
            idx = int(resolved)
            if outcomes and idx < len(outcomes):
                return outcomes[idx].strip().upper()
        except Exception:
            pass

    outcome_prices = m.get("outcomePrices")
    if isinstance(outcome_prices, str):
        try:
            outcome_prices = json.loads(outcome_prices)
        except Exception:
            return None

    if outcome_prices and outcomes:
        for i, price in enumerate(outcome_prices):
            try:
                if float(price) >= 0.99 and i < len(outcomes):
                    return outcomes[i].strip().upper()
            except Exception:
                pass
    return None


class ShadowLogger:
    def __init__(self):
        self.conn = None
        self._lock = threading.Lock()
        self._logged = set()  # market_ids ya loggeados en este proceso
        self._connect()

    def _connect(self):
        if not DATABASE_URL:
            logger.warning("ShadowLogger: no DATABASE_URL — deshabilitado")
            return
        try:
            self.conn = psycopg2.connect(DATABASE_URL)
            self.conn.autocommit = True
            with self.conn.cursor() as cur:
                cur.execute(DDL)
            logger.info("ShadowLogger DB connected")
        except Exception as e:
            logger.error(f"ShadowLogger DB error: {e}")
            self.conn = None

    def log_decision(self, market, indicators, signal, chainlink_feed, kraken_window_ts, kraken_ref_open):
        if not self.conn:
            return
        market_id = market["id"]
        with self._lock:
            if market_id in self._logged:
                return
            self._logged.add(market_id)

        asset = market["asset"]
        snap = chainlink_feed.get_snapshot(asset)
        w30 = chainlink_feed.get_window_twap(asset, 30, kraken_window_ts)
        w60 = chainlink_feed.get_window_twap(asset, 60, kraken_window_ts)
        twap30_open = w30.get("open")
        twap60_open = w60.get("open")

        data_gap = not all([
            snap.get("feed_connected"),
            snap.get("twap30_now") is not None,
            snap.get("twap60_now") is not None,
            twap30_open is not None,
            twap60_open is not None,
        ])
        if data_gap:
            logger.info(
                f"Shadow log [{asset.upper()} {market_id}]: data_gap=True "
                f"(connected={snap.get('feed_connected')} twap30_open={twap30_open} "
                f"twap30_now={snap.get('twap30_now')} twap60_open={twap60_open} "
                f"twap60_now={snap.get('twap60_now')})"
            )

        row = {
            "market_id": market_id,
            "asset": asset,
            "title": market.get("title"),
            "window_ts": kraken_window_ts,
            "market_open_timestamp": datetime.fromtimestamp(kraken_window_ts, tz=timezone.utc),
            "seconds_remaining": _safe(market.get("seconds_left")),

            "kraken_ref_open": _safe(kraken_ref_open),
            "kraken_spot_now": _safe(indicators.get("price")),

            "chainlink_spot_now": _safe(snap.get("chainlink_spot")),
            "chainlink_spot_ts": snap.get("chainlink_spot_ts"),

            "twap30_open": _safe(twap30_open),
            "twap30_open_ts": w30.get("open_ts"),
            "twap30_now": _safe(snap.get("twap30_now")),
            "twap30_now_ts": snap.get("twap30_now_ts"),
            "twap30_window_s": snap.get("twap30_window_s"),

            "twap60_open": _safe(twap60_open),
            "twap60_open_ts": w60.get("open_ts"),
            "twap60_now": _safe(snap.get("twap60_now")),
            "twap60_now_ts": snap.get("twap60_now_ts"),
            "twap60_window_s": snap.get("twap60_window_s"),

            "feed_connected": snap.get("feed_connected"),
            "feed_reconnect_count": snap.get("feed_reconnect_count"),
            "feed_lag_ms": _safe(snap.get("feed_lag_ms")),
            "data_gap": data_gap,

            "sigma": _safe(indicators.get("vol_per_sqrt_sec")),
            "z_old": _safe(signal.get("z")),
            "p_old": _safe(signal.get("model_prob")),
            "old_model_side": signal.get("side"),

            "polymarket_up_price": _safe(market.get("up_price")),
            "polymarket_down_price": _safe(market.get("down_price")),
        }

        cols = ", ".join(row.keys())
        placeholders = ", ".join(f"%({k})s" for k in row.keys())
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO shadow_decisions ({cols}) VALUES ({placeholders}) "
                    f"ON CONFLICT (market_id) DO NOTHING",
                    row,
                )
            logger.info(f"Shadow log [{asset.upper()} {market_id}] T-{market.get('seconds_left'):.0f}s recorded")
        except Exception as e:
            logger.error(f"Shadow log insert error: {e}")

    def resolve_pending(self, chainlink_feed, kraken_feed):
        if not self.conn:
            return
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT market_id, asset, window_ts, kraken_ref_open,
                           twap30_open, twap60_open, chainlink_spot_now
                    FROM shadow_decisions WHERE resolved_at IS NULL
                """)
                pending = cur.fetchall()
        except Exception as e:
            logger.error(f"Shadow resolve query error: {e}")
            return

        for row in pending:
            market_id = row["market_id"]
            try:
                r = requests.get(f"{GAMMA_API}/markets/{market_id}", timeout=8)
                if r.status_code != 200:
                    continue
                m = r.json()
                if not (m.get("closed") or m.get("resolved")):
                    continue
                outcome = _determine_outcome(m)
                if not outcome:
                    continue
                self._finalize(row, outcome, chainlink_feed, kraken_feed)
            except Exception as e:
                logger.debug(f"Shadow resolve error {market_id}: {e}")

    def _finalize(self, row, outcome, chainlink_feed, kraken_feed):
        asset = row["asset"]
        window_ts = row["window_ts"]

        w30 = chainlink_feed.get_window_twap(asset, 30, window_ts)
        w60 = chainlink_feed.get_window_twap(asset, 60, window_ts)
        twap30_close = w30.get("last")
        twap60_close = w60.get("last")
        kraken_close = kraken_feed.get_window_last_price(asset, window_ts)
        chainlink_spot_close = chainlink_feed.get_snapshot(asset).get("chainlink_spot")

        pred_30 = _winner(twap30_close - row["twap30_open"]) if (twap30_close is not None and row["twap30_open"] is not None) else None
        pred_60 = _winner(twap60_close - row["twap60_open"]) if (twap60_close is not None and row["twap60_open"] is not None) else None
        pred_spot = _winner(chainlink_spot_close - row["chainlink_spot_now"]) if (chainlink_spot_close is not None and row["chainlink_spot_now"] is not None) else None
        pred_kraken = _winner(kraken_close - row["kraken_ref_open"]) if (kraken_close is not None and row["kraken_ref_open"] is not None) else None

        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    UPDATE shadow_decisions SET
                        resolved_at = NOW(),
                        actual_outcome = %(outcome)s,
                        twap30_close = %(twap30_close)s,
                        twap30_close_ts = %(twap30_close_ts)s,
                        twap60_close = %(twap60_close)s,
                        twap60_close_ts = %(twap60_close_ts)s,
                        kraken_spot_close = %(kraken_close)s,
                        chainlink_spot_close = %(chainlink_spot_close)s,
                        predicted_winner_twap30 = %(pred_30)s,
                        predicted_winner_twap60 = %(pred_60)s,
                        predicted_winner_chainlink_spot = %(pred_spot)s,
                        predicted_winner_kraken = %(pred_kraken)s
                    WHERE market_id = %(market_id)s
                """, {
                    "outcome": outcome,
                    "twap30_close": _safe(twap30_close),
                    "twap30_close_ts": w30.get("last_ts"),
                    "twap60_close": _safe(twap60_close),
                    "twap60_close_ts": w60.get("last_ts"),
                    "kraken_close": _safe(kraken_close),
                    "chainlink_spot_close": _safe(chainlink_spot_close),
                    "pred_30": pred_30,
                    "pred_60": pred_60,
                    "pred_spot": pred_spot,
                    "pred_kraken": pred_kraken,
                    "market_id": row["market_id"],
                })
            match_30 = "✅" if pred_30 == outcome else ("❌" if pred_30 else "?")
            match_60 = "✅" if pred_60 == outcome else ("❌" if pred_60 else "?")
            logger.info(
                f"Shadow resolved [{asset.upper()} {row['market_id']}]: actual={outcome} "
                f"twap30_pred={pred_30}{match_30} twap60_pred={pred_60}{match_60} "
                f"kraken_pred={pred_kraken}"
            )
        except Exception as e:
            logger.error(f"Shadow finalize error: {e}")

    def get_validation_stats(self) -> dict:
        """Resumen rápido de la prueba binaria de fase 1: cuántas veces cada
        fuente reconstruye correctamente el ganador real."""
        if not self.conn:
            return {}
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT
                        COUNT(*) FILTER (WHERE resolved_at IS NOT NULL) as resolved,
                        COUNT(*) FILTER (WHERE resolved_at IS NOT NULL AND NOT data_gap) as resolved_clean,
                        COUNT(*) FILTER (WHERE predicted_winner_twap30 = actual_outcome AND NOT data_gap) as twap30_correct,
                        COUNT(*) FILTER (WHERE predicted_winner_twap60 = actual_outcome AND NOT data_gap) as twap60_correct,
                        COUNT(*) FILTER (WHERE predicted_winner_kraken = actual_outcome AND NOT data_gap) as kraken_correct,
                        COUNT(*) FILTER (WHERE data_gap) as data_gaps
                    FROM shadow_decisions
                """)
                return dict(cur.fetchone())
        except Exception as e:
            logger.error(f"Shadow stats error: {e}")
            return {}

    def get_decision_time_backtest(self) -> dict:
        """A diferencia de get_validation_stats() (que mide si el TWAP de
        CIERRE reconstruye el ganador real — la prueba de que entendemos
        bien el mecanismo de resolución), esto mide algo distinto y más
        relevante para trading: si en el momento de la DECISIÓN (T-120 a
        T-55s, antes de saber el cierre) el delta del TWAP acumulado hasta
        ese instante (twap_now - twap_open) ya apuntaba al lado correcto,
        comparado contra lo que el modelo viejo (Kraken + z-score + drift +
        dampener) realmente predijo en esos mismos mercados.

        Incluye además el desglose de cuando TWAP60 y el modelo viejo
        DISCREPAN en el lado — ahí es donde importa saber si el TWAP aporta
        información nueva o si es redundante con lo que ya teníamos."""
        if not self.conn:
            return {}
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    WITH base AS (
                        SELECT
                            actual_outcome,
                            old_model_side,
                            CASE WHEN twap30_now > twap30_open THEN 'UP'
                                 WHEN twap30_now < twap30_open THEN 'DOWN' END AS twap30_side,
                            CASE WHEN twap60_now > twap60_open THEN 'UP'
                                 WHEN twap60_now < twap60_open THEN 'DOWN' END AS twap60_side,
                            CASE WHEN kraken_spot_now > kraken_ref_open THEN 'UP'
                                 WHEN kraken_spot_now < kraken_ref_open THEN 'DOWN' END AS kraken_raw_side
                        FROM shadow_decisions
                        WHERE resolved_at IS NOT NULL AND NOT data_gap
                    )
                    SELECT
                        COUNT(*) AS n,
                        COUNT(*) FILTER (WHERE old_model_side IS NOT NULL) AS n_old_model,
                        COUNT(*) FILTER (WHERE old_model_side = actual_outcome) AS old_model_correct,
                        COUNT(*) FILTER (WHERE twap30_side IS NOT NULL) AS n_twap30,
                        COUNT(*) FILTER (WHERE twap30_side = actual_outcome) AS twap30_correct,
                        COUNT(*) FILTER (WHERE twap60_side IS NOT NULL) AS n_twap60,
                        COUNT(*) FILTER (WHERE twap60_side = actual_outcome) AS twap60_correct,
                        COUNT(*) FILTER (WHERE kraken_raw_side IS NOT NULL) AS n_kraken_raw,
                        COUNT(*) FILTER (WHERE kraken_raw_side = actual_outcome) AS kraken_raw_correct,
                        COUNT(*) FILTER (
                            WHERE twap60_side IS NOT NULL AND old_model_side IS NOT NULL
                              AND twap60_side != old_model_side
                        ) AS disagree_n,
                        COUNT(*) FILTER (
                            WHERE twap60_side IS NOT NULL AND old_model_side IS NOT NULL
                              AND twap60_side != old_model_side AND twap60_side = actual_outcome
                        ) AS disagree_twap60_right,
                        COUNT(*) FILTER (
                            WHERE twap60_side IS NOT NULL AND old_model_side IS NOT NULL
                              AND twap60_side != old_model_side AND old_model_side = actual_outcome
                        ) AS disagree_old_right
                    FROM base
                """)
                return dict(cur.fetchone())
        except Exception as e:
            logger.error(f"Shadow backtest error: {e}")
            return {}


_logger_instance = ShadowLogger()


def get_shadow_logger():
    return _logger_instance
