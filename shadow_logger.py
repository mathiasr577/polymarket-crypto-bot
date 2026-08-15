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
    old_model_raw_side TEXT,
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

-- CREATE TABLE IF NOT EXISTS no altera una tabla que ya existe — esta
-- columna se agregó después del deploy inicial, así que hace falta
-- migrarla explícitamente en tablas viejas.
ALTER TABLE shadow_decisions ADD COLUMN IF NOT EXISTS old_model_raw_side TEXT;
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
            "old_model_raw_side": signal.get("raw_side"),
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
        comparado contra lo que el modelo viejo predijo en esos mismos
        mercados.

        Ojo con esto: "old_model_side" solo se llena cuando el modelo viejo
        pasó TODOS sus filtros (precio, edge) y decidió tradear — es un
        subconjunto chico y auto-seleccionado (sus casos "más seguros"), muy
        distinto en tamaño a TWAP/Kraken evaluados sobre TODOS los mercados.
        Comparar 71% (n=56, filtrado) contra 81% (n=300, sin filtrar) no es
        justo. Por eso también se usa "old_model_raw_side": hacia dónde se
        inclina el modelo SIEMPRE, haya pasado el filtro o no — con eso sí
        se compara manzanas con manzanas, misma población que TWAP/Kraken.
        old_model_side/old_model_correct se mantienen aparte como métrica de
        "qué tan bien elige el modelo viejo SUS propios mejores casos", que
        es una pregunta distinta y también válida.

        Incluye además el desglose de cuando TWAP60 y la inclinación cruda
        del modelo viejo DISCREPAN en el lado — ahí es donde importa saber
        si el TWAP aporta información nueva o es redundante con Kraken."""
        if not self.conn:
            return {}
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    WITH base AS (
                        SELECT
                            actual_outcome,
                            old_model_side,
                            old_model_raw_side,
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
                        -- filtro estricto del modelo viejo (sus casos elegidos, n chico)
                        COUNT(*) FILTER (WHERE old_model_side IS NOT NULL) AS n_old_model_filtered,
                        COUNT(*) FILTER (WHERE old_model_side = actual_outcome) AS old_model_filtered_correct,
                        -- inclinación cruda del modelo viejo, misma población que TWAP/Kraken
                        COUNT(*) FILTER (WHERE old_model_raw_side IS NOT NULL) AS n_old_model_raw,
                        COUNT(*) FILTER (WHERE old_model_raw_side = actual_outcome) AS old_model_raw_correct,
                        COUNT(*) FILTER (WHERE twap30_side IS NOT NULL) AS n_twap30,
                        COUNT(*) FILTER (WHERE twap30_side = actual_outcome) AS twap30_correct,
                        COUNT(*) FILTER (WHERE twap60_side IS NOT NULL) AS n_twap60,
                        COUNT(*) FILTER (WHERE twap60_side = actual_outcome) AS twap60_correct,
                        COUNT(*) FILTER (WHERE kraken_raw_side IS NOT NULL) AS n_kraken_raw,
                        COUNT(*) FILTER (WHERE kraken_raw_side = actual_outcome) AS kraken_raw_correct,
                        COUNT(*) FILTER (
                            WHERE twap60_side IS NOT NULL AND old_model_raw_side IS NOT NULL
                              AND twap60_side != old_model_raw_side
                        ) AS disagree_n,
                        COUNT(*) FILTER (
                            WHERE twap60_side IS NOT NULL AND old_model_raw_side IS NOT NULL
                              AND twap60_side != old_model_raw_side AND twap60_side = actual_outcome
                        ) AS disagree_twap60_right,
                        COUNT(*) FILTER (
                            WHERE twap60_side IS NOT NULL AND old_model_raw_side IS NOT NULL
                              AND twap60_side != old_model_raw_side AND old_model_raw_side = actual_outcome
                        ) AS disagree_old_right
                    FROM base
                """)
                return dict(cur.fetchone())
        except Exception as e:
            logger.error(f"Shadow backtest error: {e}")
            return {}

    def get_calibration_report(self) -> dict:
        """La pregunta que importa de verdad no es "¿qué tan seguido acierta
        el lado?" sino "¿le gana al breakeven que exige el precio al que
        realmente se compra?" — con el fee del 7%, el breakeven sube muy
        rápido con el precio (a 0.50 hace falta ~52%, a 0.80 hace falta
        ~81%). Un modelo con 72% de acierto puede perder plata sistemática-
        mente si compra sobre todo en precios donde el breakeven real ya
        pasó ese 72%.

        CORREGIDO (bug detectado en producción): la versión anterior
        agrupaba SIEMPRE por el precio del lado que eligió el modelo VIEJO,
        y con esa banda medía el acierto de TWAP60 — es decir, comparaba el
        acierto de TWAP en mercados donde el precio del lado de TWAP podía
        ser completamente distinto al banded. Dos poblaciones mezcladas sin
        querer. Ahora cada modelo se agrupa por SU PROPIO precio, de forma
        independiente — self-consistent, como ya hacía correctamente
        simulate_v2_policy() (que es donde se detectó la discrepancia: un
        77% "corregido" ahí vs. un 72% mal calculado acá para la misma
        banda, antes de este fix)."""
        if not self.conn:
            return {}
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT
                        actual_outcome, old_model_raw_side, p_old,
                        polymarket_up_price, polymarket_down_price,
                        twap60_open, twap60_now
                    FROM shadow_decisions
                    WHERE resolved_at IS NOT NULL AND NOT data_gap
                """)
                rows = cur.fetchall()
        except Exception as e:
            logger.error(f"Calibration query error: {e}")
            return {}

        def breakeven(price, fee=0.07):
            return price / (price + (1 - fee) * (1 - price))

        bands = [(0.0, 0.45), (0.45, 0.55), (0.55, 0.65), (0.65, 0.75), (0.75, 0.85), (0.85, 1.01)]
        band_reports = []
        for lo, hi in bands:
            old_n = old_correct = twap_n = twap_correct = 0
            old_prices, twap_prices, p_olds = [], [], []
            for row in rows:
                old_side = row["old_model_raw_side"]
                if old_side is not None:
                    old_price = row["polymarket_up_price"] if old_side == "UP" else row["polymarket_down_price"]
                    if old_price is not None and lo <= old_price < hi:
                        old_n += 1
                        old_prices.append(old_price)
                        if row["p_old"] is not None:
                            p_olds.append(row["p_old"])
                        if old_side == row["actual_outcome"]:
                            old_correct += 1

                if row["twap60_now"] is not None and row["twap60_open"] is not None:
                    diff = row["twap60_now"] - row["twap60_open"]
                    twap_side = "UP" if diff > 0 else ("DOWN" if diff < 0 else None)
                    if twap_side is not None:
                        twap_price = row["polymarket_up_price"] if twap_side == "UP" else row["polymarket_down_price"]
                        if twap_price is not None and lo <= twap_price < hi:
                            twap_n += 1
                            twap_prices.append(twap_price)
                            if twap_side == row["actual_outcome"]:
                                twap_correct += 1

            if old_n == 0 and twap_n == 0:
                continue

            old_wr = old_correct / old_n if old_n else None
            twap_wr = twap_correct / twap_n if twap_n else None
            old_be = breakeven(sum(old_prices) / len(old_prices)) if old_prices else None
            twap_be = breakeven(sum(twap_prices) / len(twap_prices)) if twap_prices else None

            band_reports.append({
                "band": f"{lo:.2f}-{hi:.2f}",
                "old_model_avg_price": round(sum(old_prices) / len(old_prices), 3) if old_prices else None,
                "old_model_breakeven_needed": round(old_be, 3) if old_be else None,
                "old_model_n": old_n,
                "old_model_win_rate": round(old_wr, 3) if old_wr is not None else None,
                "old_model_avg_theoretical_p": round(sum(p_olds) / len(p_olds), 3) if p_olds else None,
                "old_model_beats_breakeven": (old_wr > old_be) if (old_wr is not None and old_be is not None) else None,
                "twap60_avg_price": round(sum(twap_prices) / len(twap_prices), 3) if twap_prices else None,
                "twap60_breakeven_needed": round(twap_be, 3) if twap_be else None,
                "twap60_n": twap_n,
                "twap60_win_rate": round(twap_wr, 3) if twap_wr is not None else None,
                "twap60_beats_breakeven": (twap_wr > twap_be) if (twap_wr is not None and twap_be is not None) else None,
            })
        return {"bands": band_reports, "total_n": len(rows)}

    def get_probability_calibration(self) -> dict:
        """Diagrama de confiabilidad clásico: agrupa por la probabilidad
        TEÓRICA que el modelo se auto-asignó (p_old, la fórmula de campana
        de Gauss) y compara contra el acierto EMPÍRICO real en cada grupo.
        Si p_old sistemáticamente queda por encima del acierto real, el
        modelo está sobreconfiado — toma posiciones creyendo tener más edge
        del que realmente tiene, independiente de qué feed de precio use.
        Confirmado como patrón conocido en la literatura de calibración de
        modelos: la sobreconfianza empuja a tomar posiciones agresivas con
        costos de transacción (fee, en nuestro caso) que en la realidad no
        se justifican."""
        if not self.conn:
            return {}
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT
                        WIDTH_BUCKET(p_old, 0.5, 0.95, 9) AS bucket,
                        COUNT(*) AS n,
                        AVG(p_old) AS avg_theoretical_p,
                        COUNT(*) FILTER (WHERE old_model_raw_side = actual_outcome)::float
                            / COUNT(*) AS empirical_win_rate
                    FROM shadow_decisions
                    WHERE resolved_at IS NOT NULL AND NOT data_gap
                      AND old_model_raw_side IS NOT NULL AND p_old IS NOT NULL
                    GROUP BY bucket
                    ORDER BY bucket
                """)
                rows = [dict(r) for r in cur.fetchall()]
                for r in rows:
                    r["avg_theoretical_p"] = round(r["avg_theoretical_p"], 3)
                    r["empirical_win_rate"] = round(r["empirical_win_rate"], 3)
                    r["overconfident"] = r["avg_theoretical_p"] > r["empirical_win_rate"]
                return {"buckets": rows}
        except Exception as e:
            logger.error(f"Calibration curve error: {e}")
            return {}

    def get_weekday_weekend_report(self) -> dict:
        """Chequea con datos propios la hipótesis de que el fin de semana
        rinde peor por spreads más anchos (volumen institucional de cripto
        cae 20-40% el fin de semana, según investigación externa — ver
        conversación). Compara precio promedio pagado y acierto real entre
        semana vs. fin de semana, para el mismo lado que el modelo viejo
        eligió."""
        if not self.conn:
            return {}
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT
                        (EXTRACT(DOW FROM decision_timestamp) IN (0, 6)) AS is_weekend,
                        COUNT(*) AS n,
                        AVG(CASE WHEN old_model_raw_side = 'UP' THEN polymarket_up_price ELSE polymarket_down_price END) AS avg_price_paid,
                        COUNT(*) FILTER (WHERE old_model_raw_side = actual_outcome)::float / COUNT(*) AS win_rate
                    FROM shadow_decisions
                    WHERE resolved_at IS NOT NULL AND NOT data_gap AND old_model_raw_side IS NOT NULL
                    GROUP BY is_weekend
                """)
                rows = [dict(r) for r in cur.fetchall()]
                for r in rows:
                    r["label"] = "fin de semana" if r["is_weekend"] else "entre semana"
                    r["avg_price_paid"] = round(r["avg_price_paid"], 3) if r["avg_price_paid"] else None
                    r["win_rate"] = round(r["win_rate"], 3)
                return {"rows": rows}
        except Exception as e:
            logger.error(f"Weekday/weekend report error: {e}")
            return {}

    def get_lead_signal_report(self) -> dict:
        """Prueba la hipótesis del 'D_lead' que quedó pendiente desde el
        principio: como TWAP es una ventana suavizada, el spot de Chainlink
        podría adelantarse a hacia dónde se mueve el TWAP dentro de la misma
        ventana. Si D_lead = spot_now - twap60_now predice hacia dónde se
        mueve el TWAP DESPUÉS (twap60_close - twap60_now), es una señal
        predictiva genuina y distinta de "¿el TWAP ya es preciso?" (eso ya
        está confirmado en shadow-stats). Si no le pega mejor que una
        moneda al aire, la hipótesis del lead no se sostiene y no vale la
        pena construir nada sobre ella."""
        if not self.conn:
            return {}
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT
                        COUNT(*) AS n,
                        COUNT(*) FILTER (
                            WHERE SIGN(chainlink_spot_now - twap60_now) = SIGN(twap60_close - twap60_now)
                              AND chainlink_spot_now != twap60_now AND twap60_close != twap60_now
                        ) AS lead_predicts_twap_move,
                        COUNT(*) FILTER (
                            WHERE (CASE WHEN chainlink_spot_now > twap60_now THEN 'UP'
                                        WHEN chainlink_spot_now < twap60_now THEN 'DOWN' END) = actual_outcome
                        ) AS lead_predicts_actual_outcome
                    FROM shadow_decisions
                    WHERE resolved_at IS NOT NULL AND NOT data_gap
                      AND chainlink_spot_now IS NOT NULL AND twap60_now IS NOT NULL AND twap60_close IS NOT NULL
                """)
                row = dict(cur.fetchone())
                return row
        except Exception as e:
            logger.error(f"Lead signal report error: {e}")
            return {}

    def simulate_v2_policy(self, cheap_max=0.55, favorite_min=0.85,
                            trade_favorite_band=True, stake=5.0, fee=0.07) -> dict:
        """Backtest de la política v2 propuesta: tradear el lado que indica
        TWAP60 (twap60_now vs twap60_open), pero SOLO en bandas de precio
        que get_calibration_report() ya mostró que le ganan al breakeven —
        evitando la zona 0.55-0.85 donde ni TWAP ni el modelo viejo
        funcionan hoy. Corre sobre TODO el historial ya recolectado, con el
        tamaño de posición y fórmula de fee reales del bot, para dar un
        número de plata simulada concreto en vez de solo % de acierto.

        No ejecuta nada — es 100% retroactivo sobre datos ya guardados.
        Reporta también un desglose por banda para poder ver si el
        resultado está dominado por unos pocos mercados correlacionados
        (mala señal) o distribuido parejo (buena señal)."""
        if not self.conn:
            return {}
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT actual_outcome, polymarket_up_price, polymarket_down_price,
                           twap60_open, twap60_now, decision_timestamp
                    FROM shadow_decisions
                    WHERE resolved_at IS NOT NULL AND NOT data_gap
                      AND twap60_open IS NOT NULL AND twap60_now IS NOT NULL
                """)
                rows = cur.fetchall()
        except Exception as e:
            logger.error(f"v2 policy simulation query error: {e}")
            return {}

        def band_of(price):
            if price < cheap_max:
                return "cheap"
            if price >= favorite_min:
                return "favorite"
            return "excluded"

        bands = {}
        total_pnl = 0.0
        total_n = 0
        total_wins = 0
        for row in rows:
            diff = row["twap60_now"] - row["twap60_open"]
            if diff == 0:
                continue
            side = "UP" if diff > 0 else "DOWN"
            price = row["polymarket_up_price"] if side == "UP" else row["polymarket_down_price"]
            if price is None or price <= 0 or price >= 1:
                continue
            band = band_of(price)
            if band == "excluded":
                continue
            if band == "favorite" and not trade_favorite_band:
                continue

            win = side == row["actual_outcome"]
            pnl = stake * ((1 - price) / price) * (1 - fee) if win else -stake

            b = bands.setdefault(band, {"n": 0, "wins": 0, "pnl": 0.0})
            b["n"] += 1
            b["pnl"] += pnl
            if win:
                b["wins"] += 1

            total_pnl += pnl
            total_n += 1
            if win:
                total_wins += 1

        for b in bands.values():
            b["win_rate"] = round(b["wins"] / b["n"], 3) if b["n"] else None
            b["pnl"] = round(b["pnl"], 2)
            b["avg_pnl_per_trade"] = round(b["pnl"] / b["n"], 3) if b["n"] else None

        return {
            "stake_per_trade": stake,
            "total_n": total_n,
            "total_wins": total_wins,
            "win_rate": round(total_wins / total_n, 3) if total_n else None,
            "total_simulated_pnl": round(total_pnl, 2),
            "avg_pnl_per_trade": round(total_pnl / total_n, 3) if total_n else None,
            "by_band": bands,
        }


_logger_instance = ShadowLogger()


def get_shadow_logger():
    return _logger_instance
