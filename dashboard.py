from flask import Flask, jsonify, render_template_string
from datetime import datetime, timezone

app = Flask(__name__)

HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Polymarket 5min Bot</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: #0d0f14; color: #e2e8f0; font-family: 'Segoe UI', monospace; padding: 16px; }
  h1 { color: #7dd3fc; font-size: 1.3rem; margin-bottom: 12px; }
  h2 { color: #94a3b8; font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.1em; margin-bottom: 8px; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 10px; margin-bottom: 16px; }
  .grid-wide { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 10px; margin-bottom: 16px; }
  .card { background: #1e2330; border-radius: 8px; padding: 14px; }
  .card .label { font-size: 0.68rem; color: #64748b; margin-bottom: 3px; }
  .card .value { font-size: 1.3rem; font-weight: 700; }
  .card .sublabel { font-size: 0.62rem; color: #475569; margin-top: 4px; }
  .green { color: #4ade80; }
  .red { color: #f87171; }
  .yellow { color: #fbbf24; }
  .blue { color: #60a5fa; }
  .gray { color: #64748b; }
  .purple { color: #a78bfa; }
  table { width: 100%; border-collapse: collapse; font-size: 0.78rem; }
  th { text-align: left; padding: 5px 8px; color: #64748b; border-bottom: 1px solid #1e2330; }
  td { padding: 5px 8px; border-bottom: 1px solid #1a1f2e; }
  .badge { display: inline-block; padding: 2px 7px; border-radius: 4px; font-size: 0.68rem; font-weight: 700; }
  .up { background: #052e16; color: #4ade80; }
  .down { background: #1f0909; color: #f87171; }
  .high { background: #1c1406; color: #fbbf24; }
  .medium { background: #0f172a; color: #60a5fa; }
  .section { background: #1e2330; border-radius: 8px; padding: 14px; margin-bottom: 12px; }
  .mode { background: #7c3aed; color: white; border-radius: 5px; padding: 4px 12px; display: inline-block; font-size: 0.75rem; font-weight: 700; margin-bottom: 12px; }
  .footer { color: #334155; font-size: 0.65rem; margin-top: 12px; }
  .countdown { font-weight: 700; font-size: 1rem; }
  .win { background: #052e16; color: #4ade80; }
  .loss { background: #1f0909; color: #f87171; }
  .nofill { background: #1a1a2e; color: #818cf8; }
  .divider { border-top: 1px solid #1e2330; margin: 12px 0; }
  .stat-row { display: flex; justify-content: space-between; align-items: center; padding: 4px 0; font-size: 0.78rem; }
  .stat-row .stat-label { color: #64748b; }
  .stat-row .stat-val { font-weight: 700; }
  .progress-bar { background: #0d0f14; border-radius: 4px; height: 6px; margin-top: 6px; overflow: hidden; }
  .progress-fill { height: 100%; border-radius: 4px; transition: width 0.3s; }
  .refresh-note { color: #334155; font-size: 0.62rem; float: right; }
</style>
<script>
  setTimeout(() => location.reload(), 15000);
</script>
</head>
<body>
<h1>⚡ Polymarket 5min Bot <span class="refresh-note">auto-refresh 15s</span></h1>
<div class="mode">{{ mode }}</div>

<!-- FILA 1: Cash y métricas principales -->
<div class="grid-wide">
  <div class="card">
    <div class="label">💵 Cash en Polymarket</div>
    <div class="value blue">${{ "%.2f"|format(stats.cash_balance) }}</div>
    <div class="sublabel">disponible para tradear</div>
  </div>
  <div class="card">
    <div class="label">📈 P&L Total</div>
    <div class="value {{ 'green' if stats.pnl >= 0 else 'red' }}">${{ "%+.2f"|format(stats.pnl) }}</div>
    <div class="sublabel">ROI: {{ "%.1f"|format(stats.roi) }}%</div>
  </div>
  <div class="card">
    <div class="label">🎯 Win Rate</div>
    <div class="value {{ 'green' if stats.win_rate >= 65 else 'yellow' if stats.win_rate >= 55 else 'red' }}">{{ "%.1f"|format(stats.win_rate) }}%</div>
    <div class="progress-bar">
      <div class="progress-fill" style="width:{{ [stats.win_rate, 100]|min }}%;background:{{ '#4ade80' if stats.win_rate >= 65 else '#fbbf24' if stats.win_rate >= 55 else '#f87171' }};"></div>
    </div>
    <div class="sublabel">{{ stats.wins }}W / {{ stats.losses }}L de {{ stats.completed }} resueltos</div>
  </div>
  <div class="card">
    <div class="label">🔴 Posiciones abiertas</div>
    <div class="value yellow">{{ stats.open_count }}</div>
    <div class="sublabel">en vivo ahora</div>
  </div>
</div>

<!-- FILA 2: Métricas de ejecución -->
<div class="grid">
  <div class="card">
    <div class="label">✅ Trades ejecutados</div>
    <div class="value green">{{ stats.total_trades }}</div>
    <div class="sublabel">órdenes llenadas</div>
  </div>
  <div class="card">
    <div class="label">💧 Sin liquidez</div>
    <div class="value purple">{{ stats.no_fill_count }}</div>
    <div class="sublabel">intentos sin fill</div>
  </div>
  <div class="card">
    <div class="label">📊 Señales bloqueadas</div>
    <div class="value gray">{{ stats.blocked_count }}</div>
    <div class="sublabel">precio/delta fuera de rango</div>
  </div>
  <div class="card">
    <div class="label">💰 Mejor trade</div>
    <div class="value green">${{ "%+.2f"|format(stats.best_trade) }}</div>
  </div>
  <div class="card">
    <div class="label">📉 Peor trade</div>
    <div class="value red">${{ "%+.2f"|format(stats.worst_trade) }}</div>
  </div>
  <div class="card">
    <div class="label">🏦 Balance</div>
    <div class="value">${{ "%.2f"|format(stats.balance) }}</div>
    <div class="sublabel">{{ 'real' if stats.paper is defined else 'simulado (paper)' }}</div>
  </div>
</div>

{% if stats.paper is defined %}
<div class="section">
  <h2>📄 Paper trading (simulado — no es plata real)</h2>
  <div class="grid">
    <div class="card">
      <div class="label">Balance paper</div>
      <div class="value purple">${{ "%.2f"|format(stats.paper.balance) }}</div>
    </div>
    <div class="card">
      <div class="label">P&L paper</div>
      <div class="value {{ 'green' if stats.paper.pnl >= 0 else 'red' }}">${{ "%+.2f"|format(stats.paper.pnl) }}</div>
    </div>
    <div class="card">
      <div class="label">Win rate paper</div>
      <div class="value">{{ "%.1f"|format(stats.paper.win_rate) }}%</div>
      <div class="sublabel">{{ stats.paper.wins }}W / {{ stats.paper.total_trades - stats.paper.wins }}L</div>
    </div>
  </div>
</div>
{% endif %}

{% if stats.paper_v2 is defined %}
<div class="section">
  <h2>🧪 Paper trading v2 (TWAP + bandas de precio — simulado, en paralelo)</h2>
  <div class="grid">
    <div class="card">
      <div class="label">Balance v2</div>
      <div class="value purple">${{ "%.2f"|format(stats.paper_v2.balance) }}</div>
    </div>
    <div class="card">
      <div class="label">P&L v2</div>
      <div class="value {{ 'green' if stats.paper_v2.pnl >= 0 else 'red' }}">${{ "%+.2f"|format(stats.paper_v2.pnl) }}</div>
      <div class="sublabel">ROI: {{ "%.1f"|format(stats.paper_v2.roi) }}%</div>
    </div>
    <div class="card">
      <div class="label">Win rate v2</div>
      <div class="value">{{ "%.1f"|format(stats.paper_v2.win_rate) }}%</div>
      <div class="sublabel">{{ stats.paper_v2.wins }}W / {{ stats.paper_v2.total_trades - stats.paper_v2.wins }}L</div>
    </div>
    <div class="card">
      <div class="label">Posiciones abiertas v2</div>
      <div class="value yellow">{{ stats.paper_v2.open_count }}</div>
    </div>
  </div>
</div>
{% endif %}

{% if shadow_stats and shadow_stats.resolved %}
<div class="section">
  <h2>🔬 Shadow-mode: Chainlink TWAP vs. ganador real (fase 1)</h2>
  <div class="grid">
    <div class="card">
      <div class="label">Mercados resueltos</div>
      <div class="value">{{ shadow_stats.resolved }}</div>
      <div class="sublabel">{{ shadow_stats.resolved_clean }} sin huecos de datos</div>
    </div>
    <div class="card">
      <div class="label">TWAP 30s acierta ganador</div>
      <div class="value {{ 'green' if shadow_stats.resolved_clean and shadow_stats.twap30_correct / shadow_stats.resolved_clean >= 0.9 else 'yellow' }}">
        {{ "%.0f"|format(shadow_stats.twap30_correct / shadow_stats.resolved_clean * 100 if shadow_stats.resolved_clean else 0) }}%
      </div>
    </div>
    <div class="card">
      <div class="label">TWAP 60s acierta ganador</div>
      <div class="value {{ 'green' if shadow_stats.resolved_clean and shadow_stats.twap60_correct / shadow_stats.resolved_clean >= 0.9 else 'yellow' }}">
        {{ "%.0f"|format(shadow_stats.twap60_correct / shadow_stats.resolved_clean * 100 if shadow_stats.resolved_clean else 0) }}%
      </div>
    </div>
    <div class="card">
      <div class="label">Kraken spot acierta ganador</div>
      <div class="value">{{ "%.0f"|format(shadow_stats.kraken_correct / shadow_stats.resolved_clean * 100 if shadow_stats.resolved_clean else 0) }}%</div>
    </div>
    <div class="card">
      <div class="label">⚠️ Huecos de datos</div>
      <div class="value {{ 'red' if shadow_stats.data_gaps else 'gray' }}">{{ shadow_stats.data_gaps }}</div>
      <div class="sublabel">mercados descartados de la comparación</div>
    </div>
  </div>
</div>
{% endif %}

{% if shadow_backtest and shadow_backtest.n %}
<div class="section">
  <h2>🥊 Backtest: modelo viejo vs. señal TWAP, mismo momento de decisión</h2>
  <div class="sublabel" style="margin-bottom:10px;">Comparación justa (misma población de mercados, no solo los que el modelo viejo eligió tradear):</div>
  <div class="grid">
    <div class="card">
      <div class="label">Inclinación cruda del modelo viejo</div>
      <div class="value">{{ "%.0f"|format(shadow_backtest.old_model_raw_correct / shadow_backtest.n_old_model_raw * 100 if shadow_backtest.n_old_model_raw else 0) }}%</div>
      <div class="sublabel">{{ shadow_backtest.n_old_model_raw }} mercados (incluye bloqueados)</div>
    </div>
    <div class="card">
      <div class="label">Delta TWAP 30s (open→now)</div>
      <div class="value {{ 'green' if shadow_backtest.n_twap30 and shadow_backtest.n_old_model_raw and shadow_backtest.twap30_correct / shadow_backtest.n_twap30 > shadow_backtest.old_model_raw_correct / shadow_backtest.n_old_model_raw else '' }}">
        {{ "%.0f"|format(shadow_backtest.twap30_correct / shadow_backtest.n_twap30 * 100 if shadow_backtest.n_twap30 else 0) }}%
      </div>
    </div>
    <div class="card">
      <div class="label">Delta TWAP 60s (open→now)</div>
      <div class="value {{ 'green' if shadow_backtest.n_twap60 and shadow_backtest.n_old_model_raw and shadow_backtest.twap60_correct / shadow_backtest.n_twap60 > shadow_backtest.old_model_raw_correct / shadow_backtest.n_old_model_raw else '' }}">
        {{ "%.0f"|format(shadow_backtest.twap60_correct / shadow_backtest.n_twap60 * 100 if shadow_backtest.n_twap60 else 0) }}%
      </div>
    </div>
    <div class="card">
      <div class="label">Delta Kraken spot crudo (sin drift/dampener)</div>
      <div class="value">{{ "%.0f"|format(shadow_backtest.kraken_raw_correct / shadow_backtest.n_kraken_raw * 100 if shadow_backtest.n_kraken_raw else 0) }}%</div>
    </div>
  </div>
  <div class="divider"></div>
  <div class="stat-row">
    <span class="stat-label">Filtro estricto del modelo viejo (solo cuando decide tradear, precio+edge OK)</span>
    <span class="stat-val">{{ "%.0f"|format(shadow_backtest.old_model_filtered_correct / shadow_backtest.n_old_model_filtered * 100 if shadow_backtest.n_old_model_filtered else 0) }}% ({{ shadow_backtest.n_old_model_filtered }} casos)</span>
  </div>
  <div class="divider"></div>
  <div class="stat-row">
    <span class="stat-label">Cuando TWAP60 y la inclinación cruda del modelo viejo NO coinciden ({{ shadow_backtest.disagree_n }} casos):</span>
  </div>
  <div class="stat-row">
    <span class="stat-label">→ acertó TWAP60</span>
    <span class="stat-val yellow">{{ "%.0f"|format(shadow_backtest.disagree_twap60_right / shadow_backtest.disagree_n * 100 if shadow_backtest.disagree_n else 0) }}%</span>
  </div>
  <div class="stat-row">
    <span class="stat-label">→ acertó el modelo viejo</span>
    <span class="stat-val yellow">{{ "%.0f"|format(shadow_backtest.disagree_old_right / shadow_backtest.disagree_n * 100 if shadow_backtest.disagree_n else 0) }}%</span>
  </div>
</div>
{% endif %}

{% if shadow_calibration and shadow_calibration.bands %}
<div class="section">
  <h2>💸 ¿Dónde está la fuga? Acierto real vs. breakeven, por banda de precio</h2>
  <div class="sublabel" style="margin-bottom:8px;">Con 7% de fee, el breakeven sube rápido con el precio. Cada modelo se agrupa por SU PROPIO precio (lado que él mismo eligió) — no son la misma población de mercados en cada fila.</div>
  <table>
    <tr>
      <th>Banda</th>
      <th>Modelo viejo: precio/breakeven</th><th>n</th><th>acierto real</th><th>p. teórica</th><th>¿gana?</th>
      <th>TWAP60: precio/breakeven</th><th>n</th><th>acierto real</th><th>¿gana?</th>
    </tr>
    {% for b in shadow_calibration.bands %}
    <tr>
      <td>{{ b.band }}</td>
      <td>{{ "%.2f"|format(b.old_model_avg_price) if b.old_model_avg_price else '—' }} / {{ "%.0f"|format(b.old_model_breakeven_needed * 100) if b.old_model_breakeven_needed else 0 }}%</td>
      <td>{{ b.old_model_n }}</td>
      <td class="{{ 'green' if b.old_model_beats_breakeven else 'red' if b.old_model_beats_breakeven == false else 'gray' }}">
        {{ "%.0f"|format(b.old_model_win_rate * 100) if b.old_model_win_rate is not none else '—' }}%
      </td>
      <td class="gray">{{ "%.0f"|format(b.old_model_avg_theoretical_p * 100) if b.old_model_avg_theoretical_p else '—' }}%</td>
      <td>{{ '✅' if b.old_model_beats_breakeven else ('❌' if b.old_model_beats_breakeven == false else '—') }}</td>
      <td>{{ "%.2f"|format(b.twap60_avg_price) if b.twap60_avg_price else '—' }} / {{ "%.0f"|format(b.twap60_breakeven_needed * 100) if b.twap60_breakeven_needed else 0 }}%</td>
      <td>{{ b.twap60_n }}</td>
      <td class="{{ 'green' if b.twap60_beats_breakeven else 'red' if b.twap60_beats_breakeven == false else 'gray' }}">
        {{ "%.0f"|format(b.twap60_win_rate * 100) if b.twap60_win_rate is not none else '—' }}%
      </td>
      <td>{{ '✅' if b.twap60_beats_breakeven else ('❌' if b.twap60_beats_breakeven == false else '—') }}</td>
    </tr>
    {% endfor %}
  </table>
</div>
{% endif %}

{% if prices %}
<div class="section">
  <h2>💹 Precios en vivo</h2>
  <table>
    <tr>
      <th>Asset</th><th>Precio</th><th>2min %</th>
      <th>Momentum</th><th>RSI(6)</th><th>Volatilidad</th>
    </tr>
    {% for asset, ind in prices.items() %}
    {% if ind %}
    <tr>
      <td><b>{{ asset.upper() }}</b></td>
      <td>${{ "{:,.2f}".format(ind.price) }}</td>
      <td class="{{ 'green' if ind.pct_2min > 0 else 'red' }}">
        {{ "%+.3f"|format(ind.pct_2min * 100) }}%
      </td>
      <td class="{{ 'green' if ind.momentum == 'up' else 'red' if ind.momentum == 'down' else 'gray' }}">
        {{ ind.momentum }}
      </td>
      <td class="{{ 'red' if ind.rsi > 70 else 'green' if ind.rsi < 30 else '' }}">
        {{ "%.1f"|format(ind.rsi) }}
      </td>
      <td>{{ "%.3f"|format(ind.volatility * 100) }}%</td>
    </tr>
    {% endif %}
    {% endfor %}
  </table>
</div>
{% endif %}

{% if markets %}
<div class="section">
  <h2>🎯 Mercados activos</h2>
  <table>
    <tr><th>Mercado</th><th>UP</th><th>DOWN</th><th>Tiempo</th></tr>
    {% for m in markets %}
    <tr>
      <td>{{ m.title[:35] if m.title else m.slug }}</td>
      <td class="{{ 'green' if m.up_price < 0.45 else 'gray' }}">{{ "%.2f"|format(m.up_price) }}¢</td>
      <td class="{{ 'green' if m.down_price < 0.45 else 'gray' }}">{{ "%.2f"|format(m.down_price) }}¢</td>
      <td class="countdown {{ 'red' if m.seconds_left < 60 else 'yellow' if m.seconds_left < 120 else 'green' }}">
        {{ "%.0f"|format(m.seconds_left) }}s
      </td>
    </tr>
    {% endfor %}
  </table>
</div>
{% endif %}

{% if stats.open_positions %}
<div class="section">
  <h2>🔴 Posiciones abiertas</h2>
  <table>
    <tr><th>Asset</th><th>Lado</th><th>Size</th><th>Entrada</th><th>Confianza</th></tr>
    {% for p in stats.open_positions %}
    <tr>
      <td>{{ p.asset.upper() if p.asset else '?' }}</td>
      <td><span class="badge {{ 'up' if p.side == 'UP' else 'down' }}">{{ p.side }}</span></td>
      <td>${{ "%.2f"|format(p.size) }}</td>
      <td>{{ "%.2f"|format(p.price) }}¢</td>
      <td><span class="badge {{ 'high' if p.confidence == 'HIGH' else 'medium' }}">{{ p.confidence }}</span></td>
    </tr>
    {% endfor %}
  </table>
</div>
{% endif %}

{% if stats.recent_no_fills %}
<div class="section">
  <h2>💧 Últimos intentos sin liquidez</h2>
  <table>
    <tr><th>Asset</th><th>Lado</th><th>Precio intentado</th><th>Hora</th></tr>
    {% for t in stats.recent_no_fills %}
    <tr>
      <td>{{ t.asset.upper() }}</td>
      <td><span class="badge {{ 'up' if t.side == 'UP' else 'down' }}">{{ t.side }}</span></td>
      <td>{{ "%.2f"|format(t.price) }}¢</td>
      <td style="color:#64748b;">{{ t.time }}</td>
    </tr>
    {% endfor %}
  </table>
</div>
{% endif %}

{% if stats.by_asset %}
<div class="section">
  <h2>📊 Por asset</h2>
  <table>
    <tr><th>Asset</th><th>Trades</th><th>Wins</th><th>Win Rate</th></tr>
    {% for asset, row in stats.by_asset.items() %}
    <tr>
      <td>{{ asset.upper() }}</td>
      <td>{{ row.total }}</td>
      <td>{{ row.wins }}</td>
      <td class="{{ 'green' if row.wins / row.total >= 0.55 else 'red' if row.total else '' }}">
        {{ "%.1f"|format(row.wins / row.total * 100 if row.total else 0) }}%
      </td>
    </tr>
    {% endfor %}
  </table>
</div>
{% endif %}

{% if stats.recent_trades %}
<div class="section">
  <h2>📋 Últimos trades</h2>
  <table>
    <tr><th>Asset</th><th>Lado</th><th>Size</th><th>Entrada</th><th>Resultado</th><th>P&L</th></tr>
    {% for t in stats.recent_trades %}
    <tr>
      <td>{{ t.asset.upper() if t.asset else '?' }}</td>
      <td><span class="badge {{ 'up' if t.side == 'UP' else 'down' }}">{{ t.side }}</span></td>
      <td>${{ "%.2f"|format(t.size) }}</td>
      <td>{{ "%.2f"|format(t.price) }}¢</td>
      <td>{{ t.outcome or '?' }}</td>
      <td class="{{ 'green' if t.pnl and t.pnl > 0 else 'red' }}">
        ${{ "%+.2f"|format(t.pnl or 0) }}
        <span class="badge {{ 'win' if t.win else 'loss' }}">{{ 'WIN' if t.win else 'LOSS' }}</span>
      </td>
    </tr>
    {% endfor %}
  </table>
</div>
{% endif %}

<div class="footer">{{ now }} &nbsp;|&nbsp; Auto-refresh cada 15s</div>
</body>
</html>
"""

def create_dashboard(get_stats_fn, get_prices_fn, get_markets_fn=None, mode="PAPER TRADING", get_shadow_stats_fn=None, get_shadow_backtest_fn=None, get_shadow_calibration_fn=None, get_arb_stats_fn=None, get_shadow_calib_curve_fn=None, get_shadow_weekday_fn=None, get_shadow_lead_fn=None, get_shadow_v2_sim_fn=None, get_shadow_model_comparison_fn=None, get_shadow_magnitude_fn=None, get_shadow_filtered_sim_fn=None, get_shadow_favorite_detail_fn=None, get_shadow_hourly_fn=None, get_shadow_favorite_ext_fn=None, get_shadow_v2_weekday_fn=None):
    @app.route("/")
    def index():
        stats = get_stats_fn()
        prices = get_prices_fn()
        markets = get_markets_fn() if get_markets_fn else []
        shadow_stats = get_shadow_stats_fn() if get_shadow_stats_fn else None
        shadow_backtest = get_shadow_backtest_fn() if get_shadow_backtest_fn else None
        shadow_calibration = get_shadow_calibration_fn() if get_shadow_calibration_fn else None

        # Defaults para nuevas métricas
        stats.setdefault("cash_balance", 0.0)
        stats.setdefault("no_fill_count", 0)
        stats.setdefault("blocked_count", 0)
        stats.setdefault("recent_no_fills", [])
        stats.setdefault("wins", 0)
        stats.setdefault("losses", 0)

        # Convert datetime objects
        open_pos = []
        for p in stats.get("open_positions", []):
            p2 = dict(p)
            for k, v in p2.items():
                if hasattr(v, 'isoformat'):
                    p2[k] = str(v)
            open_pos.append(p2)
        stats["open_positions"] = open_pos

        recent = []
        for t in stats.get("recent_trades", []):
            t2 = dict(t)
            for k, v in t2.items():
                if hasattr(v, 'isoformat'):
                    t2[k] = str(v)
            recent.append(t2)
        stats["recent_trades"] = recent

        # stats["paper"] / stats["paper_v2"], si existen, siguen siendo
        # dicts planos — convertirlos también a objeto para que
        # stats.paper.balance funcione en el template.
        if "paper" in stats:
            stats["paper"] = type("P", (), dict(stats["paper"]))()
        if "paper_v2" in stats:
            stats["paper_v2"] = type("P2", (), dict(stats["paper_v2"]))()

        return render_template_string(
            HTML,
            stats=type("S", (), {k: v for k, v in stats.items()})(),
            prices={k: type("I", (), v)() if v else None for k, v in prices.items()},
            markets=markets,
            mode=mode,
            shadow_stats=shadow_stats,
            shadow_backtest=shadow_backtest,
            shadow_calibration=shadow_calibration,
            now=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        )

    @app.route("/api/stats")
    def api_stats():
        return jsonify(get_stats_fn())

    @app.route("/api/shadow-stats")
    def api_shadow_stats():
        return jsonify(get_shadow_stats_fn() if get_shadow_stats_fn else {})

    @app.route("/api/shadow-backtest")
    def api_shadow_backtest():
        return jsonify(get_shadow_backtest_fn() if get_shadow_backtest_fn else {})

    @app.route("/api/shadow-calibration")
    def api_shadow_calibration():
        return jsonify(get_shadow_calibration_fn() if get_shadow_calibration_fn else {})

    @app.route("/api/arb-stats")
    def api_arb_stats():
        return jsonify(get_arb_stats_fn() if get_arb_stats_fn else {})

    @app.route("/api/shadow-calibration-curve")
    def api_shadow_calib_curve():
        return jsonify(get_shadow_calib_curve_fn() if get_shadow_calib_curve_fn else {})

    @app.route("/api/shadow-weekday")
    def api_shadow_weekday():
        return jsonify(get_shadow_weekday_fn() if get_shadow_weekday_fn else {})

    @app.route("/api/shadow-lead")
    def api_shadow_lead():
        return jsonify(get_shadow_lead_fn() if get_shadow_lead_fn else {})

    @app.route("/api/shadow-v2-sim")
    def api_shadow_v2_sim():
        return jsonify(get_shadow_v2_sim_fn() if get_shadow_v2_sim_fn else {})

    @app.route("/api/shadow-model-comparison")
    def api_shadow_model_comparison():
        return jsonify(get_shadow_model_comparison_fn() if get_shadow_model_comparison_fn else {})

    @app.route("/api/shadow-magnitude")
    def api_shadow_magnitude():
        return jsonify(get_shadow_magnitude_fn() if get_shadow_magnitude_fn else {})

    @app.route("/api/shadow-filtered-sim")
    def api_shadow_filtered_sim():
        return jsonify(get_shadow_filtered_sim_fn() if get_shadow_filtered_sim_fn else {})

    @app.route("/api/shadow-favorite-detail")
    def api_shadow_favorite_detail():
        return jsonify(get_shadow_favorite_detail_fn() if get_shadow_favorite_detail_fn else {})

    @app.route("/api/shadow-hourly")
    def api_shadow_hourly():
        return jsonify(get_shadow_hourly_fn() if get_shadow_hourly_fn else {})

    @app.route("/api/shadow-favorite-extension")
    def api_shadow_favorite_ext():
        return jsonify(get_shadow_favorite_ext_fn() if get_shadow_favorite_ext_fn else {})

    @app.route("/api/shadow-v2-weekday")
    def api_shadow_v2_weekday():
        return jsonify(get_shadow_v2_weekday_fn() if get_shadow_v2_weekday_fn else {})

    @app.route("/health")
    def health():
        return "OK"

    @app.route("/api/reset-positions", methods=["POST"])
    def reset_positions():
        from paper_trader import get_trader
        trader = get_trader()
        closed = []
        for market_id in list(trader.open_positions.keys()):
            pos = trader.open_positions[market_id]
            side = pos["side"]
            loser = "DOWN" if side == "UP" else "UP"
            trader.resolve_trade(market_id, loser)
            closed.append(market_id)
        return jsonify({"reset": len(closed), "markets": closed})

    return app