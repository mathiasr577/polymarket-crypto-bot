from flask import Flask, jsonify, render_template_string
from datetime import datetime, timezone

app = Flask(__name__)

HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Polymarket Crypto Bot</title>

<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: #0d0f14; color: #e2e8f0; font-family: 'Segoe UI', monospace; padding: 20px; }
  h1 { color: #7dd3fc; font-size: 1.4rem; margin-bottom: 16px; }
  h2 { color: #94a3b8; font-size: 0.9rem; text-transform: uppercase; letter-spacing: 0.1em; margin-bottom: 10px; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; margin-bottom: 20px; }
  .card { background: #1e2330; border-radius: 10px; padding: 16px; }
  .card .label { font-size: 0.7rem; color: #64748b; margin-bottom: 4px; }
  .card .value { font-size: 1.4rem; font-weight: 700; }
  .green { color: #4ade80; }
  .red { color: #f87171; }
  .yellow { color: #fbbf24; }
  .blue { color: #60a5fa; }
  table { width: 100%; border-collapse: collapse; font-size: 0.8rem; }
  th { text-align: left; padding: 6px 8px; color: #64748b; border-bottom: 1px solid #1e2330; }
  td { padding: 6px 8px; border-bottom: 1px solid #1a1f2e; }
  .badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 0.7rem; font-weight: 600; }
  .badge-green { background: #052e16; color: #4ade80; }
  .badge-red { background: #1f0909; color: #f87171; }
  .badge-yellow { background: #1c1406; color: #fbbf24; }
  .section { background: #1e2330; border-radius: 10px; padding: 16px; margin-bottom: 16px; }
  .mode-bar { background: #7c3aed; color: white; border-radius: 6px; padding: 6px 14px; display: inline-block; font-size: 0.8rem; font-weight: 600; margin-bottom: 16px; }
  .footer { color: #334155; font-size: 0.7rem; margin-top: 16px; }
</style>
</head>
<body>
<h1>🤖 Polymarket Crypto Bot</h1>
<div class="mode-bar">{{ mode }}</div>

<div class="grid">
  <div class="card">
    <div class="label">Balance</div>
    <div class="value blue">${{ "%.2f"|format(stats.balance) }}</div>
  </div>
  <div class="card">
    <div class="label">PnL</div>
    <div class="value {{ 'green' if stats.pnl >= 0 else 'red' }}">${{ "%+.2f"|format(stats.pnl) }}</div>
  </div>
  <div class="card">
    <div class="label">ROI</div>
    <div class="value {{ 'green' if stats.roi >= 0 else 'red' }}">{{ "%.1f"|format(stats.roi) }}%</div>
  </div>
  <div class="card">
    <div class="label">Win Rate</div>
    <div class="value {{ 'green' if stats.win_rate >= 55 else 'yellow' }}">{{ "%.1f"|format(stats.win_rate) }}%</div>
  </div>
  <div class="card">
    <div class="label">Total Trades</div>
    <div class="value">{{ stats.total_trades }}</div>
  </div>
  <div class="card">
    <div class="label">Open Positions</div>
    <div class="value yellow">{{ stats.open_count }}</div>
  </div>
  <div class="card">
    <div class="label">Best Trade</div>
    <div class="value green">${{ "%+.2f"|format(stats.best_trade) }}</div>
  </div>
  <div class="card">
    <div class="label">Worst Trade</div>
    <div class="value red">${{ "%+.2f"|format(stats.worst_trade) }}</div>
  </div>
</div>

{% if stats.open_positions %}
<div class="section">
  <h2>🔴 Live Positions</h2>
  <table>
    <tr><th>Asset</th><th>Side</th><th>Size</th><th>Confidence</th><th>Opened</th><th>Signals</th></tr>
    {% for p in stats.open_positions %}
    <tr>
      <td>{{ p.asset.upper() if p.asset else '?' }}</td>
      <td><span class="badge {{ 'badge-green' if p.side == 'UP' else 'badge-red' }}">{{ p.side }}</span></td>
      <td>${{ "%.2f"|format(p.size) }}</td>
      <td><span class="badge {{ 'badge-yellow' if p.confidence == 'HIGH' else '' }}">{{ p.confidence }}</span></td>
      <td>{{ p.opened_at|string|truncate(19, True, '') if p.opened_at else '?' }}</td>
      <td style="font-size:0.7rem;color:#64748b;">{{ p.reasons[:80] if p.reasons else '' }}</td>
    </tr>
    {% endfor %}
  </table>
</div>
{% endif %}

{% if stats.by_asset %}
<div class="section">
  <h2>📊 By Asset</h2>
  <table>
    <tr><th>Asset</th><th>Trades</th><th>Wins</th><th>Win Rate</th></tr>
    {% for asset, row in stats.by_asset.items() %}
    <tr>
      <td>{{ asset.upper() }}</td>
      <td>{{ row.total }}</td>
      <td>{{ row.wins }}</td>
      <td>{{ "%.1f"|format(row.wins / row.total * 100 if row.total else 0) }}%</td>
    </tr>
    {% endfor %}
  </table>
</div>
{% endif %}

{% if prices %}
<div class="section">
  <h2>💹 Live Prices</h2>
  <table>
    <tr><th>Asset</th><th>Price</th><th>RSI</th><th>EMA9</th><th>EMA21</th><th>Momentum</th><th>Volatility</th></tr>
    {% for asset, ind in prices.items() %}
    {% if ind %}
    <tr>
      <td>{{ asset.upper() }}</td>
      <td>${{ "%.2f"|format(ind.price) }}</td>
      <td class="{{ 'red' if ind.rsi > 70 else ('green' if ind.rsi < 30 else '') }}">{{ "%.1f"|format(ind.rsi) }}</td>
      <td>{{ "%.2f"|format(ind.ema9) if ind.ema9 else '—' }}</td>
      <td>{{ "%.2f"|format(ind.ema21) if ind.ema21 else '—' }}</td>
      <td>{{ ind.momentum }}</td>
      <td>{{ "%.3f"|format(ind.volatility * 100) }}%</td>
    </tr>
    {% endif %}
    {% endfor %}
  </table>
</div>
{% endif %}

{% if stats.recent_trades %}
<div class="section">
  <h2>📋 Recent Trades</h2>
  <table>
    <tr><th>Asset</th><th>Side</th><th>Size</th><th>Outcome</th><th>PnL</th><th>W/L</th><th>Closed</th></tr>
    {% for t in stats.recent_trades %}
    <tr>
      <td>{{ t.asset.upper() if t.asset else '?' }}</td>
      <td><span class="badge {{ 'badge-green' if t.side == 'UP' else 'badge-red' }}">{{ t.side }}</span></td>
      <td>${{ "%.2f"|format(t.size) }}</td>
      <td>{{ t.outcome or '?' }}</td>
      <td class="{{ 'green' if t.pnl and t.pnl > 0 else 'red' }}">${{ "%+.2f"|format(t.pnl or 0) }}</td>
      <td><span class="badge {{ 'badge-green' if t.win else 'badge-red' }}">{{ 'WIN' if t.win else 'LOSS' }}</span></td>
      <td style="font-size:0.7rem;">{{ t.resolved_at|string|truncate(16, True, '') if t.resolved_at else '—' }}</td>
    </tr>
    {% endfor %}
  </table>
</div>
{% endif %}

<div class="footer">{{ now }}</div>
</body>
</html>
"""

def create_dashboard(get_stats_fn, get_prices_fn, mode="PAPER TRADING"):
    @app.route("/")
    def index():
        stats = get_stats_fn()
        prices = get_prices_fn()
        return render_template_string(
            HTML,
            stats=type("S", (), {k: v for k, v in stats.items()})(),
            prices=prices,
            mode=mode,
            now=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        )

    @app.route("/api/stats")
    def api_stats():
        return jsonify(get_stats_fn())

    @app.route("/health")
    def health():
        return "OK"

    @app.route("/api/reset-positions", methods=["POST"])
    def reset_positions():
        """Emergency: close all open positions as losses."""
        from paper_trader import get_trader
        trader = get_trader()
        closed = []
        for market_id in list(trader.open_positions.keys()):
            pos = trader.open_positions[market_id]
            side = pos["side"]
            loser = "NO" if side == "YES" else "YES"
            trader.resolve_trade(market_id, loser)
            closed.append(market_id)
        return jsonify({"reset": len(closed), "markets": closed})

    return app