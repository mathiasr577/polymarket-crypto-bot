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
  .card { background: #1e2330; border-radius: 8px; padding: 14px; }
  .card .label { font-size: 0.68rem; color: #64748b; margin-bottom: 3px; }
  .card .value { font-size: 1.3rem; font-weight: 700; }
  .green { color: #4ade80; }
  .red { color: #f87171; }
  .yellow { color: #fbbf24; }
  .blue { color: #60a5fa; }
  .gray { color: #64748b; }
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
  .flow-bar { display: inline-block; height: 8px; border-radius: 4px; vertical-align: middle; }
  .footer { color: #334155; font-size: 0.65rem; margin-top: 12px; }
  .countdown { font-weight: 700; font-size: 1rem; }
  .win { background: #052e16; color: #4ade80; }
  .loss { background: #1f0909; color: #f87171; }
</style>
</head>
<body>
<h1>⚡ Polymarket 5min Bot</h1>
<div class="mode">{{ mode }}</div>

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
    <div class="value {{ 'green' if stats.win_rate >= 55 else 'yellow' if stats.win_rate >= 50 else 'red' }}">{{ "%.1f"|format(stats.win_rate) }}%</div>
  </div>
  <div class="card">
    <div class="label">Total Trades</div>
    <div class="value">{{ stats.total_trades }}</div>
  </div>
  <div class="card">
    <div class="label">Open</div>
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

{% if prices %}
<div class="section">
  <h2>💹 Live Market Data</h2>
  <table>
    <tr>
      <th>Asset</th><th>Price</th><th>2min %</th>
      <th>Buy Flow</th><th>Momentum</th><th>RSI(6)</th><th>Volatility</th>
    </tr>
    {% for asset, ind in prices.items() %}
    {% if ind %}
    <tr>
      <td><b>{{ asset.upper() }}</b></td>
      <td>${{ "{:,.2f}".format(ind.price) }}</td>
      <td class="{{ 'green' if ind.pct_2min > 0 else 'red' }}">
        {{ "%+.3f"|format(ind.pct_2min * 100) }}%
      </td>
      <td style="color:#64748b">—</td>
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
  <h2>🎯 Active Markets</h2>
  <table>
    <tr><th>Market</th><th>UP price</th><th>DOWN price</th><th>Time left</th></tr>
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
  <h2>🔴 Open Positions</h2>
  <table>
    <tr><th>Asset</th><th>Side</th><th>Size</th><th>Entry</th><th>Confidence</th><th>Signals</th></tr>
    {% for p in stats.open_positions %}
    <tr>
      <td>{{ p.asset.upper() if p.asset else '?' }}</td>
      <td><span class="badge {{ 'up' if p.side == 'UP' else 'down' }}">{{ p.side }}</span></td>
      <td>${{ "%.2f"|format(p.size) }}</td>
      <td>{{ "%.2f"|format(p.price) }}¢</td>
      <td><span class="badge {{ 'high' if p.confidence == 'HIGH' else 'medium' }}">{{ p.confidence }}</span></td>
      <td style="font-size:0.68rem;color:#64748b;">{{ p.reasons[:60] if p.reasons else '' }}</td>
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
  <h2>📋 Recent Trades</h2>
  <table>
    <tr><th>Asset</th><th>Side</th><th>Size</th><th>Entry</th><th>Outcome</th><th>PnL</th><th>W/L</th></tr>
    {% for t in stats.recent_trades %}
    <tr>
      <td>{{ t.asset.upper() if t.asset else '?' }}</td>
      <td><span class="badge {{ 'up' if t.side == 'UP' else 'down' }}">{{ t.side }}</span></td>
      <td>${{ "%.2f"|format(t.size) }}</td>
      <td>{{ "%.2f"|format(t.price) }}¢</td>
      <td>{{ t.outcome or '?' }}</td>
      <td class="{{ 'green' if t.pnl and t.pnl > 0 else 'red' }}">${{ "%+.2f"|format(t.pnl or 0) }}</td>
      <td><span class="badge {{ 'win' if t.win else 'loss' }}">{{ 'WIN' if t.win else 'LOSS' }}</span></td>
    </tr>
    {% endfor %}
  </table>
</div>
{% endif %}

<div class="footer">{{ now }}</div>
</body>
</html>
"""

def create_dashboard(get_stats_fn, get_prices_fn, get_markets_fn=None, mode="PAPER TRADING"):
    @app.route("/")
    def index():
        stats = get_stats_fn()
        prices = get_prices_fn()
        markets = get_markets_fn() if get_markets_fn else []

        # Convert datetime objects in open_positions to strings
        open_pos = []
        for p in stats.get("open_positions", []):
            p2 = dict(p)
            for k, v in p2.items():
                if hasattr(v, 'isoformat'):
                    p2[k] = str(v)
            open_pos.append(p2)
        stats["open_positions"] = open_pos

        # Convert recent_trades datetimes
        recent = []
        for t in stats.get("recent_trades", []):
            t2 = dict(t)
            for k, v in t2.items():
                if hasattr(v, 'isoformat'):
                    t2[k] = str(v)
            recent.append(t2)
        stats["recent_trades"] = recent

        return render_template_string(
            HTML,
            stats=type("S", (), {k: v for k, v in stats.items()})(),
            prices={k: type("I", (), v)() if v else None for k, v in prices.items()},
            markets=markets,
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