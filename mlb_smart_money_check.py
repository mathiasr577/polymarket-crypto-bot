"""
Cruza los partidos de MLB del día contra las dos señales nuevas de
wallet_copy_feed.py: wallets curadas (leaderboard real + activas) y
consenso general (>=3 wallets distintas, mismo lado, mismo mercado).

Uso: pensado para correr como parte del análisis diario de apuestas,
ANTES de decidir los picks -- si una wallet curada o un consenso real
ya tomó posición fuerte de un lado, es una señal adicional (no
reemplaza el criterio de precio+ERA+récord, se suma).

    railway ssh -- python3 mlb_smart_money_check.py "Yankees" "Red Sox" "Cubs"

Busca por substring de nombre de equipo (ILIKE) contra market_title, así
que alcanza con pasar el nombre del equipo de cada partido a chequear.
"""
import sys
import psycopg2
from config import DATABASE_URL


def check_team(cur, team_query: str):
    print(f"\n=== {team_query} ===")

    cur.execute("""
        SELECT proxy_wallet, pseudonym, market_title, outcome, wallet_price, wallet_size,
               exchange_ts_sec
        FROM wallet_copy_trades
        WHERE is_curated = TRUE AND market_title ILIKE %s
        ORDER BY exchange_ts_sec DESC
        LIMIT 15
    """, (f"%{team_query}%",))
    curated_rows = cur.fetchall()
    if curated_rows:
        print("  Wallets curadas (leaderboard real, activas) que tomaron posición:")
        for wallet, pseudo, title, outcome, price, size, ts in curated_rows:
            who = pseudo or wallet[:10]
            print(f"    {who:20s} -> {outcome:10s} @ {price:.3f}  (${float(price)*float(size):.0f})  | {title}")
    else:
        print("  Sin actividad de wallets curadas todavía.")

    cur.execute("""
        SELECT market_title, outcome, unique_traders, total_usd, price_at_event, crossed_at
        FROM market_consensus_events
        WHERE market_title ILIKE %s
        ORDER BY crossed_at DESC
        LIMIT 10
    """, (f"%{team_query}%",))
    consensus_rows = cur.fetchall()
    if consensus_rows:
        print("  Eventos de consenso (>=3 wallets, mismo lado):")
        for title, outcome, n, usd, price, ts in consensus_rows:
            print(f"    {outcome:10s} — {n} wallets, ~${float(usd):.0f} combinado, precio ${price}  | {ts}")
    else:
        print("  Sin consenso detectado todavía.")


def main():
    teams = sys.argv[1:]
    if not teams:
        print("Uso: python3 mlb_smart_money_check.py \"Equipo1\" \"Equipo2\" ...")
        return
    conn = psycopg2.connect(DATABASE_URL)
    with conn.cursor() as cur:
        for team in teams:
            check_team(cur, team)


if __name__ == "__main__":
    main()
