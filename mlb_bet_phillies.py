"""
Apuesta única del 10-sep (día) — Astros vs Phillies, $40.

Phillies (~63.5%): Wheeler (12-5, 3.23 ERA) vs Javier (2-5, 5.98 ERA),
equipo 82-64 vs 74-72. Las 3 señales alineadas — la mejor de la tanda
temprana de hoy (las otras dos, Rays/Braves y Rangers/Mariners, son
coinflip).

Uso:
    railway ssh -- python3 mlb_bet_phillies.py            # dry-run
    railway ssh -- python3 mlb_bet_phillies.py --live      # plata real
"""
import sys
from order_executor import place_order

STAKE_USD = 40.0
TOKEN_ID = "3959604305772021457992654145238432227509901737338161989435878139245813140930"  # Phillies
REF_PRICE = 0.635

live = "--live" in sys.argv
print(f"{'*** LIVE — plata real ***' if live else 'DRY-RUN — no se manda nada'}\n")
shares_est = round(STAKE_USD / REF_PRICE, 2)
print(f"-> Astros vs. Phillies (5:05PM ET) -> PHILLIES  ${STAKE_USD:.2f} @ ~{REF_PRICE:.3f}  (~{shares_est} shares)")

if live:
    resp = place_order(token_id=TOKEN_ID, price=round(REF_PRICE, 2), size=STAKE_USD, side="BUY")
    print(f"   respuesta: {resp}")
