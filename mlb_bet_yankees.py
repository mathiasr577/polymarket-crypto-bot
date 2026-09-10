"""
Apuesta única del 10-sep (noche) — Rockies vs Yankees, $50.

Yankees (~71.5%): Fried (4-4, 2.73 ERA) vs Feltner (5-9, 5.91 ERA),
equipo 83-62 vs 55-90. Las 3 señales alineadas, gap grande. Única que
vale de la tanda de esta noche.

Uso:
    railway ssh -- python3 mlb_bet_yankees.py            # dry-run
    railway ssh -- python3 mlb_bet_yankees.py --live      # plata real
"""
import sys
from order_executor import place_order

STAKE_USD = 50.0
TOKEN_ID = "90751690458114107337713649408403216457927076574647586480293403156551181026722"  # Yankees
REF_PRICE = 0.715

live = "--live" in sys.argv
print(f"{'*** LIVE — plata real ***' if live else 'DRY-RUN — no se manda nada'}\n")
shares_est = round(STAKE_USD / REF_PRICE, 2)
print(f"-> Rockies vs. Yankees -> YANKEES  ${STAKE_USD:.2f} @ ~{REF_PRICE:.3f}  (~{shares_est} shares)")

if live:
    resp = place_order(token_id=TOKEN_ID, price=round(REF_PRICE, 2), size=STAKE_USD, side="BUY")
    print(f"   respuesta: {resp}")
