"""
Cuarta apuesta del 9-sep — Reds vs Dodgers, $40. Separado del script
principal (mlb_manual_bets.py) que ya se corrió con las otras 3.

Dodgers (~73%): Yamamoto (12-8, 2.67 ERA) vs Lowder (6-9, 5.43 ERA),
equipo 88-57 vs 69-76. Las tres señales alineadas con margen grande.

Uso:
    railway ssh -- python3 mlb_bet_dodgers.py            # dry-run
    railway ssh -- python3 mlb_bet_dodgers.py --live      # plata real
"""
import sys
from order_executor import place_order

STAKE_USD = 40.0
TOKEN_ID = "59854477402465148240722923381953583254739823925018057435830263413172444934599"  # Dodgers
REF_PRICE = 0.725

live = "--live" in sys.argv
print(f"{'*** LIVE — plata real ***' if live else 'DRY-RUN — no se manda nada'}\n")
shares_est = round(STAKE_USD / REF_PRICE, 2)
print(f"-> Reds vs. Dodgers (2:10AM ET) -> DODGERS  ${STAKE_USD:.2f} @ ~{REF_PRICE:.3f}  (~{shares_est} shares)")

if live:
    resp = place_order(token_id=TOKEN_ID, price=round(REF_PRICE, 2), size=STAKE_USD, side="BUY")
    print(f"   respuesta: {resp}")
