"""
Reintento de UNA sola apuesta — Mets vs Marlins, $45, que falló antes
por "not enough balance" (probablemente un delay de settlement entre
las 2 órdenes anteriores y esta, no falta de plata real: el portfolio
ya mostraba $84.32 disponibles, más que los ~$47.81 que pedía la
orden). No toca Yankees ni Brewers, esas ya están confirmadas.

Uso:
    railway ssh -- python3 mlb_retry_marlins.py            # dry-run
    railway ssh -- python3 mlb_retry_marlins.py --live      # plata real
"""
import sys
from order_executor import place_order

STAKE_USD = 45.0
TOKEN_ID = "100402930733517859553033568487421284989702898538143745066977613006030392716081"  # Marlins
REF_PRICE = 0.515  # precio actual, re-chequeado antes de reintentar

live = "--live" in sys.argv
print(f"{'*** LIVE — plata real ***' if live else 'DRY-RUN — no se manda nada'}\n")
shares_est = round(STAKE_USD / REF_PRICE, 2)
print(f"-> Mets vs. Marlins -> MARLINS  ${STAKE_USD:.2f} @ ~{REF_PRICE:.3f}  (~{shares_est} shares)")

if live:
    resp = place_order(token_id=TOKEN_ID, price=round(REF_PRICE, 2), size=STAKE_USD, side="BUY")
    print(f"   respuesta: {resp}")
