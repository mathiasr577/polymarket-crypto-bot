"""
Compra manual de 3 mercados de MLB — 12-sep-2026, $40 c/u.

Mismo criterio de siempre: precio de mercado + récord de equipo +
abridor confirmado, eligiendo los 3 donde las tres señales apuntan en
la misma dirección con el mayor margen.

  1. Dodgers (62.5%) vs Marlins
     Glasnow (4-0, 3.12 ERA) vs Phillips (5-6, 3.51 ERA)
     Equipo: LAD 90-57 (mejor récord de las mayores) vs MIA 72-76

  2. Brewers (62.5%) vs Reds
     Harrison (10-4, 3.66 ERA) vs Singer (6-13, 5.03 ERA)
     Equipo: MIL 92-56 (el mejor récord de toda la liga) vs CIN 69-78

  3. Mariners (61.5%) vs Athletics
     Woo (11-9, 4.03 ERA) vs Jump (6-10, 5.12 ERA)
     Equipo: SEA 69-79 vs OAK 60-88

Descartados a propósito por señal mixta (abridor favorece un lado, el
equipo el otro — el mismo tipo de conflicto que nos hizo perder la de
Red Sox el 11-sep):
  - Royals @ Red Sox: Dobnak (2.54 ERA) mejor que Suarez, pero Boston
    es mucho mejor equipo (80-68 vs 66-82).
  - Astros @ Rays: Lambert mejor ERA que Seymour, pero Tampa Bay es
    mucho mejor equipo (88-59 vs 75-73).
  - Phillies @ Braves: abridor de Philadelphia todavía "Undecided".

Uso (desde este directorio, linkeado a Railway):
    railway ssh -- python3 mlb_manual_bets.py            # dry-run
    railway ssh -- python3 mlb_manual_bets.py --live      # plata real
"""
import sys
import time
from order_executor import place_order

STAKE_USD = 40.0

BETS = [
    {
        "label": "Dodgers vs. Marlins (12-sep 4:10PM ET) -> DODGERS  [Glasnow 3.12 ERA vs Phillips 3.51 ERA | equipo 90-57 vs 72-76]",
        "token_id": "89254313729353657865448740606800371484028707376712453212148059579290368635237",
        "ref_price": 0.625,
    },
    {
        "label": "Reds vs. Brewers (12-sep 7:10PM ET) -> BREWERS  [Harrison 10-4, 3.66 ERA vs Singer 6-13, 5.03 ERA | equipo 92-56 vs 69-78]",
        "token_id": "29709678869675293155685850008106215817128012954892729405079042054890388004047",
        "ref_price": 0.625,
    },
    {
        "label": "Mariners vs. Athletics (12-sep 9:40PM ET) -> MARINERS  [Woo 4.03 ERA vs Jump 5.12 ERA | equipo 69-79 vs 60-88]",
        "token_id": "83926356025481135538956165876000129839932688776513163284546224490960048774048",
        "ref_price": 0.615,
    },
]


def main():
    live = "--live" in sys.argv
    print(f"{'*** LIVE — plata real ***' if live else 'DRY-RUN — no se manda nada'}\n")

    for bet in BETS:
        shares_est = round(STAKE_USD / bet["ref_price"], 2)
        print(f"-> {bet['label']}")
        print(f"   ${STAKE_USD:.2f} @ ~{bet['ref_price']:.3f}  (~{shares_est} shares)")

        if not live:
            continue

        resp = place_order(
            token_id=bet["token_id"],
            price=round(bet["ref_price"], 2),
            size=STAKE_USD,
            side="BUY",
        )
        print(f"   respuesta: {resp}\n")
        time.sleep(2)


if __name__ == "__main__":
    main()
