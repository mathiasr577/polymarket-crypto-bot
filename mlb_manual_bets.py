"""
Compra manual de 3 mercados de MLB — 9-sep-2026 (noche), $40 c/u.

Mismo criterio de siempre: precio de mercado + récord de equipo +
abridor confirmado (W-L/ERA real de hoy), eligiendo los 3 donde las
tres señales apuntan en la misma dirección con el mayor margen.

  1. Yankees (69%) vs Rockies
     Equipo: NYY 82-62 vs COL 55-89 (27 juegos de diferencia)
     Abridor: Warren (4.16 ERA) vs Sugano (12-8, 5.19 ERA — más
     victorias pero peor ERA)

  2. Red Sox (67%) vs Angels
     Equipo: BOS 80-66 vs LAA 55-90 (25 juegos de diferencia)
     Abridor: Bennett (9-6, 3.34 ERA) vs Johnson (3-8, 5.17 ERA)

  3. Phillies (59%) vs Astros
     Equipo: PHI 81-64 vs HOU 74-71
     Abridor: Sánchez (16-5, 2.58 ERA — el mejor abridor de la noche)
     vs Brown (5-3, 3.31 ERA)

Descartados a propósito por señal mixta (abridor apunta fuerte para un
lado, mercado casi 50/50):
  - Guardians @ Orioles: Griffin (15-4, 3.26 ERA) claramente mejor que
    Baz (5-15, 3.91 ERA), pero el mercado lo tiene 52/49.
  - D-backs @ Royals: Lynch IV (3.51 ERA) mejor que Gallen (3-9,
    6.34 ERA — el peor abridor de la noche), pero el mercado favorece
    a Arizona 53/48 de todos modos (pesa más el equipo).

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
        "label": "Rockies vs. Yankees (9-sep 7:05PM ET) -> YANKEES  [Warren 4.16 ERA vs Sugano 5.19 ERA | equipo 82-62 vs 55-89]",
        "token_id": "102610793437419434317942016639744997329315321292066569643528136750802956679568",
        "ref_price": 0.685,
    },
    {
        "label": "Angels vs. Red Sox (9-sep 6:45PM ET) -> RED SOX  [Bennett 3.34 ERA vs Johnson 5.17 ERA | equipo 80-66 vs 55-90]",
        "token_id": "73472705305215679423165207336394684080922271528558499724867912312811116041446",
        "ref_price": 0.665,
    },
    {
        "label": "Astros vs. Phillies (9-sep 6:40PM ET) -> PHILLIES  [Sanchez 2.58 ERA, 16-5 vs Brown 3.31 ERA | equipo 81-64 vs 74-71]",
        "token_id": "26163023612231363062836937730589969759234649459704881282856491869593937399842",
        "ref_price": 0.585,
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
