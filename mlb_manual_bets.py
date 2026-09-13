"""
Compra manual de 3 mercados de MLB — 13-sep-2026, $40 c/u.

Mismo criterio de siempre: precio de mercado + récord de equipo +
abridor confirmado, eligiendo los 3 donde las tres señales apuntan en
la misma dirección con el mayor margen.

  1. Yankees (62.5%) vs Mets
     Schlittler (13-6, 2.01 ERA) vs Scott (4-4, 3.81 ERA)
     Equipo: NYY 85-63 vs NYM 69-79

  2. Cubs (59.5%) vs Pirates
     Boyd (6-1, 3.41 ERA) vs Chandler (10-10, 4.25 ERA)
     Equipo: CHC 83-66 vs PIT 74-75

  3. Braves (54.5%) vs Phillies
     Holmes (15-15, 3.74 ERA) vs Painter (3-8, 5.55 ERA)
     Equipo: ATL 88-61 vs PHI 82-67

Descartados a propósito por señal mixta (abridor favorece un lado, el
equipo el otro):
  - Astros @ Rays: Wesneski (4-1, 3.18 ERA) mejor que Peralta, pero
    Tampa Bay es mucho mejor equipo (89-59 vs 75-74).
  - Reds @ Brewers: Chase Burns (15-3, 2.73 ERA) élite, pero Milwaukee
    es mucho mejor equipo (93-56 vs 69-79).
  - Padres @ Giants: Webb (8-8, 4.10 ERA) mejor que Pivetta (viene de
    lesión, 1-2, 4.50), pero San Diego es mejor equipo (80-68 vs 62-87).

Descartado por dato de abridor no confiable:
  - Dodgers @ Marlins: stats encontrados de Sheehan inconsistentes
    entre fuentes.

Descartado a propósito aunque señales alineadas (patrón repetido):
  - Royals @ Red Sox: Red Sox (62.5%) ya nos quemó 2 veces (9-sep,
    11-sep) con señales igual de alineadas — se evita ese equipo en
    particular por ahora.

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
        "label": "Mets vs. Yankees (13-sep 1:35PM ET) -> YANKEES  [Schlittler 2.01 ERA vs Scott 3.81 ERA | equipo 85-63 vs 69-79]",
        "token_id": "101960293859407347948900822039681739125651573477238121731594035343153530077189",
        "ref_price": 0.625,
    },
    {
        "label": "Pirates vs. Cubs (13-sep 2:20PM ET) -> CUBS  [Boyd 6-1, 3.41 ERA vs Chandler 10-10, 4.25 ERA | equipo 83-66 vs 74-75]",
        "token_id": "11128162176137494751143534450945065509897046852965888068827616950952422529385",
        "ref_price": 0.595,
    },
    {
        "label": "Phillies vs. Braves (13-sep 1:35PM ET) -> BRAVES  [Holmes 15-15, 3.74 ERA vs Painter 3-8, 5.55 ERA | equipo 88-61 vs 82-67]",
        "token_id": "82990207402545549102824180090410788072844314823317600116896392916680876563304",
        "ref_price": 0.545,
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
