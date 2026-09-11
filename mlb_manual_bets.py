"""
Compra manual de 3 mercados de MLB — 11-sep-2026, $40 c/u.

Mismo criterio de siempre: precio de mercado + récord de equipo +
abridor confirmado, eligiendo los 3 donde las tres señales apuntan en
la misma dirección con el mayor margen. Hoy la cartelera tenía tres
abridores de elite clarísimos (Snell 1.97, Gray 2.69, Sale 2.10) — la
señal más limpia que vimos en varios días.

  1. Dodgers (65.5%) vs Marlins
     Abridor: Snell (3-1, 1.97 ERA — el mejor de toda la cartelera de
     hoy) vs Gusto (1-4, 4.23 ERA)
     Equipo: LAD 89-57 vs MIA 72-75

  2. Red Sox (64.5%) vs Royals
     Abridor: Gray (17-4, 2.69 ERA — el mejor récord del día) vs
     Lugo (6-8, 5.04 ERA)
     Equipo: BOS 80-67 vs KC 65-82

  3. Braves (62.5%) vs Phillies
     Abridor: Sale (14-9, 2.10 ERA) vs Nola (6-10, 4.76 ERA)
     Equipo: ATL 86-61 vs PHI 82-65

Descartados a propósito por señal mixta:
  - Reds @ Brewers: Abbott (Reds) tiene ERA levemente mejor que May
    (Brewers), pero el equipo favorece a Brewers enorme (91-56, mejor
    récord de la liga) — señales en direcciones opuestas.
  - Mets @ Yankees: abridores casi idénticos en ERA (McLean 3.06 vs
    Rodon 3.09), solo el equipo desempata — señal débil.

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
        "label": "Dodgers vs. Marlins (11-sep 7:10PM ET) -> DODGERS  [Snell 1.97 ERA vs Gusto 4.23 ERA | equipo 89-57 vs 72-75]",
        "token_id": "55002249387036495345982054447485774024728223596512918828269505992441438648612",
        "ref_price": 0.655,
    },
    {
        "label": "Royals vs. Red Sox (11-sep 7:10PM ET) -> RED SOX  [Gray 17-4, 2.69 ERA vs Lugo 5.04 ERA | equipo 80-67 vs 65-82]",
        "token_id": "38332595592467516145651294679578078035245459978716558512124893180623730000182",
        "ref_price": 0.645,
    },
    {
        "label": "Phillies vs. Braves (11-sep 7:15PM ET) -> BRAVES  [Sale 14-9, 2.10 ERA vs Nola 4.76 ERA | equipo 86-61 vs 82-65]",
        "token_id": "74834568141968122506589401336439382929458771337886372009351505602834271772474",
        "ref_price": 0.625,
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
