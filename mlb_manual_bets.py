"""
Compra manual de 3 mercados de MLB — 8-sep-2026 (noche), $45 c/u.

Re-verificado a las 5PM ET del mismo día del partido: mismos 3
abridores confirmados (sin cambios), precios de mercado casi
idénticos a los de anoche, y una noticia extra a favor de Yankees
(Aaron Judge vuelve a la alineación hoy tras 3 meses lesionado —
refuerza más el pick ya más fuerte de los 3). Único dato negativo
menor: Marlins perdió a Kyle Stowers (IL, isquiotibial) — no cambia
la apuesta pero es la más floja de las 3 igual (52%, la más ajustada).

Esta vez sumé un dato que no había usado antes: el RÉCORD GENERAL del
equipo en la temporada (via ESPN), no solo el abridor de hoy. Elegí
los 3 donde las TRES señales (récord del equipo, ERA/récord del
abridor, y precio de mercado) apuntan al mismo lado con el mayor
margen — no solo dos de tres como en rondas anteriores.

  1. Yankees (75%) vs Rockies
     Equipo: NYY 81-62 vs COL 55-88 (26 juegos de diferencia)
     Abridor: Schlittler (12-6, 2.04 ERA — el mejor ERA de TODA la
     cartelera de hoy) vs Hughes (0-6, 6.19 ERA — un desastre)
     -> las tres señales alineadas y con el margen más grande de la
     noche. La apuesta más sólida de las 3.

  2. Brewers (66%) vs Cubs
     Equipo: MIL 89-56 (el mejor récord de toda la liga hoy) vs
     CHC 81-64
     Abridor: Misiorowski (14-5, 1.97 ERA — el SEGUNDO mejor ERA de
     la cartelera) vs Peterson (7-8, 5.39 ERA)
     -> mismo patrón: equipo, abridor y mercado alineados.

  3. Marlins (55%) vs Mets
     Equipo: MIA 72-73 vs NYM 66-78
     Abridor: Alcantará (13-9, 3.54 ERA, número de victorias más alto
     de este trío) vs Manaea (4-7, 4.70 ERA)
     -> el mercado acá es más ajustado (55%) que las otras dos, pero
     las tres señales igual apuntan para el mismo lado.

Descartados a propósito por señal mixta o dato insuficiente:
  - Astros @ Phillies: el abridor de Houston es mejor (3.18 vs 5.55
    ERA) pero el EQUIPO Philadelphia es mucho mejor (81-63 vs 73-71)
    y el mercado los favorece — señales en direcciones opuestas.
  - Angels @ Red Sox: el abridor de Angels (Detmers, 3.44 ERA) es
    mejor que el de Boston (Sandoval, 4.41 ERA), pero el EQUIPO Boston
    es muchísimo mejor (80-65 vs 54-90) — misma contradicción.
  - D-backs @ Royals: buen récord de equipo para Arizona (77-68 vs
    64-81) pero no hay ERA/récord publicado todavía para su abridor
    (Burnes) — dato insuficiente, y el mercado lo tiene casi 50/50 de
    todos modos.

Uso (desde este directorio, linkeado a Railway):
    railway ssh -- python3 mlb_manual_bets.py            # dry-run
    railway ssh -- python3 mlb_manual_bets.py --live      # plata real
"""
import sys
import time
from order_executor import place_order

STAKE_USD = 45.0

BETS = [
    {
        "label": "Rockies vs. Yankees (8-sep 11:05PM ET) -> YANKEES  [Schlittler 2.04 ERA, 12-6 vs Hughes 6.19 ERA, 0-6 | equipo 81-62 vs 55-88 | +Judge vuelve hoy]",
        "token_id": "88149961926978287782196780314449628985118065203658161345636243514846352878820",
        "ref_price": 0.76,
    },
    {
        "label": "Cubs vs. Brewers (8-sep 11:40PM ET) -> BREWERS  [Misiorowski 1.97 ERA, 14-5 vs Peterson 5.39 ERA | equipo 89-56 vs 81-64]",
        "token_id": "20132959382596203032910164842937952852490220949031243736381111143354467768294",
        "ref_price": 0.66,
    },
    {
        "label": "Mets vs. Marlins (8-sep 10:40PM ET) -> MARLINS  [Alcantara 3.54 ERA, 13-9 vs Manaea 4.70 ERA | equipo 72-73 vs 66-78 | -Stowers IL]",
        "token_id": "100402930733517859553033568487421284989702898538143745066977613006030392716081",
        "ref_price": 0.52,
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
