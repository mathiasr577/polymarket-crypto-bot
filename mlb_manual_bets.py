"""
Compra manual de 3 mercados de MLB — 6-sep-2026, $35 c/u.

Mismo método que el 5-sep (precio de mercado + abridor probable real,
ERA y récord de temporada, sacado de MLB.com hoy), con un filtro extra
que ayer no apliqué con suficiente cuidado: prioricé los partidos
donde el mercado y el pitcheo apuntan en la MISMA dirección con el
mayor margen posible, y descarté cualquier caso con muestra chica
(pitchers con 0-1 decisiones) porque esos ERA no dicen mucho todavía.

  1. Guardians (61.5%) vs Tigers
     Gavin Williams (13-7, 3.81 ERA) vs Jackson Jobe (1-2, 4.63 ERA)
     -> Williams tiene 13 victorias en la temporada, récord y ERA
     mejores por lejos que un abridor con apenas 3 decisiones.

  2. Mariners (64.5%) vs Athletics
     Bryan Woo (10-9, 4.25 ERA) vs Gage Jump (6-9, 5.15 ERA)
     -> mismo tipo de mismatch que el 5-sep (esa perdimos — es una
     apuesta distinta, con otros abridores, no "revancha").

  3. Dodgers (64.5%) vs Nationals
     Justin Wrobleski (11-5, 3.65 ERA) vs Andrew Alvarez (2-6, 3.47 ERA)
     -> acá el ERA es parejo, pero el récord (11-5 vs 2-6) y la fuerza
     general del equipo Dodgers son la base real de esta, no el ERA
     solo — igual que la del 5-sep, que sí ganamos.

Descartados a propósito por señal mixta o muestra chica:
  - Braves @ Phillies: ERA parejo, mercado 50/51 — coinflip real.
  - Giants @ Mets: el abridor de Giants tiene mejor ERA (2.25) pero
    CERO decisiones en la temporada — muestra insuficiente.
  - D-backs @ Astros: Eduardo Rodríguez (14-5, 2.59 ERA, el mejor
    abridor de HOY) pero el mercado tiene esto 50/51 — el mercado no
    lo acompaña, sería una apuesta de "valor" especulativo, no segura.

Uso (desde este directorio, linkeado a Railway):
    railway ssh -- python3 mlb_manual_bets.py            # dry-run
    railway ssh -- python3 mlb_manual_bets.py --live      # plata real
"""
import sys
import time
from order_executor import place_order

STAKE_USD = 35.0

BETS = [
    {
        "label": "Tigers vs. Guardians (6-sep 1:40PM ET) -> GUARDIANS  [Williams 3.81 ERA, 13-7 vs Jobe 4.63 ERA, 1-2]",
        "token_id": "15625999509797276893820228747122054491128791147014300727249978943863135689919",
        "ref_price": 0.615,
    },
    {
        "label": "Athletics vs. Mariners (6-sep 4:10PM ET) -> MARINERS  [Woo 4.25 ERA vs Jump 5.15 ERA]",
        "token_id": "40145216935047072679618812220967389587237342261367356792779848965121234822271",
        "ref_price": 0.645,
    },
    {
        "label": "Nationals vs. Dodgers (6-sep 10:10PM ET) -> DODGERS  [Wrobleski 11-5 vs Alvarez 2-6]",
        "token_id": "101279472385609715829438473394947480656628050512671135483131814465078673073731",
        "ref_price": 0.645,
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
