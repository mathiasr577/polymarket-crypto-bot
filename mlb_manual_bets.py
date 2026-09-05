"""
Compra manual de 3 mercados de MLB — 5-sep-2026, $40 c/u.

Esta vez el criterio no es solo precio de Polymarket: para cada
partido cruzo el precio de mercado contra el abridor probable de cada
equipo (récord/ERA de temporada, sacado de MLB.com hoy). Los 3
elegidos son los que tienen la MAYOR probabilidad de mercado Y el
mismatch de pitcheo más claro en la misma dirección — cuando ambas
señales apuntan al mismo lado, hay más razón para confiar que cuando
es solo el precio.

  1. Pirates (64%) vs Angels
     Braxton Ashcraft (14-5, 3.59 ERA) vs Yusei Kikuchi (0-5, 5.65 ERA)
     -> mismatch de pitcheo enorme, en la misma dirección que el mercado.

  2. Dodgers (65%) vs Nationals
     Tyler Glasnow (4-0, 2.79 ERA) vs Cade Cavalli (12-5, 3.12 ERA)
     -> Glasnow tiene el mejor ERA de los 6 abridores de hoy, más el
     peso de la franquicia Dodgers. Cavalli también es sólido, pero
     no alcanza.

  3. Mariners (68%) vs Athletics
     George Kirby (9-10, 4.19 ERA) vs Jeffrey Springs (3-13, 6.37 ERA)
     -> Springs es el peor abridor de la lista de hoy por lejos.

Descartados a propósito por CONTRADECIR el precio de mercado (serían
apuestas de "valor" especulativo, no las seguras que pediste):
  - Rays @ Rangers: Rasmussen (14-5, 2.95) es claramente mejor que
    deGrom (10-9, 4.00) pero el mercado favorece a Rangers 52%. Señal
    mixta, se deja afuera.
  - D-backs @ Astros: Pfaadt (7-2, 3.49) domina a Pecko (1-0, 6.23,
    apenas 1 salida) pero el mercado lo tiene 51/50. Podría ser valor
    real, pero no es una apuesta "segura" — se deja afuera a propósito.

Uso (desde este directorio, linkeado a Railway):
    railway ssh -- python3 mlb_manual_bets.py            # dry-run
    railway ssh -- python3 mlb_manual_bets.py --live      # plata real

Corré esto vos, no se dispara solo. Usar railway ssh (no railway run)
porque la IP residencial da 403 geoblock — la de Railway EU West no.
"""
import sys
import time
from order_executor import place_order

STAKE_USD = 40.0

BETS = [
    {
        "label": "Angels vs. Pirates (5-sep 6:40PM ET) -> PIRATES  [Ashcraft 3.59 ERA vs Kikuchi 5.65 ERA]",
        "token_id": "111113894891170594394149079773916491591885380177813538143106329660202794558400",
        "ref_price": 0.635,
    },
    {
        "label": "Nationals vs. Dodgers (5-sep 9:10PM ET) -> DODGERS  [Glasnow 2.79 ERA vs Cavalli 3.12 ERA]",
        "token_id": "99866995184257910609148611344011582187394703370527940150087404254207359792772",
        "ref_price": 0.645,
    },
    {
        "label": "Athletics vs. Mariners (5-sep 9:40PM ET) -> MARINERS  [Kirby 4.19 ERA vs Springs 6.37 ERA]",
        "token_id": "43507198494641002372671746028295196331127705437890934926162041212287067862674",
        "ref_price": 0.675,
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
