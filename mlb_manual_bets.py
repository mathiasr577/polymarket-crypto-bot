"""
Compra manual de 3 mercados de MLB — un disparo único, elegido a mano
hoy (3-sep-2026). NO es parte de ningún bot automático y NO se corre
solo: lo tenés que ejecutar VOS desde tu terminal.

Los 3 elegidos (mayor probabilidad implícita de la cartelera del
4-sep, sacados en vivo de gamma-api.polymarket.com):
  - Pirates  (65%) vs Angels    | 4-sep 6:45 PM ET
  - Mets     (64%) vs Giants    | 4-sep 7:10 PM ET
  - Dodgers  (74%) vs Nationals | 4-sep 10:10 PM ET

Uso (desde este directorio, que ya está linkeado a Railway):
    railway run python3 mlb_manual_bets.py            # dry-run, no manda nada
    railway run python3 mlb_manual_bets.py --live      # ejecuta de verdad, plata real

`railway run` inyecta PRIVATE_KEY/FUNDER del servicio sin que tengas
que exportarlos a mano. Corré primero SIN --live para ver qué haría.

Los precios de referencia son los que estaban en el libro cuando arme
esto — si se movieron mucho para cuando corras esto, el margen de
slippage (3%, mismo que usa el resto del bot) puede no alcanzar y la
orden simplemente no se llena (no paga de más silenciosamente).
"""
import sys
import time
from order_executor import place_order

STAKE_USD = 30.0

BETS = [
    {
        "label": "Angels vs. Pirates (4-sep 6:45PM ET) -> PIRATES",
        "token_id": "43797855474710463342539060080918268666012118245871428808651866780360879877284",
        "ref_price": 0.645,
    },
    {
        "label": "Giants vs. Mets (4-sep 7:10PM ET) -> METS",
        "token_id": "10333160508521593657175826571699847556632864581503617955746824827412179308362",
        "ref_price": 0.635,
    },
    {
        "label": "Nationals vs. Dodgers (4-sep 10:10PM ET) -> DODGERS",
        "token_id": "23450853163862604024762059009323548949257870053052451419800797889700808979874",
        "ref_price": 0.735,
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
