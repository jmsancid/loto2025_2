#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from datetime import date
import logging

from constants import PRIMITIVA, EUROMILLONES
from other_utils.file_utils import need_db_update, actualizacion_db, check_results_db_file

from db_utils.db_management import DBManager
from other_utils.ranking_semanal import compute_weekly_apuestas, format_weekly_result


def main():
    """
    Programa para preparar combinaciones de primitiva y euromillones
    :return: 0 si Todo es correcto
    """

    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")

    # Compruebo si es necesario actualizar la base de datos de Primitivas y Euromillones

    if need_db_update(EUROMILLONES):
        if actualizacion_db(EUROMILLONES):
            print(f"\nActualización Base de datos de Euromillones FINALIZADA")
        else:
            print(f"No se ha realizado la actualización de Euromillones")
    else:
        print(f"NO ES NECESARIO ACTUALIZAR la Base de datos de Euromillones\n")

    if need_db_update(PRIMITIVA):
        if actualizacion_db(PRIMITIVA):
            print(f"\nActualización Base de datos de Primitiva FINALIZADA")
        else:
            print(f"No se ha realizado la actualización de Primitiva")
    else:
        print(f"NO ES NECESARIO ACTUALIZAR la Base de datos de Primitiva\n")

    loto_db_path = check_results_db_file()
    if not loto_db_path:
        print(f"No se ha encontrado el archivo de base de datos {loto_db_path}")
        raise FileNotFoundError

    loto_db = DBManager(loto_db_path)  # Instancia de la base de datos de sorteos de primitiva y euromillones

    print("DB PATH:", loto_db.db_path)

    with loto_db as db:
        db.sync_sorteo_influencers()

    apuestas_semanales = compute_weekly_apuestas(db=loto_db,
        today=date.today()
    )

    week_start = apuestas_semanales.week_start
    week_end = apuestas_semanales.week_end
    tol_p = apuestas_semanales.tol_primitiva
    tol_e = apuestas_semanales.tol_euro

    print("===WEEKLY_RESULT_BEGIN===")
    print(format_weekly_result(apuestas_semanales))
    print("===WEEKLY_RESULT_END===")

    with loto_db as db:
        # weekly ya calculado
        db.upsert_santi_primitiva(
            apuestas_semanales.apuestas_primitiva,
            week_start=week_start, week_end=week_end,
            tol_frac=tol_p, method_version="v1", city="Madrid"
        )
        db.upsert_santi_euromillones(
            apuestas_semanales.apuestas_euromillones,
            week_start=week_start, week_end=week_end,
            tol_frac=tol_e, method_version="v1", city="Paris"
        )

    return 0


if __name__ == '__main__':
    main()