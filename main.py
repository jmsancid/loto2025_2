#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from constants import PRIMITIVA, EUROMILLONES, Q_RESULTADOS, LUNES, MARTES, JUEVES, VIERNES, SABADO
from other_utils.file_utils import need_db_update, actualizacion_db, check_results_db_file
from lotto_analysis.data_processing import (load_primitiva_data, load_euromillones_data, analizar_primitiva,
                                            analizar_euromillon, mostrar_combinaciones_por_dia)

from db_utils.db_management import DBManager

def main():
    """
    Programa para preparar combinaciones de primitiva y euromillones
    :return: 0 si Todo es correcto
    """

    # Compruebo si es necesario actualizar la base de datos de Primitivas y Euromillones

    if need_db_update(EUROMILLONES):
        if actualizacion_db(EUROMILLONES):
            print(f"\nActualización Base de datos de Euromillones FINALIZADA")
        else:
            print(f"No se ha realizado la actualización de Euromillones")
    else:
        print(f"\nNO ES NECESARIO ACTUALIZAR la Base de datos de Euromillones\n")

    if need_db_update(PRIMITIVA):
        if actualizacion_db(PRIMITIVA):
            print(f"\nActualización Base de datos de Primitiva FINALIZADA")
        else:
            print(f"No se ha realizado la actualización de Primitiva")
    else:
        print(f"\nNO ES NECESARIO ACTUALIZAR la Base de datos de Primitiva\n")

    loto_db_path = check_results_db_file()
    if not loto_db_path:
        print(f"No se ha encontrado el archivo de base de datos {loto_db_path}")
        raise FileNotFoundError

    loto_db = DBManager(loto_db_path)  # Instancia de la base de datos de sorteos de primitiva y euromillones
    primi_df = load_primitiva_data(loto_db)  # Panda dataframe de primitiva
    euro_df = load_euromillones_data(loto_db)  # Panda dataframes de números y estrellas
    # euro_num_df, euro_estrellas_df = load_euromillones_data(loto_db)  # Panda dataframes de números y estrellas
    # de euromillones

    # Analisis de primitiva en base a la fase lunar
    resultados_primitiva = analizar_primitiva(primi_df)

    # Filtro por las fases lunares de lunes, jueves y sábado
    resultados_lunes = mostrar_combinaciones_por_dia(resultados_primitiva, LUNES, Q_RESULTADOS)
    print(f"\nRESULTADOS CALCULADOS PARA EL LUNES\n"
          f"***********************************\n"
          f"{resultados_lunes.to_string(index=False)}")

    resultados_jueves = mostrar_combinaciones_por_dia(resultados_primitiva, JUEVES, Q_RESULTADOS)
    print(f"\nRESULTADOS CALCULADOS PARA EL JUEVES\n"
          f"***********************************\n"
          f"{resultados_jueves.to_string(index=False)}")

    resultados_sabado = mostrar_combinaciones_por_dia(resultados_primitiva, SABADO, Q_RESULTADOS)
    print(f"\nRESULTADOS CALCULADOS PARA EL SABADO\n"
          f"***********************************\n"
          f"{resultados_sabado.to_string(index=False)}")

    # Analisis de los números de euromillones en base a la fase lunar
    resultados_euromillon = analizar_euromillon(euro_df)

    # Filtro por las fases lunares de martes y viernes (sólo imprimo 2 combinaciones
    # resultados_martes = resultados_euromillon[resultados_euromillon['fase_lunar'] ==
    #                                           fase_lunar_martes].head(Q_RESULTADOS - 3)
    resultados_martes = mostrar_combinaciones_por_dia(resultados_euromillon, MARTES, Q_RESULTADOS)
    print(f"\nRESULTADOS CALCULADOS PARA EL MARTES\n"
          f"***********************************\n"
          f"{resultados_martes.to_string(index=False)}")

    resultados_viernes = mostrar_combinaciones_por_dia(resultados_euromillon, VIERNES, Q_RESULTADOS)
    print(f"\nRESULTADOS CALCULADOS PARA EL VIERNES\n"
          f"***********************************\n"
          f"{resultados_viernes.to_string(index=False)}")

    return 0


if __name__ == '__main__':
    main()