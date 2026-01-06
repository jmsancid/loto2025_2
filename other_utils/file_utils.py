#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from datetime import datetime, timedelta
from typing import Union
from constants import DBFILE, PRIMITIVA, PRIMIFIELDS, EUROMILLONES, EUROFIELDS
from pathlib import Path
from db_utils.db_management import DBManager
from web_utils.get_web_loto_results import getPrimiLatestResults, getEuroLatestResults


def check_results_db_file() -> Path | None:
    """
    Verifica si el archivo loterias.db existe en el mismo directorio que el archivo main.py.
    :return Path si existe o None si no existe la base de datos
    """
    # Se obtiene la ruta del directorio del archivo main.py
    # __file__ se refiere a la ruta del script actual (file_utils.py)
    # y parent.parent nos permite subir dos niveles para llegar a la carpeta principal
    main_dir = Path(__file__).parent.parent

    # Se construye la ruta completa al archivo datos.db
    db_file_path = main_dir / DBFILE

    # Se comprueba si el archivo existe
    if db_file_path.is_file():
        # print(f"El archivo {db_file_path} existe.")
        return db_file_path
    else:
        print(f"El archivo {db_file_path} no existe.")
        return None


def get_latest_results_in_db(sorteo) -> Union[datetime.date, int]:
    """
    Devuelve la fecha del último sorteo almacenado en la base de datos de primi o euro
    :param sorteo: tipo de sorteo, euromillones o primitiva
    :return: fecha del último sorteo en formato datetime
    """

    # Compruebo si existe la base de datos (debe haberse creado previamente)
    loto_db = check_results_db_file()
    # print(loto_db)

    if not loto_db:
        # No existe la base de datos de loterías.
        # Abandono el programa con error
        print("\nEs necesario crear la base de datos de loterías con los resultados históricos")
        return 1

    # Accedo a la base de datos
    gestor_db = DBManager(loto_db)

    # Extraigo la fecha del último sorteo almacenado en la base de datos
    fecha_ultimo_sorteo_almacenado = gestor_db.fecha_ultimo_resultado(sorteo, 'fecha')

    if fecha_ultimo_sorteo_almacenado:
        fecha_datetime = datetime.strptime(fecha_ultimo_sorteo_almacenado, "%Y-%m-%d")
        fecha = datetime(fecha_datetime.year, fecha_datetime.month, fecha_datetime.day).date()
        return fecha

    return 1


def convierte_combinaciones_en_lista_dict(combinaciones, columnas) -> list | None:
    """
    Convierte un diccionario formado por las combinaciones premiadas en los sorteos de Primitiva
    y Euromillones a otro formato de diccionario con los campos utilizados en las tablas de las
    bases de datos.
    El tipo de sorteo se identifica por la fecha. M-V Euromilloones. L-J-S Primitiva
    :param combinaciones: diccionario con la fecha como clave y una lista con los números que han salido en
    sorteo correspondiente
    :param columnas: campos de la base de datos correspondientes a las fechas, números y estrellas o
    complementario más reintegro
    :return: True si se ha realizado la conversión y False en caso contrario
    """

    # Construyo la lista de diccionarios con los campos de la tabla de la db
    lista_de_combinaciones = [
        {
            # La fecha es el primer elemento
            columnas[0]: fecha,
            # Los otros campos se extraen de los valores
            **{columnas[i]: valores[i - 1] for i in range(1, len(columnas))}
        }
        for fecha, valores in combinaciones.items()
    ]

    if lista_de_combinaciones:
        return lista_de_combinaciones

    return None


def inserta_resultados_sorteos_en_db(combinaciones) -> bool | None:
    """
    Inserta una combinación en la base de datos de loterías, en la tabla del sorteo correspondiente.
    El tipo de sorteo se identifica por la fecha. M-V Euromilloones. L-J-S Primitiva
    :param combinaciones: diccionario con la fecha como clave y una lista con los números que han salido en el
    :return: True si se ha insertado el registro correctamente y False en caso contrario
    """

    # Compruebo si la fecha de la combinación corresponde a Primitiva o Euromillón
    # según el día de la semaan en el que se ha celebrado. Basta con mirar la fecha del
    # primer elemento del diccionario

    fecha_sorteo = next(iter(combinaciones))  # extraigo la primera fecha del diccionario
    dia_semana = fecha_sorteo.isoweekday()

    if dia_semana in (1, 4, 6):
        tabla_sorteo = PRIMITIVA
        columnas = PRIMIFIELDS[1:]  # Ignoramos el campo idx, que se rellena automáticamente
    elif dia_semana in (2, 5):
        tabla_sorteo = EUROMILLONES
        columnas = EUROFIELDS[1:]  # Ignoramos el campo idx, que se rellena automáticamente
    else:
        print(f"La fecha extraída de la combinación, {fecha_sorteo} no corresponde ni a "
              f"Primitiva ni a Euromillones\n\n."
              f"Registro NO INSERTADO")
        return None

    # Convertimos el diccionario 'combinaciones' al formato en el que se puede insertar en la base de datos.
    comb_a_db = convierte_combinaciones_en_lista_dict(combinaciones, columnas)

    # Compruebo si existe la base de datos (debe haberse creado previamente)
    loto_db = check_results_db_file()
    # print(loto_db)

    if not loto_db:
        # No existe la base de datos de loterías.
        # Abandono el programa con error
        print("\nEs necesario crear la base de datos de loterías con los resultados históricos")
        return False

    # Accedo a la base de datos
    gestor_db = DBManager(loto_db)

    if gestor_db.insertar_registros(tabla_sorteo, comb_a_db):
        print(*comb_a_db, sep='\n')
        return True
    else:
        print(f"No se ha podido insertar ningún registro de la lista"
              f"Combinaciones a insertar: {comb_a_db}")
        return False


def need_db_update(sorteo: str) -> bool:
    """
    Determina si hay que actualizar la tabla del sorteo, Primitiva o Euromillones
    Sólo se actualiza la base de datos al completar la semana, o sea, se actualiza cuando
    no haya datos de la semana anterior.
    :param sorteo: Primitiva o Euromillones
    :return: True si hay que actualizar y False en caso contrario
    """
    #este_anno, esta_semana, este_dia = datetime.now().isocalendar()  # devuelve año, número de semana y
    # día semana (1 a 7)  BORRAR CUANDO COMPRUEBE LAS LÍNEAS SIGUIENTES

    # Expreso el número de semana considerando también el año para que, por ejemplo, la semana 1 de 2026 sea mayor
    # que la semana 54 de 2025. Ejemplo, semana 2532 es la semana 32 de 2025
    hoy = datetime.now()
    this_week = hoy.isocalendar().year % 100 * 100 + hoy.isocalendar().week
    last_week_date = hoy - timedelta(days=7)
    last_week = last_week_date.isocalendar().year % 100 * 100 + last_week_date.isocalendar().week
    este_dia = hoy.isoweekday()

    if sorteo == PRIMITIVA:
        fecha_ultimo_sorteo_primi_guardado = get_latest_results_in_db(PRIMITIVA)
        # num_semana_ultima_primi = fecha_ultimo_sorteo_primi_guardado.isocalendar()[1]
        num_semana_ultima_primi = (fecha_ultimo_sorteo_primi_guardado.year % 100 * 100 +
                                   fecha_ultimo_sorteo_primi_guardado.isocalendar().week)
        dia_semana_ultima_primi = fecha_ultimo_sorteo_primi_guardado.isoweekday()
        print(f"Fecha último sorteo guardado en base de datos de primitiva; "
              f"{fecha_ultimo_sorteo_primi_guardado}\n\n{type(fecha_ultimo_sorteo_primi_guardado)}")
        if num_semana_ultima_primi < last_week or \
                num_semana_ultima_primi < this_week and dia_semana_ultima_primi < 6 or \
                num_semana_ultima_primi == this_week and dia_semana_ultima_primi < 6 and este_dia == 7:
            # La primitiva se actualiza siempre que la última guardada sea de hace más de 2 semanas o
            # si la última guardada es anterior al sábado de la semana anterior o
            # si la última guardada es de la semana anterior y el día actual es domingo.
            return True
    if sorteo == EUROMILLONES:
        fecha_ultimo_sorteo_euro_guardado = get_latest_results_in_db(EUROMILLONES)
        # num_semana_ultima_euro = fecha_ultimo_sorteo_euro_guardado.isocalendar()[1]
        num_semana_ultima_euro = (fecha_ultimo_sorteo_euro_guardado.year % 100 * 100 +
                                  fecha_ultimo_sorteo_euro_guardado.isocalendar().week)

        dia_semana_ultima_euro = fecha_ultimo_sorteo_euro_guardado.isoweekday()
        print(f"Fecha último sorteo guardado en base de datos de euromillones: "
              f"{fecha_ultimo_sorteo_euro_guardado}\n{type(fecha_ultimo_sorteo_euro_guardado)}")
        if num_semana_ultima_euro < last_week or \
                num_semana_ultima_euro < this_week and dia_semana_ultima_euro < 5 or \
                num_semana_ultima_euro == this_week and dia_semana_ultima_euro < 5 and este_dia == 7:
            # Euromillones se actualiza siempre que la última guardada sea de hace más de 2 semanas o
            # si la última guardada es anterior al viernes de la semana anterior o
            # si la última guardada es de la semana anterior y el día actual es domingo.
            return True
    return False


def actualizacion_db(sorteo: str) -> bool | None:
    """
    Actualiza la base de datos del sorteo correspondiente con los últimos resultados
    :param sorteo: Primitiva o Euromillones
    :return: True si la actualización es correcta y False en caso contrario
    """
    if sorteo == EUROMILLONES:
        print(f"\nACTUALIZANDO Base de datos de Euromillones")
        fecha_ultimo_sorteo_euro_guardado = get_latest_results_in_db(EUROMILLONES)
        ultimos_resultados_euromillon = getEuroLatestResults()
        # print(ultimos_resultados_euromillon)
        euro_comb_faltantes_en_db = {fecha: combi for fecha, combi in ultimos_resultados_euromillon.items()
                                     if fecha > fecha_ultimo_sorteo_euro_guardado}
        # print(euro_comb_faltantes_en_db)
        return inserta_resultados_sorteos_en_db(euro_comb_faltantes_en_db)
    if sorteo == PRIMITIVA:
        print(f"\nACTUALIZANDO Base de datos de Primitiva")
        fecha_ultimo_sorteo_primi_guardado = get_latest_results_in_db(PRIMITIVA)
        ultimos_resultados_primitiva = getPrimiLatestResults()
        # print(ultimos_resultados_primitiva)
        primi_comb_faltantes_en_db = {fecha: combi for fecha, combi in ultimos_resultados_primitiva.items()
                                      if fecha > fecha_ultimo_sorteo_primi_guardado}
        # print(primi_comb_faltantes_en_db)
        return inserta_resultados_sorteos_en_db(primi_comb_faltantes_en_db)

    return False
