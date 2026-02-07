#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# lotto_analysis/data_processing.py
import pandas as pd
from db_utils.db_management import DBManager
from other_utils.fase_lunar import obtener_fase_lunar, get_whole_week_moon_phase
from constants import PRIMITIVA, EUROMILLONES, Q_RESULTADOS

def load_primitiva_data(db_manager: DBManager):
    """
    Carga los datos de la Primitiva desde la base de datos
    y los convierte en un DataFrame de Pandas.
    """
    # Nombres de los campos numéricos para Transformar el DataFrame a formato largo
    numeros_cols = [f'n{i}' for i in range(1, 7)]
    adicionales_cols = ['compl', 're']

    # Unimos las listas de columnas para el melt.
    todas_las_cols = numeros_cols + adicionales_cols

    # Hay reintegros, re, nulos porque en los primeros sorteos no existían.
    # Se convierte el formato de todos los numeros a Int64 para poder manipular valores nulos
    fmt_num_todas_las_cols = {campo: 'Int64' for campo in todas_las_cols}

    try:
        # Aquí es donde se establece la conexión y se consulta
        with db_manager as db: # Asumiendo que get_connection() es tu gestor de contexto
            df_primitiva = pd.read_sql_query(f"SELECT * FROM {PRIMITIVA}", db.conn,
                                             dtype=fmt_num_todas_las_cols)


        df_primitiva_largo = pd.melt(
            df_primitiva,
            id_vars=['fecha'],
            value_vars=todas_las_cols,
            var_name='tipo_numero',  # Nuevo nombre para diferenciar si es n1, compl, re, etc.
            value_name='numero'
       )
        df_primitiva_largo['fecha'] = pd.to_datetime(df_primitiva_largo['fecha'])
        df_primitiva_largo['fase_lunar'] = df_primitiva_largo['fecha'].apply(obtener_fase_lunar)
        return df_primitiva_largo

    except Exception as e:
        print(f"Error al cargar los datos de la Primitiva: {e}")
        return None


def load_euromillones_data(db_manager: DBManager):
    """
    Carga los datos de la Euromillones desde la base de datos
    y los convierte en un DataFrame de Pandas.
    """
    # --- DataFrame para los números principales ---
    numeros_cols = [f'n{i}' for i in range(1, 6)]
    estrellas_cols = [f'e{i}' for i in range(1, 3)]
    todas_las_cols = numeros_cols + estrellas_cols

    # Se convierte el formato de todos los numeros a Int64 para poder manipular valores nulos
    fmt_num_todas_las_cols = {campo: 'Int64' for campo in todas_las_cols}

    try:
        # Aquí es donde se establece la conexión y se consulta
        with db_manager as db: # Asumiendo que get_connection() es tu gestor de contexto
            df_euromillones = pd.read_sql_query(f"SELECT * FROM {EUROMILLONES}", db.conn,
                                                dtype=fmt_num_todas_las_cols)

            df_numeros = pd.melt(
                df_euromillones,
                id_vars=['fecha'],
                value_vars=todas_las_cols,
                var_name='tipo_numero',
                value_name='numero'
            )
            df_numeros['fecha'] = pd.to_datetime(df_numeros['fecha'])
            df_numeros['fase_lunar'] = df_numeros['fecha'].apply(obtener_fase_lunar)

            # # --- DataFrame para las estrellas ---
            # estrellas_cols = ['e1', 'e2']
            # df_estrellas = pd.melt(
            #     df_euromillones,
            #     id_vars=['fecha'],
            #     value_vars=estrellas_cols,
            #     var_name='tipo_estrella',
            #     value_name='estrella'
            # )
            # df_estrellas['fecha'] = pd.to_datetime(df_estrellas['fecha'])
            # df_estrellas['fase_lunar'] = df_estrellas['fecha'].apply(obtener_fase_lunar)


            # Retornamos ambos DataFrames en una tupla.
            # return df_numeros, df_estrellas
            return df_numeros

    except Exception as e:
        print(f"Error al cargar los datos de Euromillones: {e}")
        return None, None


def analizar_primitiva(df_primitiva):
    """
    Analiza el DataFrame de la Primitiva agrupando por fase lunar y tipo de número,
    para encontrar la frecuencia de cada valor en la columna 'numero'.
    """
    # La agrupación se realiza por 'fase_lunar' y 'tipo_numero'.
    # Luego, se aplica .value_counts() a la columna 'numero' en cada grupo.
    # El resultado será una serie con un índice multi-nivel.
    frecuencia_por_grupo = df_primitiva.groupby(['fase_lunar', 'tipo_numero'])['numero'].value_counts()

    # Se convierte la serie resultante en un DataFrame para una mejor visualización.
    df_frecuencia = frecuencia_por_grupo.to_frame(name='frecuencia').reset_index()

    # Se ordenan los resultados para mostrar los más frecuentes primero en cada grupo.
    df_frecuencia = df_frecuencia.sort_values(by=['fase_lunar', 'tipo_numero', 'frecuencia'],
                                              ascending=[True, True, False])

    # Limita los resultados a los primeros Q_RESULTADOS de cada grupo.
    # Se usa .groupby().head() para obtener los top N resultados por grupo.
    # top_resultados = df_frecuencia.groupby(['fase_lunar', 'tipo_numero']).head(Q_RESULTADOS)

    combinaciones = transformar_primitiva_a_dataframe(df_frecuencia)

    return combinaciones


# def analizar_euromillon(df_euromillon_numeros, df_euromillon_estrellas):
#     top_resultados_euromillon_numeros = analizar_euromillon_numeros(df_euromillon_numeros)
#     top_resultados_euromillon_estrellas = analizar_euromillon_estrellas(df_euromillon_estrellas)
#     combinaciones = transformar_euromillon_a_tuplas(top_resultados_euromillon_numeros,
#                                                     top_resultados_euromillon_estrellas)

    # return combinaciones


# def analizar_euromillon_numeros(df_euromillon):
def analizar_euromillon(df_euromillon):
    """
    Analiza los números del Euromillón para encontrar los más frecuentes
    por fase lunar, limitando los resultados a Q_RESULTADOS.
    """

    # Agrupa y cuenta la frecuencia.
    frecuencia_por_grupo = df_euromillon.groupby(['fase_lunar', 'tipo_numero'])['numero'].value_counts()

    df_frecuencia = frecuencia_por_grupo.to_frame(name='frecuencia').reset_index()
    df_frecuencia = df_frecuencia.sort_values(by=['fase_lunar', 'tipo_numero', 'frecuencia'],
                                              ascending=[True, True, False])

    # Limita los resultados a los primeros Q_RESULTADOS de cada grupo.
    # Se usa .groupby().head() para obtener los top N resultados por grupo.
    # top_resultados = df_frecuencia.groupby(['fase_lunar', 'tipo_numero']).head(Q_RESULTADOS)
    #
    # return top_resultados

    combinaciones = transformar_euromillon_a_dataframe(df_frecuencia)

    return combinaciones



def analizar_euromillon_estrellas(df_estrellas):
    """
    Analiza el DataFrame de las estrellas del Euromillón agrupando por fase lunar
    y tipo de número para encontrar la frecuencia de cada estrella.
    """
    # Se agrupa y se cuenta la frecuencia de las estrellas.
    frecuencia_por_grupo = df_estrellas.groupby(['fase_lunar', 'tipo_estrella'])['estrella'].value_counts()

    df_frecuencia = frecuencia_por_grupo.to_frame(name='frecuencia').reset_index()
    df_frecuencia = df_frecuencia.sort_values(by=['fase_lunar', 'tipo_estrella', 'frecuencia'],
                                              ascending=[True, True, False])

    # Limita los resultados a los primeros Q_RESULTADOS de cada grupo.
    # Se usa .groupby().head() para obtener los top N resultados por grupo.
    # top_resultados = df_frecuencia.groupby(['fase_lunar', 'tipo_estrella']).head(Q_RESULTADOS)
    #
    # return top_resultados

    return df_frecuencia


# Función sustituida por transformar_primitiva_a_dataframe que es más potente
def transformar_primitiva_a_tuplas(df_primitiva_frecuencia):
    """
    Transforma el DataFrame de frecuencia de la Primitiva en una lista de tuplas.
    Cada tupla contiene los 6 números y el reintegro más frecuentes.
    """
    # Se filtran los números principales (n1 a n6)
    df_numeros = df_primitiva_frecuencia[
        df_primitiva_frecuencia['tipo_numero'].isin(['n1', 'n2', 'n3', 'n4', 'n5', 'n6'])
    ].copy()

    # Se filtra el reintegro ('re')
    df_reintegro = df_primitiva_frecuencia[
        df_primitiva_frecuencia['tipo_numero'] == 're'
        ].copy()

    # Se calcula el ranking de frecuencia para los números y el reintegro
    df_numeros['rank'] = df_numeros.groupby(['fase_lunar', 'tipo_numero'])['frecuencia'].rank(method='first',
                                                                                              ascending=False)
    df_reintegro['rank'] = df_reintegro.groupby(['fase_lunar', 'tipo_numero'])['frecuencia'].rank(method='first',
                                                                                                  ascending=False)

    tuplas_por_fase = {}

    # Se itera por cada fase lunar
    for fase in df_primitiva_frecuencia['fase_lunar'].unique():
        tuplas_fase = []
        # Se itera para cada uno de los 10 rankings de frecuencia
        for rank in range(1, Q_RESULTADOS + 1):
            # Se obtienen los 6 números y el reintegro con el mismo ranking de frecuencia
            numeros_rank = df_numeros[
                (df_numeros['fase_lunar'] == fase) & (df_numeros['rank'] == rank)
                ]['numero'].tolist()
            reintegro_rank = df_reintegro[
                (df_reintegro['fase_lunar'] == fase) & (df_reintegro['rank'] == rank)
                ]['numero'].iloc[0] if not df_reintegro[
                (df_reintegro['fase_lunar'] == fase) & (df_reintegro['rank'] == rank)
                ].empty else None

            # Se crea la tupla y se añade a la lista
            if reintegro_rank is not None:
                tuplas_fase.append((tuple(numeros_rank), reintegro_rank))

        tuplas_por_fase[fase] = tuplas_fase

    return tuplas_por_fase

# Función que sustituye a transformar_primitiva_a_tuplas por ofrecer más opciones
def transformar_primitiva_a_dataframe(df_primitiva_frecuencia):
    """
    Transforma el DataFrame de frecuencia de la Primitiva en un nuevo DataFrame
    con números únicos en cada combinación, respetando el ranking y las columnas originales.
    """
    # Asegúrate de que los números sean enteros.
    # df_primitiva_frecuencia['numero'] = df_primitiva_frecuencia['numero'].astype(int)

    dfs_por_fase = {}

    for fase in df_primitiva_frecuencia['fase_lunar'].unique():
        fase_data = []

        # Crear un diccionario de DataFrames para cada tipo de número (n1 a n6 y re), ordenados por frecuencia.
        ranked_lists = {}
        for tipo in ['n1', 'n2', 'n3', 'n4', 'n5', 'n6', 're']:
            ranked_lists[tipo] = df_primitiva_frecuencia[
                (df_primitiva_frecuencia['fase_lunar'] == fase) &
                (df_primitiva_frecuencia['tipo_numero'] == tipo)
                ].sort_values(by='frecuencia', ascending=False)
            ranked_lists[tipo] = ranked_lists[tipo]['numero'].tolist()

        for i in range(Q_RESULTADOS):
            combination = {'fase_lunar': fase, 'rank': i + 1}
            selected_numeros = []

            # Recorrer cada tipo de número (n1 a n6)
            for j in range(1, 7):
                num_type = f'n{j}'

                # Encontrar el primer número disponible en el ranking que no esté en la combinación.
                num = None
                rank_idx = i
                while rank_idx < len(ranked_lists[num_type]):
                    potential_num = ranked_lists[num_type][rank_idx]
                    if potential_num not in selected_numeros:
                        num = potential_num
                        break
                    rank_idx += 1

                # Si no se encuentra un número único, se interrumpe la combinación.
                if num is None:
                    selected_numeros = []
                    break

                selected_numeros.append(num)
                combination[num_type] = num

            # Si se encontraron 6 números únicos, se añade el reintegro.
            if len(selected_numeros) == 6:
                if i < len(ranked_lists['re']):
                    combination['reintegro'] = ranked_lists['re'][i]
                    fase_data.append(combination)

        if fase_data:
            dfs_por_fase[fase] = pd.DataFrame(fase_data)

    if not dfs_por_fase:
        return pd.DataFrame(columns=['fase_lunar', 'rank', 'n1', 'n2', 'n3', 'n4', 'n5', 'n6', 'reintegro'])

    return pd.concat(dfs_por_fase.values(), ignore_index=True)


# Función sustituida por transformar_euromillo_a_dataframe que es más potente
def transformar_euromillon_a_tuplas(df_euromillon_frecuencia_numeros, df_euromillon_frecuencia_estrellas):
    """
    Transforma el DataFrame de frecuencia del Euromillón en una lista de tuplas,
    donde cada tupla contiene los 5 números y las 2 estrellas más frecuentes,
    ordenados por su ranking de frecuencia.
    """

    # Se filtran los números principales
    df_numeros = df_euromillon_frecuencia_numeros[
        df_euromillon_frecuencia_numeros['tipo_numero'].isin(['n1', 'n2', 'n3', 'n4', 'n5'])
    ].copy()

    # Se filtran las estrellas
    df_estrellas = df_euromillon_frecuencia_estrellas[
        df_euromillon_frecuencia_estrellas['tipo_estrella'].isin(['e1', 'e2'])
    ].copy()

    # Se calcula el ranking de frecuencia para los números y las estrellas
    df_numeros['rank'] = df_numeros.groupby(['fase_lunar', 'tipo_numero'])['frecuencia'].rank(method='first',
                                                                                              ascending=False)
    df_estrellas['rank'] = df_estrellas.groupby(['fase_lunar', 'tipo_estrella'])['frecuencia'].rank(method='first',
                                                                                                  ascending=False)
    tuplas_por_fase = {}

    # Se itera por cada fase lunar
    for fase in df_euromillon_frecuencia_numeros['fase_lunar'].unique():
        tuplas_fase = []
        # Se itera para cada uno de los 10 rankings
        for rank in range(1, Q_RESULTADOS + 1):
            # Se obtienen los 5 números y las 2 estrellas con el mismo ranking de frecuencia
            numeros_rank = df_numeros[
                (df_numeros['fase_lunar'] == fase) & (df_numeros['rank'] == rank)
                ]['numero'].tolist()
            estrellas_rank = df_estrellas[
                (df_estrellas['fase_lunar'] == fase) & (df_estrellas['rank'] == rank)
                ]['estrella'].tolist()

            # Se crea la tupla y se añade a la lista
            if numeros_rank and estrellas_rank:
                tuplas_fase.append((tuple(numeros_rank), tuple(estrellas_rank)))

        tuplas_por_fase[fase] = tuplas_fase

    return tuplas_por_fase


# Función que sustituye a transformar_euromillon_a_tuplas por ofrecer más opciones
def transformar_euromillon_a_dataframe(df_euromillon_frecuencia):
    """
    Transforma el DataFrame de frecuencia del Euromillones en un nuevo DataFrame
    con números y estrellas únicos en cada combinación, respetando el ranking.
    """
    # Convertir los números a enteros.
    # df_euromillon_frecuencia['numero'] = df_euromillon_frecuencia['numero'].astype(int)

    dfs_por_fase = {}

    for fase in df_euromillon_frecuencia['fase_lunar'].unique():
        fase_data = []

        # Crear un diccionario de DataFrames para cada tipo de número y estrella.
        ranked_lists = {}
        for tipo in ['n1', 'n2', 'n3', 'n4', 'n5', 'e1', 'e2']:
            ranked_lists[tipo] = df_euromillon_frecuencia[
                (df_euromillon_frecuencia['fase_lunar'] == fase) &
                (df_euromillon_frecuencia['tipo_numero'] == tipo)
                ].sort_values(by='frecuencia', ascending=False)
            ranked_lists[tipo] = ranked_lists[tipo]['numero'].tolist()

        for i in range(Q_RESULTADOS):
            combination = {'fase_lunar': fase, 'rank': i + 1}
            selected_numeros = []
            selected_estrellas = []

            # Seleccionar los 5 números principales.
            for j in range(1, 6):
                num_type = f'n{j}'
                num = None
                rank_idx = i
                while rank_idx < len(ranked_lists[num_type]):
                    potential_num = ranked_lists[num_type][rank_idx]
                    if potential_num not in selected_numeros:
                        num = potential_num
                        break
                    rank_idx += 1

                if num is None:
                    selected_numeros = []
                    break

                selected_numeros.append(num)
                combination[num_type] = num

            # Seleccionar las 2 estrellas.
            if len(selected_numeros) == 5:
                for k in range(1, 3):
                    star_type = f'e{k}'
                    star = None
                    rank_idx = i
                    while rank_idx < len(ranked_lists[star_type]):
                        potential_star = ranked_lists[star_type][rank_idx]
                        if potential_star not in selected_estrellas:
                            star = potential_star
                            break
                        rank_idx += 1

                    if star is None:
                        selected_estrellas = []
                        break

                    selected_estrellas.append(star)
                    combination[star_type] = star

            # Si se completó la combinación, añadirla.
            if len(selected_numeros) == 5 and len(selected_estrellas) == 2:
                fase_data.append(combination)

        if fase_data:
            dfs_por_fase[fase] = pd.DataFrame(fase_data)

    if not dfs_por_fase:
        return pd.DataFrame(columns=['fase_lunar', 'rank', 'n1', 'n2', 'n3', 'n4', 'n5', 'e1', 'e2'])

    return pd.concat(dfs_por_fase.values(), ignore_index=True)


def mostrar_combinaciones_por_dia(df, dia, cantidad):
    """
    Filtra un DataFrame para mostrar las combinaciones de un sorteo,
    identificando si es Primitiva o Euromillones, y selecciona las columnas
    relevantes para su visualización.

    Args:
        df (pd.DataFrame): El DataFrame completo con todas las combinaciones.
        dia (int): el dia de la semana para el que se quieren obtener las combinaciones
        cantidad (int): El número de combinaciones a mostrar.

    Returns:
        pd.DataFrame: Un nuevo DataFrame con los resultados filtrados y seleccionados.
    """
    this_week_moon_phases = get_whole_week_moon_phase()
    fase = this_week_moon_phases.get(dia)
    print(f"\nFase lunar {fase}")

    df_filtrado = df[df['fase_lunar'] == fase].head(cantidad)

    # Comprobamos las columnas para determinar el tipo de sorteo
    columnas_sorteo = []
    if 'reintegro' in df_filtrado.columns:
        # Es Primitiva
        columnas_sorteo = ['n1', 'n2', 'n3', 'n4', 'n5', 'n6', 'reintegro']
    elif 'e1' in df_filtrado.columns:
        # Es Euromillones
        columnas_sorteo = ['n1', 'n2', 'n3', 'n4', 'n5', 'e1', 'e2']
    else:
        # En caso de que no se reconozca el sorteo, devolvemos un DataFrame vacío
        print("Error: No se puede determinar el tipo de sorteo.")
        return pd.DataFrame()

    df_seleccion = df_filtrado[columnas_sorteo]

    return df_seleccion
