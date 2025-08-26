#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import sqlite3

class DBManager:
    """Clase para gestionar todas las consultas a la base de datos."""

    def __init__(self, db_path):
        """Inicializa el gestor con la ruta a la base de datos."""
        self.db_path = db_path
        self.conn = None

    def __enter__(self):
        # Este método se ejecuta al entrar en el bloque 'with'
        self.conn = sqlite3.connect(self.db_path)
        return self  # Retorna el objeto DBManager para usarlo dentro del bloque

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Este método se ejecuta al salir del bloque 'with'
        # Se asegura de que la conexión se cierre
        if self.conn:
            self.conn.close()
        # Si la conexión se cierra, el retorno 'None' no suprime la excepción
        return False

    def _ejecutar_consulta(self, query, params=()):
        """
        Método privado para ejecutar una consulta genérica.
        Utiliza un gestor de contexto para la conexión.
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                return cursor.fetchall()
        except sqlite3.Error as e:
            print(f"Error de base de datos: {e}")
            return None

    def _ejecutar_modificacion(self, query, params=()):
        """
        Método privado para ejecutar consultas de modificación (INSERT, UPDATE, DELETE).
        Asegura que los cambios se confirmen.
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                conn.commit()
                return True
        except sqlite3.Error as e:
            print(f"Error al modificar la base de datos: {e}")
            return False


    def fecha_ultimo_resultado(self, nombre_tabla, nombre_columna_fecha):
        """
        Obtiene la fecha más reciente de una tabla específica.
        """
        query = f"SELECT MAX({nombre_columna_fecha}) FROM {nombre_tabla}"
        resultado = self._ejecutar_consulta(query)
        if resultado and resultado[0]:
            return resultado[0][0]
        return None

    def obtener_valores_por_fecha(self, nombre_tabla, fecha, campos):
        """
        Obtiene los valores de campos específicos para una fecha dada.
        'campos' debe ser una lista de nombres de columnas.
        """
        campos_str = ", ".join(campos)
        query = f"SELECT {campos_str} FROM {nombre_tabla} WHERE fecha = ?"
        return self._ejecutar_consulta(query, (fecha,))


    def insertar_registros(self, nombre_tabla:str, combinaciones:list) -> bool:
        """
        Inserta uno o varios registros en la tabla especificada.

        Args:
            nombre_tabla (str): El nombre de la tabla, Primitiva o Euromillones.
            combinaciones (list): lista de diccionarios con la misma estructura de la tabla en la que se van a insertar
            los datos: fecha, n1..n5 + estrellas 1 y 2 de euromillones o n6 complementario y reintegro
            en primitiva

        Returns:
            bool: True si la inserción fue exitosa, False en caso contrario.
        """

        if not combinaciones:
            print("Error: El diccionario de datos está vacío. No se ha insertado nada.")
            return False

        # Las columnas se construirán a partir de las claves del diccionario
        # Suponemos que la primera columna es la fecha y el resto son los datos numéricos.
        # Si tienes nombres de columnas fijos, puedes usar una constante como discutimos antes.

        # Asumimos que todos los diccionarios tienen las mismas claves
        columnas = ', '.join(combinaciones[0].keys())
        placeholders = ', '.join(['?'] * len(combinaciones[0]))

        # Preparamos la lista de tuplas con los valores a insertar
        lista_de_valores = [tuple(registro.values()) for registro in combinaciones]

        # Construimos la consulta SQL
        query = f"INSERT INTO {nombre_tabla} ({columnas}) VALUES ({placeholders})"

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.executemany(query, lista_de_valores)
                conn.commit()
                return True
        except sqlite3.Error as e:
            print(f"Error al insertar registros: {e}")
            return False


    # Puedes agregar más métodos para otras operaciones, como:
    # def obtener_promedio_valores(self, nombre_tabla, campo_numerico):
    #     ...