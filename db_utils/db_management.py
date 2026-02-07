#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from __future__ import annotations
import sqlite3
from datetime import datetime, date
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple, Optional, TYPE_CHECKING

from other_utils.fase_lunar import obtener_fase_lunar, obtener_valor_fase_lunar
from other_utils.humidity_meteostat import get_daily_atmospheric_state, City

from db_utils.santi_rows import santi_primitiva_row, santi_euromillones_row


if TYPE_CHECKING:
    from other_utils.ranking_semanal import Apuesta_Primitiva, Apuesta_Euromillones


@dataclass(frozen=True)
class HistRowPrimitiva:
    n: Tuple[int, int, int, int, int, int]
    re: Optional[int]
    temp: float
    rh: float
    ah: float
    moon_val: float


@dataclass(frozen=True)
class HistRowEuro:
    n: Tuple[int, int, int, int, int]
    e: Tuple[int, int]
    temp: float
    rh: float
    ah: float
    moon_val: float


def _to_date(value: Any) -> date:
    """
    Convierte un valor 'fecha' leído de SQLite a datetime.date.
    Soporta:
      - 'YYYY-MM-DD' (str)
      - datetime/date
      - 'YYYY-MM-DD HH:MM:SS' (str) (por si alguna vez se coló)
    """
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        v = value.strip()
        # ISO date
        if len(v) >= 10:
            try:
                return datetime.strptime(v[:10], "%Y-%m-%d").date()
            except ValueError:
                pass
    raise ValueError(f"Formato de fecha no soportado: {value!r}")


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
        self.conn = None  # <- CRÍTICO
        # Si la conexión se cierra, el retorno 'None' no suprime la excepción
        return False

    def _get_conn(self) -> sqlite3.Connection:
        """
        Devuelve una conexión abierta.
        - Si estamos dentro de 'with DBManager(...)', usa self.conn
        - Si self.conn está cerrada por cualquier motivo, la reabre
        """
        if self.conn is not None:
            try:
                # test barato: si está cerrada, esto lanza ProgrammingError
                self.conn.execute("SELECT 1")
                return self.conn
            except sqlite3.ProgrammingError:
                # estaba cerrada: la “olvidamos” y abrimos una nueva
                self.conn = None

        return sqlite3.connect(self.db_path)

    def _ejecutar_consulta(self, query, params=()):
        conn = None
        conn_local = False
        try:
            conn_local = (self.conn is None)
            conn = self._get_conn()
            cur = conn.cursor()
            cur.execute(query, params)
            return cur.fetchall()
        except sqlite3.Error as e:
            print(f"Error ejecutando consulta de base de datos: {e}")
            return None
        finally:
            if conn_local and conn is not None:
                conn.close()

    def _ejecutar_modificacion(self, query, params=()):
        conn = None
        conn_local = False
        try:
            conn_local = (self.conn is None)
            conn = self._get_conn()
            cur = conn.cursor()
            cur.execute(query, params)
            conn.commit()
            return True
        except sqlite3.Error as e:
            print(f"Error al modificar la base de datos: {e}")
            return False
        finally:
            if conn_local and conn is not None:
                conn.close()

    def _ejecutar_many(self, query, values):
        conn = None
        conn_local = False
        try:
            conn_local = (self.conn is None)
            conn = self._get_conn()
            cur = conn.cursor()
            cur.executemany(query, values)
            conn.commit()
            return True
        except sqlite3.Error as e:
            print(f"Error al ejecutar many: {e}")
            return False
        finally:
            if conn_local and conn is not None:
                conn.close()

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

    def insertar_registros(self, nombre_tabla: str, combinaciones: List[Dict[str, Any]]) -> bool:
        """
        Inserta o actualiza (UPSERT) uno o varios registros en la tabla especificada.
        Requiere un UNIQUE INDEX sobre (fecha) en la tabla destino.

        Args:
            nombre_tabla: 'Primitiva' o 'Euromillones'
            combinaciones: lista de dicts con claves que incluyen 'fecha' y el resto de columnas numéricas

        Returns:
            True si OK, False si error
        """
        if not combinaciones:
            print("Error: La lista de datos está vacía. No se ha insertado nada.")
            return False

        # Validación mínima: debe existir la clave 'fecha'
        if "fecha" not in combinaciones[0]:
            print("Error: Falta la clave 'fecha' en los datos.")
            return False

        # Asegurar que todos los dicts tienen las mismas claves (importante para executemany)
        claves = list(combinaciones[0].keys())
        for i, r in enumerate(combinaciones[1:], start=1):
            if list(r.keys()) != claves:
                print(f"Error: Estructura inconsistente en el registro {i}.")
                return False

        columnas = ", ".join(claves)
        placeholders = ", ".join(["?"] * len(claves))

        # Columnas a actualizar: todas excepto 'fecha'
        columnas_update = [c for c in claves if c != "fecha"]
        if not columnas_update:
            print("Error: No hay columnas a actualizar (solo existe 'fecha').")
            return False

        set_clause = ", ".join([f"{c}=excluded.{c}" for c in columnas_update])

        query = f"""
            INSERT INTO {nombre_tabla} ({columnas})
            VALUES ({placeholders})
            ON CONFLICT(fecha) DO UPDATE SET
              {set_clause};
        """

        lista_de_valores = [tuple(registro[c] for c in claves) for registro in combinaciones]

        try:
            return self._ejecutar_many(query, lista_de_valores)
        except sqlite3.Error as e:
            print(f"Error al insertar/actualizar registros en {nombre_tabla}: {e}")
            return False

    # Puedes agregar más métodos para otras operaciones, como:
    # def obtener_promedio_valores(self, nombre_tabla, campo_numerico):
    #     ...

    def obtener_fechas_pendientes_influencers(self) -> List[Tuple[str, date, str]]:
        """
        Devuelve una lista de (juego, fecha_date, ciudad_str) para sorteos que existen en Primitiva/Euromillones
        pero aún no tienen fila en SorteoInfluencers.
        """
        sql = """
        SELECT s.juego, s.fecha, s.ciudad
        FROM (
          SELECT 'Primitiva' AS juego, fecha, 'Madrid' AS ciudad FROM Primitiva
          UNION ALL
          SELECT 'Euromillones' AS juego, fecha, 'Paris'  AS ciudad FROM Euromillones
        ) s
        LEFT JOIN SorteoInfluencers si
          ON si.juego = s.juego AND si.fecha = s.fecha
        WHERE si.fecha IS NULL
        ORDER BY s.fecha;
        """

        pendientes: List[Tuple[str, date, str]] = []
        rows = self._ejecutar_consulta(sql) or []
        for juego, fecha_raw, ciudad in rows:
            pendientes.append((juego, _to_date(fecha_raw), ciudad))
        return pendientes

    def upsert_sorteo_influencers(self, influencers: List[Dict[str, Any]]) -> bool:
        """
        UPSERT en SorteoInfluencers. Requiere PK (juego, fecha).
        """
        if not influencers:
            return True

        claves = list(influencers[0].keys())
        for i, r in enumerate(influencers[1:], start=1):
            if list(r.keys()) != claves:
                print(f"Error: estructura inconsistente en influencers[{i}]")
                return False

        columnas = ", ".join(claves)
        placeholders = ", ".join(["?"] * len(claves))

        # actualizar todo excepto la clave compuesta
        columnas_update = [c for c in claves if c not in ("juego", "fecha")]
        set_clause = ", ".join([f"{c}=excluded.{c}" for c in columnas_update])

        query = f"""
            INSERT INTO SorteoInfluencers ({columnas})
            VALUES ({placeholders})
            ON CONFLICT(juego, fecha) DO UPDATE SET
              {set_clause},
              ingested_at = datetime('now');
        """

        valores = [tuple(r[c] for c in claves) for r in influencers]

        try:
            return self._ejecutar_many(query, valores)
        except sqlite3.Error as e:
            print(f"Error al upsert de SorteoInfluencers: {e}")
            return False

    def sync_sorteo_influencers(
            self,
            station_limit: int = 6,
            batch_size: int = 250,
    ) -> bool:
        """
        Rellena / actualiza SorteoInfluencers para todas las fechas que falten.
        Útil tanto para la carga histórica inicial como para el refresco semanal.

        station_limit: se pasa a get_daily_atmospheric_state()
        batch_size: inserciones por lotes (mejor rendimiento)
        """
        try:
            pendientes = self.obtener_fechas_pendientes_influencers()
        except Exception as e:
            print(f"Error leyendo fechas pendientes: {e}")
            return False

        if not pendientes:
            return True

        # Mapeo ciudad_str -> City del módulo clima (ajusta nombres si difieren)
        city_map: Dict[str, City] = {
            "Madrid": "MADRID",
            "Paris": "PARIS",
        }

        ok_count = 0
        fail_count = 0

        batch: List[Dict[str, Any]] = []
        for juego, d, ciudad_str in pendientes:
            try:
                city = city_map[ciudad_str]

                # Luna: tus funciones aceptan datetime, así que convierto date -> datetime
                dt = datetime(d.year, d.month, d.day)
                luna_phase_value = float(obtener_valor_fase_lunar(dt))
                luna_fase = str(obtener_fase_lunar(dt))

                # Clima: tu función acepta date
                # atmos, station_id = get_daily_atmospheric_state(d=d, city=city, station_limit=station_limit)
                try_limits = (station_limit, max(station_limit, 12), max(station_limit, 24))
                last_err = None
                for lim in try_limits:
                    try:
                        atmos, station_id = get_daily_atmospheric_state(d=d, city=city, station_limit=lim)
                        break
                    except Exception as e:
                        last_err = e
                        # atmos = None
                else:
                    raise last_err  # no se pudo con ningún límite

                if atmos.rh_pct is not None and not (0 <= atmos.rh_pct <= 100):
                    raise ValueError(f"rh_pct fuera de rango: {atmos.rh_pct}")
                if atmos.abs_humidity_g_m3 is not None and atmos.abs_humidity_g_m3 < 0:
                    raise ValueError(f"abs_humidity_g_m3 negativa: {atmos.abs_humidity_g_m3}")

                # Construir fila para SorteoInfluencers.
                # Nota: en SQLite conviene guardar fecha como 'YYYY-MM-DD' (aunque el tipo sea DATE)
                row = {
                    "juego": juego,
                    "fecha": d.isoformat(),
                    "ciudad": ciudad_str,

                    "temp_media": float(atmos.temp_c),
                    "rhum_media": float(atmos.rh_pct),
                    "ahum_media": float(atmos.abs_humidity_g_m3),

                    "luna_phase_value": luna_phase_value,
                    "luna_fase": luna_fase,

                    # Trazabilidad (ajusta si tu módulo da station_id real)
                    "source": "meteostat",
                    "station_id": station_id,
                    "method": "hourly_mean_18_23",
                }

                batch.append(row)
                ok_count += 1

                # UPSERT por lotes
                if len(batch) >= batch_size:
                    if not self.upsert_sorteo_influencers(batch):
                        return False
                    batch.clear()

            except KeyError:
                print(f"Ciudad no soportada: {ciudad_str!r} (juego={juego}, fecha={d})")
                return False
            except Exception as e:
                fail_count += 1
                print(f"[WARN] influencers falló (juego={juego}, fecha={d}, ciudad={ciudad_str}): {e}")
                continue

        # último lote
        if batch:
            if not self.upsert_sorteo_influencers(batch):
                return False
            batch.clear()

        print(f"sync_sorteo_influencers: OK={ok_count}, FAIL={fail_count}")
        return ok_count > 0

    def load_history_primitiva(self) -> List[HistRowPrimitiva]:
        sql = """
        SELECT
          p.n1, p.n2, p.n3, p.n4, p.n5, p.n6,
          p.re,
          si.temp_media, si.rhum_media, si.ahum_media, si.luna_phase_value
        FROM Primitiva p
        JOIN Sorteoinfluencers si
          ON si.fecha = p.fecha AND si.juego = 'Primitiva'
        """
        rows = self._ejecutar_consulta(sql) or []
        out: List[HistRowPrimitiva] = []

        for r in rows:
            n = (int(r[0]), int(r[1]), int(r[2]), int(r[3]), int(r[4]), int(r[5]))
            # n = (n1, n2, n3, n4, n5, n6)
            re_val = r[6]  # índice del reintegro
            re = None if re_val is None else int(re_val)
            temp, rh, ah, moonv = float(r[7]), float(r[8]), float(r[9]), float(r[10])
            out.append(HistRowPrimitiva(n=n, re=re, temp=temp, rh=rh, ah=ah, moon_val=moonv))
        return out

    def load_history_euromillones(self) -> List[HistRowEuro]:
        sql = """
        SELECT
          e.n1, e.n2, e.n3, e.n4, e.n5,
          e.e1, e.e2,
          si.temp_media, si.rhum_media, si.ahum_media, si.luna_phase_value
        FROM Euromillones e
        JOIN Sorteoinfluencers si
          ON si.fecha = e.fecha AND si.juego = 'Euromillones'
        """
        rows = self._ejecutar_consulta(sql) or []
        out: List[HistRowEuro] = []
        for r in rows:
            n = (int(r[0]), int(r[1]), int(r[2]), int(r[3]), int(r[4]))
            stars = (int(r[5]), int(r[6]))
            temp, rh, ah, moonv = float(r[7]), float(r[8]), float(r[9]), float(r[10])
            out.append(HistRowEuro(n=n, e=stars, temp=temp, rh=rh, ah=ah, moon_val=moonv))
        return out

    def upsert_santi_primitiva(
            self,
            apuestas: tuple[tuple[date, Apuesta_Primitiva], ...],
            *,
            week_start: date,
            week_end: date,
            tol_frac: float,
            method_version: str = "v1",
            city: str = "Madrid",
    ) -> bool:

        rows: list[dict[str, Any]] = []

        for d, ap in apuestas:
            combos = list(ap.combinaciones)  # 5 combos

            # combos -> c1..c5, cada uno 6 nums
            c = [tuple(map(int, combo)) for combo in combos]
            if len(c) != 5:
                raise ValueError(f"Apuesta_Primitiva debe tener 5 combinaciones, tiene {len(c)}")

            row = santi_primitiva_row(
                    target_date=d,
                    week_start=week_start,
                    week_end=week_end,
                    apuesta=ap,
                    tol_frac=float(tol_frac),
                    method_version=method_version,
                    city=city,
            )

            rows.append(row)

        if not rows:
            return True

        cols = list(rows[0].keys())
        for r in rows[1:]:
            if list(r.keys()) != cols:
                raise ValueError("Estructura inconsistente en upsert_santi_primitiva")

        # claves críticas siempre presentes
        for i, r in enumerate(rows):
            if not r.get("target_date"):
                raise ValueError(f"rows[{i}] sin target_date")
            if not r.get("signature"):
                raise ValueError(f"rows[{i}] sin signature")

        conflict_cols = "target_date, signature"
        assert conflict_cols == "target_date, signature"

        columns = ", ".join(cols)
        placeholders = ", ".join(["?"] * len(cols))

        # actualiza todo excepto las columnas de la constraint
        # Ajusta el ON CONFLICT a tu índice real:

        update_cols = [c for c in cols if c not in ("target_date", "signature")]
        set_clause = ", ".join([f"{c}=excluded.{c}" for c in update_cols])

        sql = f"""
        INSERT INTO SantiPrimitiva ({columns})
        VALUES ({placeholders})
        ON CONFLICT({conflict_cols}) DO UPDATE SET
          {set_clause},
          generated_at = datetime('now');
        """

        # chequeo
        # required = {"week_start", "week_end", "target_date", "signature", "tol_frac", "method_version", "city", "re"}
        #
        # missing_required = [k for k in required if k not in cols]
        # if missing_required:
        #     raise ValueError(f"Faltan columnas requeridas en rows[0]: {missing_required}. cols={cols}")
        #
        # for i, r in enumerate(rows):
        #     if not r.get("target_date"):
        #         raise ValueError(f"rows[{i}] target_date vacío/None: {r}")
        #     if not r.get("signature"):
        #         raise ValueError(f"rows[{i}] signature vacía/None: {r}")

        # generated_at lo rellena SQLite con DEFAULT datetime('now')
        # notnull_cols = ["week_start", "week_end", "method_version", "tol_frac", "city", "re", "signature",
        #                 "target_date"]
        #
        # for i, r in enumerate(rows):
        #     bad = [c for c in notnull_cols if r.get(c) is None]
        #     if bad:
        #         raise ValueError(f"rows[{i}] tiene NULL en NOT NULL {bad}: {r}")

        # ------------------------------------
        values = [tuple(r[c] for c in cols) for r in rows]
        return self._ejecutar_many(sql, values)

    def upsert_santi_euromillones(
            self,
            apuestas: tuple[tuple[date, Apuesta_Euromillones], ...],
            # apuestas: Iterable[Any],  # Apuesta_Euromillones
            *,
            week_start: date,
            week_end: date,
            tol_frac: float,
            method_version: str = "v1",
            city: str = "Paris",
    ) -> bool:

        rows: list[dict[str, Any]] = []

        for d, ap in apuestas:
            combos = list(ap.combinaciones)  # 2 combos
            if len(combos) != 2:
                raise ValueError(f"Apuesta_Euromillones debe tener 2 combinaciones, tiene {len(combos)}")

            row = santi_euromillones_row(
                    target_date=d,
                    week_start=week_start,
                    week_end=week_end,
                    apuesta=ap,
                    tol_frac=float(tol_frac),
                    method_version=method_version,
                    city=city,
            )

            rows.append(row)

        if not rows:
            return True

        cols = list(rows[0].keys())
        for r in rows[1:]:
            if list(r.keys()) != cols:
                raise ValueError("Estructura inconsistente en upsert_santi_euromillones")

        # claves críticas siempre presentes
        for i, r in enumerate(rows):
            if not r.get("target_date"):
                raise ValueError(f"rows[{i}] sin target_date")
            if not r.get("signature"):
                raise ValueError(f"rows[{i}] sin signature")

        conflict_cols = "target_date, signature"
        assert conflict_cols == "target_date, signature"

        columns = ", ".join(cols)
        placeholders = ", ".join(["?"] * len(cols))

        update_cols = [c for c in cols if c not in ("target_date", "signature")]
        set_clause = ", ".join([f"{c}=excluded.{c}" for c in update_cols])

        sql = f"""
        INSERT INTO SantiEuromillones ({columns})
        VALUES ({placeholders})
        ON CONFLICT({conflict_cols}) DO UPDATE SET
          {set_clause},
          generated_at = datetime('now');
        """

        values = [tuple(r[c] for c in cols) for r in rows]
        return self._ejecutar_many(sql, values)
