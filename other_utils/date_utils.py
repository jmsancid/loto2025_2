import re
from datetime import datetime, date, timedelta

# from astral import moon


# def obtener_valor_fase_lunar(fecha: datetime) -> float:
#     """
#     Devuelve el valor la fase lunar a partir de una fecha dada
#
#     Args:
#         fecha (datetime): La fecha para la cual se desea obtener la fase lunar.
#
#     Returns:
#         str: El nombre de la fase lunar.
#     """
#     moon_phase = moon.phase(fecha)
#
#     return moon_phase
#
#
# def obtener_fase_lunar(fecha: datetime) -> str:
#     """
#     Devuelve la fase lunar a partir de una fecha dada
#
#     Args:
#         fecha (datetime): La fecha para la cual se desea obtener la fase lunar.
#
#     Returns:
#         str: El nombre de la fase lunar.
#     """
#     moon_phase = moon.phase(fecha)
#
#
#     # Convertir la elongación en un nombre de fase
#     if 0.0 <= moon_phase < 7.0:
#         return "Luna Nueva"
#     elif 7.0 <= moon_phase < 14.0:
#         return "Cuarto Creciente"
#     elif 14.0 <= moon_phase < 21.0:
#         return "Luna Llena"
#     elif 21.0 <= moon_phase < 28.0:
#         return "Cuarto Menguante"
#     else:  # Valor inesperado de moon.phase
#         print(f"Error. No se ha podido obtener la fase de la luna. Moon.phase{fecha}: {moon_phase}")
#         raise ValueError


def procesa_fecha(fecha: str) -> date | None:
    """
    Versión 2025 con regular expressions
    procesa la fecha de un determinado sorteo, extraída como string de la web de euromillones
    y la convierte en un objeto Datetime
    :param fecha: en formato str AAAA-MM-DD
    :return: fecha en formato datetime.date
    """
    match = re.search(r'(\d{1,2}) de (\w+) de (\d{4})', fecha)

    if match:
        dia_str, mes_str, ano_str = match.groups()

        # Mapear nombres de meses a números
        meses_map = {
            'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
            'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
            'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
        }
        mes_num = meses_map.get(mes_str.lower())

        if mes_num:
            # Construir la fecha completa y el formato de salida
            anno = int(ano_str)
            mes = mes_num
            dia = int(dia_str)
            fecha = datetime(anno, mes, dia).date()

    return fecha


# def get_whole_week_moon_phase(week_number:None | int=None, year:None | int=None) -> dict:
#     """
#     Devuelve un diccionario con la fase lunar de cada día de la semana correspondiente al año y número de semana
#     indicados. Si no se especifica el año, se toma el año actual. Lo mismo para la semana
#     :param year: año de cuya semana se quieren obtener las fases lunares
#     :param week_number: número de semana anual para obtener las fases lunares
#     :return: diccionario con la fase lunar de cada día de la semana indicada para el año solicitado. Las claves
#     son integer del 1 al 7, siendo 1 el lunes y 7 el domingo. Las fases lunares pueden ser luna nueva,
#     cuarto creciente, luna llena y cuarto menguante
#     """
#
#     this_year, this_week_number, this_week_day = datetime.now().isocalendar()
#
#     year = year if year else this_year
#     week_number = week_number if week_number else this_week_number
#
#     # Crear un objeto de fecha para el lunes (día 1) de la semana
#     monday = datetime.fromisocalendar(year, week_number, 1)
#
#     # Generar la lista de 7 días
#     week_dates = [monday + timedelta(days=i) for i in range(7)]
#
#     # Genero el diccionario con las fases lunares de cada día
#
#     week_moon_phases = {
#         i + 1: obtener_fase_lunar(week_dates[i]) for i in range(7)
#     }
#
#     return week_moon_phases
#

def fecha_dia_semana_sorteo(dia_sem_obj: int, hoy: date | None = None) -> str:
    """
    Devuelve la fecha (AAAA-MM-DD) del día de semana dia_sem_obj (1=lunes..7=domingo)
    según la regla:
      - si hoy es domingo -> semana próxima
      - si hoy no es domingo -> semana actual
    """
    if hoy is None:
        hoy = date.today()

    wd = hoy.weekday()  # date.weekday() devuelve: lunes=0 … domingo=6

    # Lunes de la semana actual (ISO-like, lunes como inicio)
    lunes_semana_actual = hoy - timedelta(days=wd)

    # Si hoy es domingo (6), saltamos a la semana siguiente
    if wd == 6:
        lunes_referencia = lunes_semana_actual + timedelta(days=7)
    else:
        lunes_referencia = lunes_semana_actual

    fecha_sorteo = lunes_referencia + timedelta(days=dia_sem_obj - 1)
    return fecha_sorteo.isoformat()  # isoformat es '2025-12-30T11:44:34.426618'

