
import re
from datetime import datetime, date

import requests
from bs4 import BeautifulSoup

import constants as cte
from other_utils.date_utils import procesa_fecha


def filtra_combinaciones_nuevas(combinaciones, last_date):
    if last_date is None:
        return combinaciones
    return {d: vals for d, vals in combinaciones.items() if d > last_date}


def getEuroLatestResults():
    """
    Devuelve un diccionario con los últimos resultados de euromillones.
    :return: dict[datetime.date, list[int]]
    """

    response = requests.get(cte.EUROWEB)
    # response = requests.get(lotoparams.EUROWEB)
    # soup = BeautifulSoup(response.text, 'lxml')
    soup = BeautifulSoup(response.text, 'lxml')
    # print(soup.title.text)
    # print(soup.prettify())
    # Las combinaciones se encuentran dentro del bloque h4
    h4_sorteos = soup.find_all('h4', string=re.compile('^Euromillones'))
    print('**************   COMBINACIONES EUROMILLONES       ****************')
    i = 0
    combinacionesExtraidas = {}
    for h4 in h4_sorteos:
        # Extraer la fecha del texto de h4 y convertirla a formato AAAAMMDD
        texto_h4 = h4.get_text(strip=True)
        fecha = procesa_fecha(texto_h4)
        # Buscar los números y estrellas dentro del mismo bloque de sorteo
        bloque_sorteo = h4.parent
        numeros = [int(n.get_text(strip=True)) for n in bloque_sorteo.find_all('li', class_='numeros')]
        estrellas = [int(e.get_text(strip=True)) for e in bloque_sorteo.find_all('li', class_='estrellas')]

        # Combinar en una tupla y añadir al diccionario
        combinacionesExtraidas[fecha] = numeros + estrellas
        # print(fecha, combinacionesExtraidas[fecha])

    return combinacionesExtraidas


def getPrimiLatestResults(fecha_inicial: date | None = None) -> dict[date, list[int]]:
    '''
    Devuelve un diccionario con los resultados de primitiva del último mes, siendo la clave una cadena
    con la fecha y el valor una lista con los números extraídos.
    La fecha final corresponde al lunes de la semana siguiente a la actual y la fecha inicial a la del lunes de
    5 semanas atrás
    :return: diccionario {fecha: [num1, num2, num3, num4, num5, num6, comp, re]}
            1 si ha habido algún error
    '''
    # En 2025 he tenido que cambiar la forma de extraer los números de la primitiva, utilizando un script que
    # encontré mientras inspeccionaba la web de primitivas y que se llama buscadorSorteos
    # next_monday = datetime.now() + timedelta(days=8-datetime.now().isoweekday())
    # four_mondays_ago = next_monday + timedelta(weeks=-4)


    # url = 'https://www.loteriasyapuestas.es/servicios/buscadorSorteos'
    headers = {
        'Host': 'www.loteriasyapuestas.es',
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:135.0) Gecko/20100101 Firefox/135.0',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'X-Requested-With': 'XMLHttpRequest',
        'Alt-Used': 'www.loteriasyapuestas.es',
        'Connection': 'keep-alive',
        'Referer': 'https://www.loteriasyapuestas.es/es/resultados/primitiva',
        'Cookie': 'usr-lang=es; UUID=WEB-b5034850-cfee-4bc8-9c03-3f7c2c7e4179; '
                  'CookieConsent={stamp:%271UCnsPBJe8OGTadACzKrwaj8WPRPRWzQh+AUa9ZQnF8dtK0egRLfxQ=='
                  '%27%2Cnecessary:true%2Cpreferences:true%2Cstatistics:true%2Cmarketing:true%2Cmethod:'
                  '%27explicit%27%2Cver:1%2Cutc:1740931548034%2Cregion:%27es%27}'
    }

    hoy = date.today()
    if fecha_inicial is None:
        fecha_inicio_str = f"{hoy.year}0101"
    else:
        # sumas un día para no repetir el último ya guardado
        fecha_inicio_str = (fecha_inicial).strftime("%Y%m%d")

    params = {
        'game_id': 'LAPR',
        'celebrados': 'true',
        'fechaInicioInclusiva': fecha_inicio_str,
        'fechaFinInclusiva': f"{datetime.now().year}{datetime.now().month:02d}{datetime.now().day:02d}"
    }

    response = requests.get(cte.PRIMIWEB, headers=headers, params=params)

    if response.status_code != 200:
        print(f"Error accediendo a la web {cte.PRIMIWEB}\n Código de Error: {response.status_code}")
        return 1
    print('++++++++++++++   COMBINACIONES PRIMITIVA +++++++++++++++++')
    #
    sorteos = response.json()  # En 2025, la web de primitivas devuelve un json con los resultados.
    combinaciones_extraidas = {}
    for sorteo in sorteos:
        # Extraigo la fecha y la convierto en formato datetime.date
        fecha_sorteo = datetime.strptime(
            sorteo.get("fecha_sorteo"), '%Y-%m-%d %H:%M:%S'
        ).date()
        # fecha = datetime(fecha_sorteo.year, fecha_sorteo.month,fecha_sorteo.day).date()

        # if fecha is None:
        #   continue
        # str_comb = sorteo.get("combinacion")
        # combinaciones_extraidas[fecha] = list(map(int, re.findall(r'\d+', str_comb)))

        nums = list(map(int, re.findall(r"\d+", sorteo["combinacion"])))
        combinaciones_extraidas[fecha_sorteo] = nums


    return combinaciones_extraidas


