#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# cur_year = str(datetime.now().year)  # A�o actual para descargar las combinaciones de primitiva
PRIMIFIELDS = ('idx', 'fecha', 'n1', 'n2', 'n3', 'n4', 'n5', 'n6', 'compl', 're')  # campos de la db de primitivas
EUROFIELDS = ('idx', 'fecha', 'n1', 'n2', 'n3', 'n4', 'n5', 'e1', 'e2')  # campos de la db de euromillones
PRIMITIVA = 'Primitiva'  # Nombre de la db con todas las combinaciones hist�ricas de Primitiva
SELPRIMI = 'SelPrimi'  # Nombre de la db con todas las apuestas de Primitiva, identificadas por jueves y s�bado
SELPRIMITOT = 'SelPrimiTot'  # Nombre de la db con todas las apuestas de Primitiva considerando todos los n�meros,
# sin separar jueves y s�bados y ordenados por semanas
PREMIADOSPRIMI = 'PremiadosPrimi'  # Nombre de la db con todas las apuestas de Primitiva que coinciden
# con alguna premiada anteriormente
EUROMILLONES = 'Euromillones'  # Nombre de la db con todas las combinaciones hist�ricas de Euromillones,
# identificados par martes y viernes
SELEURO = 'SelEuro'  # Nombre de la db con todas las apuestas de Euromillones considerando todos los n�meros,
# sin separar martes y viernes y ordenados por semanas
SELEUROTOT = 'SelEuroTot'  # Nombre de la db con todas las apuestas de Euromillones
PREMIADOSEURO = 'PremiadosEuro'  # Nombre de la db con todas las apuestas de Euromillones que coinciden
# con alguna premiada anteriormente
# DBDIR = r'/./'
DBFILE = r'./loterias.db'
PICKLEDIR = r'/home/chema/PycharmProjects/loterias/pickleFiles/'
LOTOPICKERFILE = 'loterias.pkl' # contiene un diccionario con los tipos de sorte como clave y las listas de
# combinaciones como valor
EUROWEB = 'https://www.euromillones.com.es/resultados-anteriores.html'
PRIMIWEB = 'https://www.loteriasyapuestas.es/servicios/buscadorSorteos'  # Actualizaci�n marzo 2025
PRIMIDAYS = (46, 1, 4, 6)  # D�as de primitiva. Domingo es d�a 0. Primitiva es lunes, jueves y s�bado: 1, 4 y 6. El 46
# representa el total de sorteos de primitiva de jueves y s�bado
EURODAYS = (25, 2, 5)  # D�as de euromillones. Domingo es d�a 0. Euromilones es martes y viernes: 2 y 5. El 25
# representa el total de sorteos de euromillones de martes y viernes
PRIMINUMBERS = 6  # Cantidad de numeros que forman una apuesta de primitiva, sin contar complementario y reintegro
EURONUMBERS = 5  # Cantidad de numeros que forman una apuesta de euromillones, sin contar las estrellas
PRIMI_Q_BETS = 5  # N� de apuestas de primitiva
EURO_Q_BETS = 5  # N� de apuestas de euromillones
Q_NUM_MAS_FREQ = 10  # Cantidad de n�meros seleccionados en la lista de m�s frecuentes
EVEN    = 'e'
ODD     = 'o'
LUNES       = 1
MARTES      = 2
MIERCOLES   = 3
JUEVES      = 4
VIERNES     = 5
SABADO      = 6
DOMINGO     = 7
PRIMAVERA   = 1
VERANO      = 2
OTONO       = 3
INVIERNO    = 4
ESTACIONES  = {1: 'PRIMAVERA',
               2: 'VERANO',
               3: 'OTOÑO',
               4: 'INVIERNO'}
Q_RESULTADOS = 5
