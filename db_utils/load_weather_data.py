from datetime import date

from other_utils.humidity_meteostat import get_daily_atmospheric_state


hoy = date.today()

print(get_daily_atmospheric_state(hoy,"PARIS"))