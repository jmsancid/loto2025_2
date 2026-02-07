# weather_forecast.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Iterable, TypedDict
import requests
from datetime import datetime
from math import exp

from other_utils.humidity_meteostat import CityCfg  #, CITY

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"


@dataclass(frozen=True)
class Location:
    name: str
    country: str
    latitude: float
    longitude: float
    timezone: str


@dataclass(frozen=True)
class WindowDailyAvg:
    date: str                 # YYYY-MM-DD
    hours_used: list[int]     # horas incluidas (p.ej. [18,19,20,21,22])
    temp_mean_c: float | None
    rh_mean_pct: float | None
    n_samples: int


class _DayBucket(TypedDict):
    temp: list[float]
    rh: list[float]
    hours: set[int]


def _mean(values: Iterable[float]) -> float | None:
    vals = [v for v in values if v is not None]
    return round((sum(vals) / len(vals)), 2) if vals else None


def fetch_hourly_temp_rh(city: CityCfg, *, days: int = 7) -> dict:
    """
    Devuelve previsión horaria (arrays paralelos).
    Útil si luego tú agregas o consumes por horas.
    """
    params = {
        "latitude": city.lat,
        "longitude": city.lon,
        "timezone": city.timezone,
        "forecast_days": days,
        "hourly": "temperature_2m,relative_humidity_2m",
    }
    r = requests.get(FORECAST_URL, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()

    hourly = data.get("hourly") or {}
    return {
        "time": hourly.get("time", []),
        "temperature_2m": hourly.get("temperature_2m", []),
        "relative_humidity_2m": hourly.get("relative_humidity_2m", []),
    }


def daily_window_means_from_hourly(
    *,
    time: list[str],
    temperature_2m: list[float],
    relative_humidity_2m: list[float],
    start_hour: int = 18,
    end_hour: int = 23,
) -> list[WindowDailyAvg]:
    """
    Agrega por día la media de temperatura y humedad relativa en la ventana horaria [start_hour, end_hour].
    Se asume que 'time' viene en hora local si has llamado a Open-Meteo con timezone=<zona>.
    Formato esperado en 'time': 'YYYY-MM-DDTHH:MM' (Open-Meteo típico).

    Devuelve una lista ordenada por fecha.
    """
    if not (len(time) == len(temperature_2m) == len(relative_humidity_2m)):
        raise ValueError("Arrays hourly desalineados")

    buckets: dict[str, _DayBucket] = {}

    for t, temp, rh in zip(time, temperature_2m, relative_humidity_2m):
        dt = datetime.fromisoformat(t)
        h = dt.hour

        if start_hour <= h <= end_hour:
            d = dt.date().isoformat()

            if d not in buckets:
                buckets[d] = {
                    "temp": [],
                    "rh": [],
                    "hours": set(),
                }

            buckets[d]["temp"].append(temp)
            buckets[d]["rh"].append(rh)
            buckets[d]["hours"].add(h)

    out: list[WindowDailyAvg] = []

    for d in sorted(buckets.keys()):
        bucket = buckets[d]

        hours: list[int] = sorted(bucket["hours"])

        out.append(
            WindowDailyAvg(
                date=d,
                hours_used=hours,                 # ✔ list[int]
                temp_mean_c=_mean(bucket["temp"]),
                rh_mean_pct=_mean(bucket["rh"]),
                n_samples=len(hours),
            )
        )

    return out


def fetch_window_daily_means(city: CityCfg, *, days: int = 7, start_hour: int = 18, end_hour: int = 23):
    hourly = fetch_hourly_temp_rh(city, days=days)
    return daily_window_means_from_hourly(
        time=hourly["time"],
        temperature_2m=hourly["temperature_2m"],
        relative_humidity_2m=hourly["relative_humidity_2m"],
        start_hour=start_hour,
        end_hour=end_hour,
    )


def calc_abs_humidity(temp_c: float, rh_pct: float) -> float:
    """
    Humedad absoluta (g/m³) a partir de T (°C) y RH (%)
    Fórmula Magnus–Tetens
    """

    if rh_pct < 0 or rh_pct > 100:
        raise ValueError(f"RH fuera de rango: {rh_pct}")
    if temp_c < -80 or temp_c > 60:
        raise ValueError(f"Temperatura sospechosa: {temp_c}")

    es = 6.112 * exp((17.67 * temp_c) / (temp_c + 243.5))
    e = (rh_pct / 100.0) * es
    return 216.7 * e / (temp_c + 273.15)
