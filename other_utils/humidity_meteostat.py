from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Literal #, Optional

import numpy as np
# import pandas as pd
import meteostat as ms


City = Literal["MADRID", "PARIS"]

CITY_STATIONS: dict[City, list[str]] = {
    "MADRID": ["08222", "08221", "LEMM0"],   # principal + fallbacks
    "PARIS":  ["07149", "07147", "07157"],
}

# -----------------------------
# Objeto de retorno
# -----------------------------
@dataclass(frozen=True)
class DailyAtmosphericState:
    temp_c: float              # temperatura media diaria (°C)
    rh_pct: float              # humedad relativa media diaria (%)
    abs_humidity_g_m3: float   # humedad absoluta media diaria (g/m³)


# -----------------------------
# Configuración ciudades
# -----------------------------
@dataclass(frozen=True)
class CityCfg:
    key: str          # id interno estable: "madrid", "paris"
    name: str         # nombre humano: "Madrid", "Paris"
    lat: float
    lon: float
    elev: int
    timezone: str     # IANA tz: "Europe/Madrid", "Europe/Paris"


CITY: dict[City, CityCfg] = {
    "MADRID": CityCfg(
        key="madrid",
        name="Madrid",
        lat=40.4454788,
        lon=-3.7127234,
        elev=694,
        timezone="Europe/Madrid",
    ),
    "PARIS": CityCfg(
        key="paris",
        name="Paris",
        lat=48.8393552,
        lon=2.2399547,
        elev=34,
        timezone="Europe/Paris",
    ),
}


class MeteostatDataError(RuntimeError):
    pass


# -----------------------------
# Utilidades internas
# -----------------------------
def _to_df(x):
    """Si x tiene .fetch() → DataFrame; si ya es DataFrame → se devuelve tal cual."""
    return x.fetch() if hasattr(x, "fetch") else x


def _day_bounds(d: date) -> tuple[datetime, datetime]:
    start = datetime.combine(d, time(18, 0))
    end = datetime.combine(d, time(23, 59))
    return start, end


def _estimate_point_elevation(stations_df) -> float:
    if stations_df is None or len(stations_df) == 0 or "elevation" not in stations_df.columns:
        return 0.0
    e = stations_df["elevation"].dropna()
    return float(np.median(e)) if len(e) else 0.0


def _absolute_humidity_g_m3(temp_c: np.ndarray, rh_pct: np.ndarray) -> np.ndarray:
    """
    Humedad absoluta (g/m³) a partir de T (°C) y RH (%)
    Fórmula Magnus–Tetens
    """
    es = 6.112 * np.exp((17.67 * temp_c) / (temp_c + 243.5))
    e = rh_pct / 100.0 * es
    return 216.7 * e / (temp_c + 273.15)


# -----------------------------
# API pública
# -----------------------------
def _day_bounds_evening(d: date) -> tuple[datetime, datetime]:
    """
    Ventana 18:00..23:59 del día d.
    En Meteostat usamos end exclusivo, así que ponemos end = día siguiente 00:00.
    """
    start = datetime.combine(d, time(18, 0, 0))
    end = datetime.combine(d + timedelta(days=1), time(0, 0, 0))
    return start, end


def get_daily_atmospheric_state(
    d: date,
    city: City,
    station_limit: int = 6,  # lo dejamos por compatibilidad aunque ya no se use
) -> tuple[DailyAtmosphericState, str]:
    """
    Devuelve el estado atmosférico medio diario (18:00–23:59) para la ciudad y fecha indicadas:
      - temp media (°C)
      - humedad relativa media (%)
      - humedad absoluta media (g/m³)

    Además devuelve el station_id finalmente utilizado.
    """
    if city not in CITY_STATIONS:
        raise ValueError(f"city must be one of {list(CITY_STATIONS.keys())}")

    start, end = _day_bounds_evening(d)

    last_err: Exception | None = None

    for station_id in CITY_STATIONS[city]:
        try:
            ts = ms.hourly(station_id, start, end)
            df = ts.fetch(fill=True)

            if df is None or df.empty:
                raise MeteostatDataError("Empty dataframe")

            # Necesitamos temp y rhum
            if "temp" not in df.columns or "rhum" not in df.columns:
                raise MeteostatDataError("Missing temp/rhum")

            # Alinear y limpiar conjuntamente para evitar broadcast
            df2 = df[["temp", "rhum"]].dropna()
            if df2.empty:
                raise MeteostatDataError("No overlapping temp/rhum data")

            temp_mean = float(df2["temp"].mean())
            rh_mean = float(df2["rhum"].mean())

            # humedad absoluta (vector) y media
            abs_h = _absolute_humidity_g_m3(df2["temp"].to_numpy(), df2["rhum"].to_numpy())
            abs_h_mean = float(np.mean(abs_h))

            state = DailyAtmosphericState(
                temp_c=round(temp_mean, 2),
                rh_pct=round(rh_mean, 2),
                abs_humidity_g_m3=round(abs_h_mean, 3),
            )
            return state, station_id

        except Exception as e:
            last_err = e
            continue

    raise MeteostatDataError(f"No usable station for {city} on {d.isoformat()}: {last_err}")

