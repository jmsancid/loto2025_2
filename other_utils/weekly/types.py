from dataclasses import dataclass
from datetime import date
from typing import Optional


# -----------------------------
# Domain models (apuestas)
# -----------------------------

@dataclass(frozen=True)
class Apuesta_Primitiva:
    # 5 combinaciones de 6 números cada una + reintegro común
    combinaciones: tuple[tuple[int, int, int, int, int, int], ...]  # len=5
    reintegro: int  # 0..9

@dataclass(frozen=True)
class Apuesta_Euromillones:
    # 2 combinaciones: 5 números + 2 estrellas
    combinaciones: tuple[tuple[tuple[int, int, int, int, int], tuple[int, int]], ...]  # len=2


@dataclass(frozen=True)
class WeeklyResult:
    primitiva_dates: tuple[date, ...]
    euromillones_dates: tuple[date, ...]
    apuestas_primitiva: tuple[tuple[date, Apuesta_Primitiva], ...]
    apuestas_euromillones: tuple[tuple[date, Apuesta_Euromillones], ...]
    week_start: Optional[date] = None
    week_end: Optional[date] = None
    tol_primitiva: Optional[float] = None
    tol_euro: Optional[float] = None
    method_version: str = "v1"

