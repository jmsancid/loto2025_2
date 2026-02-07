from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date, timedelta, time
import math
from typing import Iterable, Optional

from db_utils.db_management import DBManager
from other_utils.fase_lunar import obtener_valor_fase_lunar
from other_utils.prevision_temp_hr import fetch_hourly_temp_rh, daily_window_means_from_hourly, calc_abs_humidity
from other_utils.humidity_meteostat import CITY


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


# -----------------------------
# Helper: convertir a date lo que devuelve SQLite
# -----------------------------

def _to_date_sql(d: str | date | None) -> Optional[date]:
    if d is None:
        return None
    if isinstance(d, date):
        return d
    # esperado: "YYYY-MM-DD"
    return datetime.strptime(d, "%Y-%m-%d").date()


# -----------------------------
# Helpers: lunar binning (8 bins dinámicos)
# -----------------------------

def moon_value_from_date(d: date) -> float:
    return float(obtener_valor_fase_lunar(datetime.combine(d, time.min)))


def moon_bin_8(v_0_28: float) -> int:
    """Discretiza un valor lunar [0,28] en 8 bins uniformes de ancho 3.5."""
    if v_0_28 >= 28:
        return 7
    if v_0_28 < 0:
        return 0
    return int(v_0_28 // 3.5)  # 0..7


# -----------------------------
# Helpers: scoring
# -----------------------------

def gauss_score(delta: float, tol: float) -> float:
    """Score gaussiano estable: exp(-(delta/tol)^2). tol debe ser > 0."""
    if tol <= 0:
        return 0.0
    z = delta / tol
    return math.exp(-(z * z))

def tol_temp(target_temp: float, frac: float) -> float:
    return max(1.5, frac * abs(target_temp))

def tol_rh(target_rh: float, frac: float) -> float:
    return max(5.0, frac * target_rh)

def tol_ah(target_ah: float, frac: float) -> float:
    # Ajusta el mínimo si tu AH está en otra unidad. Con g/m³ suele ir bien.
    return max(0.5, frac * abs(target_ah))


# -----------------------------
# Semana objetivo y sorteos pendientes
# -----------------------------

def _start_end_week_window(today: date) -> tuple[date, date]:
    """
    Devuelve (start, end) de la ventana semanal objetivo.
    Semana objetivo:
      - Si hoy es domingo: lunes..sábado de la semana siguiente
      - Si hoy es lunes..sábado: hoy..sábado de la semana en curso
    """
    # weekday(): Monday=0 ... Sunday=6
    wd = today.weekday()

    if wd == 6:  # Sunday
        start = today + timedelta(days=1)  # Monday next day
    else:
        start = today

    # end = Saturday of the start's week
    # Saturday is weekday 5
    end = start + timedelta(days=(5 - start.weekday()))
    return start, end

def pending_draw_dates(today: date) -> dict[str, list[date]]:
    """
    Devuelve fechas de sorteos pendientes en la semana objetivo.
    Keys: "Primitiva", "Euromillones"
    """
    start, end = _start_end_week_window(today)

    primitiva_days = {0, 3, 5}  # Mon, Thu, Sat
    euro_days = {1, 4}          # Tue, Fri

    primitiva: list[date] = []
    euro: list[date] = []

    d = start
    while d <= end:
        if d.weekday() in primitiva_days:
            primitiva.append(d)
        if d.weekday() in euro_days:
            euro.append(d)
        d += timedelta(days=1)

    return {"Primitiva": primitiva, "Euromillones": euro}


# -----------------------------
# DB read: histórico + influencers
# -----------------------------

@dataclass(frozen=True)
class HistRowPrimitiva:
    n: tuple[int, int, int, int, int, int]
    re: int
    temp: float
    rh: float
    ah: float
    moon_val: float

@dataclass(frozen=True)
class HistRowEuro:
    n: tuple[int, int, int, int, int]
    e: tuple[int, int]
    temp: float
    rh: float
    ah: float
    moon_val: float


# -----------------------------
# Ranking: contextual + fallback global
# -----------------------------

def global_rank_counts(values: Iterable[int]) -> dict[int, float]:
    """Fallback global: score = frecuencia."""
    counts: dict[int, float] = {}
    for v in values:
        counts[v] = counts.get(v, 0.0) + 1.0
    return counts

def sort_score_dict(d: dict[int, float]) -> dict[int, float]:
    """Devuelve dict ordenado por score desc (inserción ya ordenada)."""
    return dict(sorted(d.items(), key=lambda kv: kv[1], reverse=True))

def score_primitiva_for_target(
    history: list[HistRowPrimitiva],
    *,
    target_temp: float,
    target_rh: float,
    target_ah: float,
    target_moon_bin: int,
    frac: float,
) -> tuple[dict[int, float], dict[int, float]]:
    """
    Devuelve (score_numeros, score_reintegro) para un target (un sorteo futuro).
    Filtra por luna_bin exacto. Meteo entra como score gaussiano.
    """
    s_nums: dict[int, float] = {}
    s_re: dict[int, float] = {}

    tT = tol_temp(target_temp, frac)
    tRH = tol_rh(target_rh, frac)
    tAH = tol_ah(target_ah, frac)

    for row in history:
        if moon_bin_8(row.moon_val) != target_moon_bin:
            continue

        score_row = (
            gauss_score(row.temp - target_temp, tT) *
            gauss_score(row.rh - target_rh, tRH) *
            gauss_score(row.ah - target_ah, tAH)
        )

        if score_row <= 0.0:
            continue

        for num in row.n:
            s_nums[num] = s_nums.get(num, 0.0) + score_row

        # reintegro (0..9)
        s_re[row.re] = s_re.get(row.re, 0.0) + score_row

    return sort_score_dict(s_nums), sort_score_dict(s_re)

def score_euro_for_target(
    history: list[HistRowEuro],
    *,
    target_temp: float,
    target_rh: float,
    target_ah: float,
    target_moon_bin: int,
    frac: float,
) -> tuple[dict[int, float], dict[int, float]]:
    """
    Devuelve (score_numeros, score_estrellas) para un target (un sorteo futuro).
    """
    s_nums: dict[int, float] = {}
    s_stars: dict[int, float] = {}

    tT = tol_temp(target_temp, frac)
    tRH = tol_rh(target_rh, frac)
    tAH = tol_ah(target_ah, frac)

    for row in history:
        if moon_bin_8(row.moon_val) != target_moon_bin:
            continue

        score_row = (
            gauss_score(row.temp - target_temp, tT) *
            gauss_score(row.rh - target_rh, tRH) *
            gauss_score(row.ah - target_ah, tAH)
        )

        if score_row <= 0.0:
            continue

        for num in row.n:
            s_nums[num] = s_nums.get(num, 0.0) + score_row
        for st in row.e:
            s_stars[st] = s_stars.get(st, 0.0) + score_row

    return sort_score_dict(s_nums), sort_score_dict(s_stars)


def merge_scores_sum(dicts: list[dict[int, float]]) -> dict[int, float]:
    merged: dict[int, float] = {}
    for d in dicts:
        for k, v in d.items():
            merged[k] = merged.get(k, 0.0) + v
    return sort_score_dict(merged)


# -----------------------------
# Candidate selection with fallback
# -----------------------------

def select_top_unique(
    primary_rank: dict[int, float],
    fallback_rank: dict[int, float],
    *,
    needed: int,
    exclude: set[int] | None = None,
) -> list[int]:
    """
    Selecciona `needed` candidatos únicos:
    - primero de primary_rank
    - si no llega, completa desde fallback_rank
    """
    if exclude is None:
        exclude = set()

    out: list[int] = []

    def take_from(rank: dict[int, float]) -> None:
        nonlocal out
        for k in rank.keys():
            if k in exclude:
                continue
            exclude.add(k)
            out.append(k)
            if len(out) >= needed:
                return

    take_from(primary_rank)
    if len(out) < needed:
        take_from(fallback_rank)

    return out


# -----------------------------
# Build apuestas from weekly rankings (sin repetidos)
# -----------------------------

def build_apuestas_primitiva(
    weekly_nums_rank: dict[int, float],
    weekly_re_rank: dict[int, float],
    global_nums_rank: dict[int, float],
    global_re_rank: dict[int, float],
) -> list[Apuesta_Primitiva]:
    # cada apuesta consume 30 números únicos
    apuestas: list[Apuesta_Primitiva] = []
    used_global: set[int] = set()

    # reintegro común: top-1 (con fallback)
    reintegro_list = select_top_unique(weekly_re_rank, global_re_rank, needed=1, exclude=set())
    if not reintegro_list:
        return []
    reintegro = reintegro_list[0]

    # construir tantas apuestas como permita el ranking
    while True:
        nums = select_top_unique(
            weekly_nums_rank,
            global_nums_rank,
            needed=30,
            exclude=used_global,
        )
        if len(nums) < 30:
            used_global.update(nums)
            break

        # repartir en 5 combinaciones de 6
        combs: list[tuple[int, int, int, int, int, int]] = []
        for i in range(0, 30, 6):
            block = sorted(nums[i:i+6])
            combs.append(tuple(block))  # type: ignore[arg-type]

        apuestas.append(Apuesta_Primitiva(combinaciones=tuple(combs), reintegro=reintegro))

    return apuestas

def build_apuestas_euromillones(
    weekly_nums_rank: dict[int, float],
    weekly_stars_rank: dict[int, float],
    global_nums_rank: dict[int, float],
    global_stars_rank: dict[int, float],
) -> list[Apuesta_Euromillones]:
    # cada apuesta consume 10 números y 4 estrellas únicas
    apuestas: list[Apuesta_Euromillones] = []
    used_nums: set[int] = set()
    used_stars: set[int] = set()

    while True:
        nums = select_top_unique(weekly_nums_rank, global_nums_rank, needed=10, exclude=used_nums)
        stars = select_top_unique(weekly_stars_rank, global_stars_rank, needed=4, exclude=used_stars)

        if len(nums) < 10 or len(stars) < 4:
            used_nums.update(nums)
            used_stars.update(stars)
            break

        combs = []
        for i in range(0, 10, 5):
            n_block = tuple(sorted(nums[i:i+5]))  # 5 nums
            # estrellas: 2 por combinación
            s_block = tuple(sorted(stars[(i//5)*2:(i//5)*2 + 2]))  # 2 stars
            combs.append((n_block, s_block))

        apuestas.append(Apuesta_Euromillones(combinaciones=tuple(combs)))

    return apuestas


# -----------------------------
# Main: weekly ranking + apuestas
# -----------------------------

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


def compute_weekly_apuestas(
    *,
    db: DBManager,
    today: date,
    window_days: int = 7,
) -> WeeklyResult:
    """
    Calcula apuestas semanales para sorteos pendientes:
      - Primitiva (Madrid)
      - Euromillones (Paris)

    Estrategia:
      - ranking contextual con frac=0.10
      - si faltan candidatos: frac=0.15
      - si aún faltan: fallback global histórico
    """
    # 1) fechas pendientes
    pending = pending_draw_dates(today)

    last_p = _to_date_sql(db.fecha_ultimo_resultado("Primitiva", "fecha"))
    last_e = _to_date_sql(db.fecha_ultimo_resultado("Euromillones", "fecha"))

    prim_dates = [d for d in pending["Primitiva"] if last_p is None or d > last_p]
    euro_dates = [d for d in pending["Euromillones"] if last_e is None or d > last_e]

    print("[INFO] Pendientes Primitiva:", [d.isoformat() for d in prim_dates])
    print("[INFO] Pendientes Euromillones:", [d.isoformat() for d in euro_dates])

    # 2) cargar histórico (una vez)
    hist_p = db.load_history_primitiva()
    hist_e = db.load_history_euromillones()

    # fallback global rankings
    global_p_nums = sort_score_dict(global_rank_counts(num for r in hist_p for num in r.n))
    global_p_re = sort_score_dict(global_rank_counts(r.re for r in hist_p if r.re is not None))
    global_e_nums = sort_score_dict(global_rank_counts(num for r in hist_e for num in r.n))
    global_e_stars = sort_score_dict(global_rank_counts(st for r in hist_e for st in r.e))

    # 3) forecast window por ciudad (una vez)
    #    (map date -> (temp_mean, rh_mean))
    def forecast_map_for_city(citycfg):
        hourly = fetch_hourly_temp_rh(citycfg, days=window_days)
        daily = daily_window_means_from_hourly(
            time=hourly["time"],
            temperature_2m=hourly["temperature_2m"],
            relative_humidity_2m=hourly["relative_humidity_2m"],
            start_hour=18,
            end_hour=23,  # alineado con tu histórico actual (18-23)
        )
        return {date.fromisoformat(d.date): d for d in daily}

    fc_madrid = forecast_map_for_city(CITY["MADRID"])
    fc_paris = forecast_map_for_city(CITY["PARIS"])

    # 4) construir scores por sorteo futuro y sumar a ranking semanal
    def target_context(d: date, city_fc_map):
        day = city_fc_map.get(d)
        if day is None or day.temp_mean_c is None or day.rh_mean_pct is None:
            raise ValueError(f"No hay forecast 18-23 para la fecha {d.isoformat()}")
        T = float(day.temp_mean_c)
        RH = float(day.rh_mean_pct)
        AH = float(calc_abs_humidity(T, RH))
        moonv = moon_value_from_date(d)
        m_bin = moon_bin_8(moonv)
        return T, RH, AH, m_bin

    tol_options = (0.10, 0.15)

    # Primitiva (Madrid) - por fecha

    # weekly_p_nums_10: list[dict[int, float]] = []
    # weekly_p_re_10: list[dict[int, float]] = []
    # weekly_p_nums_15: list[dict[int, float]] = []
    # weekly_p_re_15: list[dict[int, float]] = []

    apuestas_primitiva: list[tuple[date, Apuesta_Primitiva]] = []
    tol_p_used: float = tol_options[0] if prim_dates else None  # type: ignore[assignment]


    for d in prim_dates:
        T, RH, AH, mb = target_context(d, fc_madrid)

        ap_p_list: list[Apuesta_Primitiva] = []
        used_frac = tol_options[-1]

        for frac in tol_options:
            s_nums, s_re = score_primitiva_for_target(
                hist_p,
                target_temp=T, target_rh=RH, target_ah=AH,
                target_moon_bin=mb,
                frac=frac,
            )
            ap_p_list = build_apuestas_primitiva(
                weekly_nums_rank=s_nums,
                weekly_re_rank=s_re,
                global_nums_rank=global_p_nums,
                global_re_rank=global_p_re,
            )
            if ap_p_list:
                used_frac = frac
                break

        if not ap_p_list:
            # fallback “puro global”
            ap_p_list = build_apuestas_primitiva(
                weekly_nums_rank={},
                weekly_re_rank={},
                global_nums_rank=global_p_nums,
                global_re_rank=global_p_re,
            )
            used_frac = tol_options[-1]

        if ap_p_list:
            apuestas_primitiva.append((d, ap_p_list[0]))  # 1 apuesta por sorteo
            tol_p_used = used_frac if tol_p_used is None else max(tol_p_used, used_frac)

    # weekly_p_nums_rank_10 = merge_scores_sum(weekly_p_nums_10) if weekly_p_nums_10 else {}
    # weekly_p_re_rank_10 = merge_scores_sum(weekly_p_re_10) if weekly_p_re_10 else {}
    # weekly_p_nums_rank_15 = merge_scores_sum(weekly_p_nums_15) if weekly_p_nums_15 else {}
    # weekly_p_re_rank_15 = merge_scores_sum(weekly_p_re_15) if weekly_p_re_15 else {}

    # Euromillones (Paris) - por fecha

    # weekly_e_nums_10: list[dict[int, float]] = []
    # weekly_e_stars_10: list[dict[int, float]] = []
    # weekly_e_nums_15: list[dict[int, float]] = []
    # weekly_e_stars_15: list[dict[int, float]] = []

    # --- EUROMILLONES por fecha ---
    apuestas_euromillones: list[tuple[date, Apuesta_Euromillones]] = []
    tol_e_used: float = tol_options[0] if euro_dates else None  # type: ignore[assignment]

    for d in euro_dates:
        T, RH, AH, mb = target_context(d, fc_paris)

        ap_e_list: list[Apuesta_Euromillones] = []
        used_frac = tol_options[-1]

        for frac in tol_options:
            s_nums, s_st = score_euro_for_target(
                hist_e,
                target_temp=T, target_rh=RH, target_ah=AH,
                target_moon_bin=mb,
                frac=frac,
            )
            ap_e_list = build_apuestas_euromillones(
                weekly_nums_rank=s_nums,
                weekly_stars_rank=s_st,
                global_nums_rank=global_e_nums,
                global_stars_rank=global_e_stars,
            )
            if ap_e_list:
                used_frac = frac
                break

        if not ap_e_list:
            ap_e_list = build_apuestas_euromillones(
                weekly_nums_rank={},
                weekly_stars_rank={},
                global_nums_rank=global_e_nums,
                global_stars_rank=global_e_stars,
            )
            used_frac = tol_options[-1]

        if ap_e_list:
            apuestas_euromillones.append((d, ap_e_list[0]))
            tol_e_used = used_frac if tol_e_used is None else max(tol_e_used, used_frac)
    week_start, week_end = _start_end_week_window(today)

    return WeeklyResult(
        primitiva_dates=tuple(prim_dates),
        euromillones_dates=tuple(euro_dates),
        apuestas_primitiva=tuple(apuestas_primitiva),
        apuestas_euromillones=tuple(apuestas_euromillones),
        week_start=week_start,
        week_end=week_end,
        tol_primitiva=tol_p_used if prim_dates else None,
        tol_euro=tol_e_used if euro_dates else None,
        method_version="v1",
    )


_ES_DAYS = ["Lunes","Martes","Miércoles","Jueves","Viernes","Sábado","Domingo"]

def _fmt_day(d: date) -> str:
    return f"{_ES_DAYS[d.weekday()]} {d.isoformat()}"

def _fmt_nums(nums) -> str:
    return " ".join(f"{n:02d}" for n in nums)

def _fmt_euro_combo(combo) -> str:
    nums, stars = combo
    nums_s = _fmt_nums(sorted(nums))
    e1, e2 = sorted(stars)
    return f"{nums_s} ⭐ {e1:02d} {e2:02d}"

def format_weekly_result(weekly: WeeklyResult) -> str:
    ws = weekly.week_start.isoformat() if weekly.week_start else "?"
    we = weekly.week_end.isoformat() if weekly.week_end else "?"
    lines = [f"🗓 Semana {ws} → {we}", ""]

    lines.append("🎯 PRIMITIVA (Madrid)")
    if weekly.apuestas_primitiva:
        for d, ap in weekly.apuestas_primitiva:
            lines.append(f"📌 {_fmt_day(d)}  |  Reintegro: {int(ap.reintegro)}")
            for i, combo in enumerate(ap.combinaciones, 1):
                lines.append(f"  {i}) {_fmt_nums(combo)}")
            lines.append("")
    else:
        lines.append("  (sin apuestas generadas)")
        lines.append("")

    lines.append("🎯 EUROMILLONES (París)")
    if weekly.apuestas_euromillones:
        for d, ap in weekly.apuestas_euromillones:
            lines.append(f"📌 {_fmt_day(d)}")
            for i, combo in enumerate(ap.combinaciones, 1):
                lines.append(f"  {i}) {_fmt_euro_combo(combo)}")
            lines.append("")
    else:
        lines.append("  (sin apuestas generadas)")
        lines.append("")

    return "\n".join(lines).rstrip()
