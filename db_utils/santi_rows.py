from datetime import date
from typing import Any, Sequence


def _fmt_date(d: date | str) -> str:
    return d.isoformat() if isinstance(d, date) else str(d)


def make_signature_primitiva(
        target_date: date | str,
        re: int,
        combos: Sequence[Sequence[int]],  # 5 combos de 6
) -> str:
    norm_combos = sorted(tuple(sorted(map(int, c))) for c in combos)
    parts = ["P", _fmt_date(target_date), f"R{int(re)}"]
    parts += ["-".join(f"{n:02d}" for n in c) for c in norm_combos]
    return "|".join(parts)


def make_signature_euromillones(
        target_date: date | str,
        combos7: Sequence[Sequence[int]],  # 2 combos: 5 nums + 2 estrellas
) -> str:
    norm_combos: list[tuple[int, ...]] = []
    for c in combos7:
        c = list(map(int, c))
        nums = sorted(c[:5])
        e1, e2 = sorted(c[5:7])
        norm_combos.append(tuple(nums + [e1, e2]))
    norm_combos.sort()

    parts = ["E", _fmt_date(target_date)]
    parts += [
        "-".join([*(f"{n:02d}" for n in c[:5]), f"S{c[5]:02d}", f"S{c[6]:02d}"])
        for c in norm_combos
    ]
    return "|".join(parts)


def santi_primitiva_row(
        *,
        target_date: date,
        week_start: date,
        week_end: date,
        apuesta,
        tol_frac: float,
        method_version: str = "v1",
        city: str = "Madrid",
) -> dict[str, Any]:
    """
    Construye una fila para SantiPrimitiva (1 apuesta = 5 combinaciones + 1 reintegro)
    """
    # combos: tuple[tuple[int,int,int,int,int,int], ...] (len=5)
    combos = apuesta.combinaciones
    re = int(apuesta.reintegro)

    row: dict[str, Any] = {
        "week_start": week_start.isoformat(),
        "week_end": week_end.isoformat(),
        "target_date": target_date.isoformat(),
        "method_version": method_version,
        "tol_frac": float(tol_frac),
        "city": city,
        "re": re,
    }

    # c1n1..c5n6
    for ci, combo in enumerate(combos, start=1):
        for ni, n in enumerate(combo, start=1):
            row[f"c{ci}n{ni}"] = int(n)

    row["signature"] = make_signature_primitiva(
        target_date=target_date,
        re=re,
        combos=combos,
    )

    return row


def santi_euromillones_row(
        *,
        target_date: date,
        week_start: date,
        week_end: date,
        apuesta,
        tol_frac: float,
        method_version: str = "v1",
        city: str = "Paris",
) -> dict[str, Any]:
    """
    Construye una fila para SantiEuromillones (1 apuesta = 2 combinaciones)
    """
    row: dict[str, Any] = {
        "week_start": week_start.isoformat(),
        "week_end": week_end.isoformat(),
        "target_date": target_date.isoformat(),
        "method_version": method_version,
        "tol_frac": float(tol_frac),
        "city": city,
    }

    combos7: list[tuple[int, ...]] = []

    # combos: ((n1..n5),(e1,e2))
    for ci, (nums, stars) in enumerate(apuesta.combinaciones, start=1):
        nums = tuple(sorted(nums))
        stars = tuple(sorted(stars))
        combos7.append(tuple(nums + stars))

        for ni, n in enumerate(nums, start=1):
            row[f"c{ci}n{ni}"] = int(n)
        row[f"c{ci}e1"] = int(stars[0])
        row[f"c{ci}e2"] = int(stars[1])

    row["signature"] = make_signature_euromillones(
        target_date=target_date,
        combos7=combos7,
    )

    return row
