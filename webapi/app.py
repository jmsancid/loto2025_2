# webapi/app.py
from __future__ import annotations

from datetime import datetime, timezone, date

from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from fastapi.responses import PlainTextResponse

from other_utils.weekly import generate_weekly, format_weekly
from webapi.schemas import WeeklyResponseV1, WeeklyApuestaEntryV1, WeeklyMetaV1

from db_utils.db_management import DBManager  # <-- AJUSTA AQUÍ
from constants import DBFILE

app = FastAPI(title="santiloto-webapi", version="1.0.0")

def _today():
    import os
    if "SANTILOTO_TODAY" in os.environ:
        return date.fromisoformat(os.environ["SANTILOTO_TODAY"])
    return date.today()


def _weekly_to_v1(result) -> WeeklyResponseV1:
    # Serializa apuestas como payload JSON estable
    apuestas_primitiva = [
        WeeklyApuestaEntryV1(draw_date=d, payload=jsonable_encoder(apuesta))
        for (d, apuesta) in result.apuestas_primitiva
    ]
    apuestas_euromillones = [
        WeeklyApuestaEntryV1(draw_date=d, payload=jsonable_encoder(apuesta))
        for (d, apuesta) in result.apuestas_euromillones
    ]

    return WeeklyResponseV1(
        method_version=result.method_version,
        primitiva_dates=list(result.primitiva_dates),
        euromillones_dates=list(result.euromillones_dates),
        apuestas_primitiva=apuestas_primitiva,
        apuestas_euromillones=apuestas_euromillones,
        week_start=result.week_start,
        week_end=result.week_end,
        tol_primitiva=result.tol_primitiva,
        tol_euro=result.tol_euro,
        meta=WeeklyMetaV1(generated_at=datetime.now(timezone.utc)),
    )


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/weekly", response_model=WeeklyResponseV1)
def weekly() -> WeeklyResponseV1:
    today = _today()
    with DBManager(DBFILE) as db:
        result = generate_weekly(db=db, today=today)
    return _weekly_to_v1(result)


@app.get("/weekly.txt")
def weekly_txt():
    today = _today()
    with DBManager(DBFILE) as db:
        result = generate_weekly(db=db, today=today)
    txt = format_weekly(result)
    return PlainTextResponse(content=txt, media_type="text/plain; charset=utf-8")