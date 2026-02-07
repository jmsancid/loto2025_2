from __future__ import annotations
import json
from pathlib import Path

from other_utils.humidity_meteostat import CITY
from other_utils.ranking_semanal import fetch_hourly_temp_rh

OUTDIR = Path("tests/fixtures/forecast")

def main() -> None:
    OUTDIR.mkdir(parents=True, exist_ok=True)
    for key in ("MADRID", "PARIS"):
        cfg = CITY[key]
        hourly = fetch_hourly_temp_rh(cfg, days=7)
        (OUTDIR / f"{cfg.key}.json").write_text(
            json.dumps(hourly, ensure_ascii=False),
            encoding="utf-8",
        )
        print("Wrote", OUTDIR / f"{cfg.key}.json")

if __name__ == "__main__":
    main()

