from datetime import datetime
import pandas as pd
import meteostat as ms

# STATIONS = ["07156", "07147", "07149", "07150", "07157"]
STATIONS = ["08222", "08221", "LEMM0", "08223"]

def df_from_ts(ts):
    # ts es TimeSeries en tu build
    df = ts.fetch(fill=True)  # fill=True suele ayudar mucho
    if df is None:
        return pd.DataFrame()
    return df

def check_one(station_id: str, start: datetime, end: datetime) -> tuple[int, int, int]:
    ts = ms.hourly(station_id, start, end)
    df = df_from_ts(ts)

    if df.empty:
        return 0, 0, 0

    n_rows = len(df)
    n_temp = int(df["temp"].notna().sum()) if "temp" in df.columns else 0
    n_rhum = int(df["rhum"].notna().sum()) if "rhum" in df.columns else 0
    return n_rows, n_temp, n_rhum

def main():
    # ventana corta en 1985
    start_1985 = datetime(1985, 1, 1, 0, 0, 0)
    end_1985   = datetime(1985, 1, 8, 0, 0, 0)

    # ventana corta reciente
    start_recent = datetime(2024, 1, 1, 0, 0, 0)
    end_recent   = datetime(2024, 1, 8, 0, 0, 0)

    for sid in STATIONS:
        r85 = check_one(sid, start_1985, end_1985)
        r24 = check_one(sid, start_recent, end_recent)
        print(
            f"{sid} | 1985 rows/temp/rhum: {r85[0]}/{r85[1]}/{r85[2]} "
            f"| 2024 rows/temp/rhum: {r24[0]}/{r24[1]}/{r24[2]}"
        )

if __name__ == "__main__":
    main()
