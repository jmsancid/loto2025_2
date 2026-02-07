#!/usr/bin/env python3
import argparse
import pandas as pd
import meteostat as ms

def to_df(obj):
    # En tu build, nearby() a veces devuelve DataFrame directamente
    if isinstance(obj, pd.DataFrame):
        return obj
    fetch = getattr(obj, "fetch", None)
    if callable(fetch):
        out = fetch()
        if isinstance(out, pd.DataFrame):
            return out
    raise TypeError(f"No puedo convertir a DataFrame: {type(obj)!r}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lat", type=float, required=True)
    ap.add_argument("--lon", type=float, required=True)
    ap.add_argument("--elev", type=int, default=None)
    ap.add_argument("--limit", type=int, default=30)
    args = ap.parse_args()

    point = ms.Point(args.lat, args.lon, args.elev) if args.elev is not None else ms.Point(args.lat, args.lon)

    nearby = ms.stations.nearby(point, limit=args.limit)
    df = to_df(nearby)

    # Dejamos solo columnas útiles (si existen)
    cols = [c for c in ["name", "country", "region", "latitude", "longitude", "elevation", "timezone", "distance"] if c in df.columns]
    out = df[cols].copy() if cols else df.copy()

    # Mostrar top N con station_id (índice)
    print(out.head(args.limit).to_string())

if __name__ == "__main__":
    main()
