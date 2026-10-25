#!/usr/bin/env python3
"""Télécharge une tranche du dataset Chicago Taxi Trips via l'API SODA (sans Docker).

Exemples :
  python scripts/download_sample.py --start 2023-01-01 --end 2023-04-01 --out data/raw     # T1 2023
  python scripts/download_sample.py --start 2023-01-01 --end 2023-01-03 --max-rows-per-day 5000

Layout produit : <out>/trip_date=YYYY-MM-DD/trips.csv (identique au bronze du datalake).
"""
import argparse
import os
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.ingestion import soda  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--start", required=True, help="YYYY-MM-DD (inclus)")
    p.add_argument("--end", required=True, help="YYYY-MM-DD (exclu)")
    p.add_argument("--out", default="data/raw")
    p.add_argument("--page-size", type=int, default=50_000)
    p.add_argument("--max-rows-per-day", type=int, default=0)
    a = p.parse_args()

    session = soda.make_session(os.getenv("SODA_APP_TOKEN") or None)
    total = 0
    for day in soda.daterange(date.fromisoformat(a.start), date.fromisoformat(a.end)):
        dest = Path(a.out) / f"trip_date={day.isoformat()}" / "trips.csv"
        n = soda.download_day(day, dest, page_size=a.page_size, max_rows=a.max_rows_per_day, session=session)
        total += n
        print(f"{day}: {n} lignes", flush=True)
    print(f"Total: {total} lignes -> {a.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
