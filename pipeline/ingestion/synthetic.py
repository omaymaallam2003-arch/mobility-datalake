"""Générateur de données factices au schéma Chicago Taxi (mode hors-ligne / démo / tests).

Injecte volontairement ~5 % d'anomalies (doublons, montants négatifs, fin < début,
trip_id manquant, durée nulle) pour exercer les règles de qualité de la couche Silver.
"""
from __future__ import annotations

import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

from .soda import TRIP_COLUMNS

_PAYMENTS = ["Credit Card", "Cash", "Mobile", "Prcard", "Unknown", "No Charge"]
_PAY_W = [55, 35, 5, 3, 1, 1]
_COMPANIES = [
    "Flash Cab", "Taxi Affiliation Services", "Sun Taxi", "City Service",
    "Blue Ribbon Taxi Association", "Chicago Carriage Cab Corp", "5 Star Taxi", None,
]
# Zones très demandées (Loop, Near North, O'Hare, Near West, Lake View, Lincoln Park…)
_HOT_AREAS = [32, 8, 76, 28, 6, 7, 24, 33, 22, 3]
_HOUR_W = [2, 1, 1, 1, 1, 2, 4, 7, 8, 6, 5, 5, 6, 6, 6, 7, 8, 9, 9, 8, 6, 5, 4, 3]


def _ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000")


def generate_day(day: date, dest: Path, n_rows: int = 2000, seed: int = 42) -> int:
    """Écrit un CSV de `n_rows` trajets (+ anomalies). Retourne le nombre de lignes écrites."""
    rng = random.Random(f"{seed}-{day.isoformat()}")
    dest.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for _ in range(n_rows):
        hour = rng.choices(range(24), weights=_HOUR_W)[0]
        start = datetime(day.year, day.month, day.day, hour, rng.choice([0, 15, 30, 45]))
        seconds = max(60, int(rng.lognormvariate(6.5, 0.6)))
        miles = round(seconds / 3600 * rng.uniform(8, 22), 1)
        fare = round(3.25 + miles * 2.25 + seconds / 60 * 0.2, 2)
        pay = rng.choices(_PAYMENTS, weights=_PAY_W)[0]
        tips = round(fare * rng.choice([0, 0.1, 0.15, 0.2]), 2) if pay == "Credit Card" else 0.0
        extras = rng.choice([0, 0, 1, 2])
        pu = rng.choice(_HOT_AREAS) if rng.random() < 0.7 else rng.randint(1, 77)
        do = rng.choice(_HOT_AREAS) if rng.random() < 0.5 else rng.randint(1, 77)
        rows.append({
            "trip_id": f"{rng.getrandbits(128):032x}",
            "taxi_id": f"{rng.getrandbits(256):064x}",
            "trip_start_timestamp": _ts(start),
            "trip_end_timestamp": _ts(start + timedelta(seconds=seconds)),
            "trip_seconds": seconds,
            "trip_miles": miles,
            "pickup_census_tract": "",
            "dropoff_census_tract": "",
            "pickup_community_area": pu if rng.random() > 0.03 else "",
            "dropoff_community_area": do,
            "fare": fare, "tips": tips, "tolls": 0.0, "extras": extras,
            "trip_total": round(fare + tips + extras, 2),
            "payment_type": pay,
            "company": rng.choice(_COMPANIES) or "",
            "pickup_centroid_latitude": round(rng.uniform(41.70, 41.98), 6),
            "pickup_centroid_longitude": round(rng.uniform(-87.80, -87.58), 6),
            "pickup_centroid_location": "",
            "dropoff_centroid_latitude": round(rng.uniform(41.70, 41.98), 6),
            "dropoff_centroid_longitude": round(rng.uniform(-87.80, -87.58), 6),
            "dropoff_centroid_location": "",
        })
    # --- anomalies ---
    k = max(1, n_rows // 100)
    for _ in range(k):                                   # doublons exacts
        rows.append(dict(rng.choice(rows)))
    for r in rng.sample(rows, k):                        # montant négatif
        r["fare"], r["trip_total"] = -abs(float(r["fare"])), -abs(float(r["trip_total"]))
    for r in rng.sample(rows, k):                        # fin avant début
        r["trip_end_timestamp"] = _ts(datetime.fromisoformat(r["trip_start_timestamp"]) - timedelta(minutes=10))
    for r in rng.sample(rows, k):                        # trip_id manquant
        r["trip_id"] = ""
    for r in rng.sample(rows, k):                        # durée nulle
        r["trip_seconds"] = 0
    rng.shuffle(rows)
    with open(dest, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=TRIP_COLUMNS)
        w.writeheader()
        w.writerows(rows)
    return len(rows)
