"""Téléchargement paginé du dataset Chicago Taxi Trips via l'API SODA (Socrata).

Module volontairement indépendant de Dagster/Spark : réutilisable en CLI
(scripts/download_sample.py) et testable sans infrastructure.
"""
from __future__ import annotations

import csv
import io
from datetime import date, timedelta
from pathlib import Path
from typing import Iterator, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

SODA_URL = "https://data.cityofchicago.org/resource/wrvz-psew.csv"

# En-tête CSV renvoyé par l'API (utilisé aussi par le générateur synthétique)
TRIP_COLUMNS = [
    "trip_id", "taxi_id", "trip_start_timestamp", "trip_end_timestamp",
    "trip_seconds", "trip_miles", "pickup_census_tract", "dropoff_census_tract",
    "pickup_community_area", "dropoff_community_area", "fare", "tips", "tolls",
    "extras", "trip_total", "payment_type", "company",
    "pickup_centroid_latitude", "pickup_centroid_longitude", "pickup_centroid_location",
    "dropoff_centroid_latitude", "dropoff_centroid_longitude", "dropoff_centroid_location",
]


def daterange(start: date, end: date) -> Iterator[date]:
    """Jours de [start, end) — end exclu."""
    d = start
    while d < end:
        yield d
        d += timedelta(days=1)


def build_where(day: date) -> str:
    nxt = day + timedelta(days=1)
    return (
        f"trip_start_timestamp >= '{day.isoformat()}T00:00:00' "
        f"AND trip_start_timestamp < '{nxt.isoformat()}T00:00:00'"
    )


def make_session(app_token: Optional[str] = None) -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=6, backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    if app_token:
        s.headers["X-App-Token"] = app_token
    return s


def _count_rows(text: str) -> int:
    """Nombre de lignes de données (hors en-tête) dans un CSV."""
    n = sum(1 for _ in csv.reader(io.StringIO(text)))
    return max(n - 1, 0)


def download_day(
    day: date,
    dest: Path,
    *,
    page_size: int = 50_000,
    max_rows: int = 0,
    session: Optional[requests.Session] = None,
    url: str = SODA_URL,
    timeout: int = 180,
) -> int:
    """Télécharge tous les trajets démarrés le jour `day` dans `dest` (CSV avec en-tête).

    Pagination $limit/$offset avec tri stable sur trip_id.
    `max_rows` > 0 plafonne le volume du jour. Retourne le nombre de lignes écrites
    (0 => aucun fichier créé).
    """
    session = session or make_session()
    dest.parent.mkdir(parents=True, exist_ok=True)
    offset, total = 0, 0
    f = None
    try:
        while True:
            limit = page_size if not max_rows else min(page_size, max_rows - total)
            if limit <= 0:
                break
            resp = session.get(
                url,
                params={
                    "$where": build_where(day),
                    "$order": "trip_id",
                    "$limit": limit,
                    "$offset": offset,
                },
                timeout=timeout,
            )
            resp.raise_for_status()
            text = resp.content.decode("utf-8")
            rows = _count_rows(text)
            if rows == 0:
                break
            if f is None:
                f = open(dest, "w", encoding="utf-8", newline="")
                chunk = text
            else:
                chunk = text.split("\n", 1)[1] if "\n" in text else ""  # on retire l'en-tête répété
            f.write(chunk if chunk.endswith("\n") else chunk + "\n")
            total += rows
            offset += rows
            if rows < limit:
                break
    finally:
        if f is not None:
            f.close()
    return total
