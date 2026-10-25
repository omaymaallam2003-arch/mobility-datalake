"""BRONZE : ingestion des CSV bruts dans le stockage objet, sans aucune modification.

Layout : s3://<bucket>/bronze/chicago_taxi/trip_date=YYYY-MM-DD/trips.csv
Idempotent : un jour déjà présent est ignoré (sauf force=true).
"""
import os
import tempfile
from datetime import date
from pathlib import Path

from dagster import AssetExecutionContext, Config, Failure, MaterializeResult, asset

from ..config import BRONZE_PREFIX
from ..ingestion import soda, synthetic
from ..resources import S3Resource


class BronzeConfig(Config):
    """Paramètres modifiables dans l'UI Dagster au lancement (valeurs par défaut = variables d'env)."""
    source_mode: str = os.getenv("SOURCE_MODE", "soda")       # soda | synthetic
    start_date: str = os.getenv("START_DATE", "2023-01-01")   # inclusive
    end_date: str = os.getenv("END_DATE", "2023-01-15")       # exclusive
    page_size: int = int(os.getenv("SODA_PAGE_SIZE", "50000"))
    max_rows_per_day: int = int(os.getenv("MAX_ROWS_PER_DAY", "0"))  # 0 = illimité
    synthetic_rows_per_day: int = 2000
    force: bool = False                                       # ré-ingérer même si le jour existe


@asset(group_name="bronze", compute_kind="python",
       description="CSV bruts Chicago Taxi Trips (API SODA), partitionnés par jour, non modifiés.")
def bronze_trips(context: AssetExecutionContext, config: BronzeConfig, s3: S3Resource) -> MaterializeResult:
    if config.source_mode not in ("soda", "synthetic"):
        raise Failure(f"source_mode invalide: {config.source_mode!r} (soda | synthetic)")
    start, end = date.fromisoformat(config.start_date), date.fromisoformat(config.end_date)
    if end <= start:
        raise Failure("end_date doit être strictement postérieure à start_date")

    s3.ensure_bucket()
    client = s3.client()
    session = soda.make_session(os.getenv("SODA_APP_TOKEN") or None) if config.source_mode == "soda" else None

    ingested = skipped = empty = rows_total = bytes_total = 0
    with tempfile.TemporaryDirectory() as tmp:
        for day in soda.daterange(start, end):
            key = f"{BRONZE_PREFIX}/trip_date={day.isoformat()}/trips.csv"
            if not config.force and s3.exists(key):
                skipped += 1
                context.log.info(f"[skip] {day} déjà présent")
                continue
            local = Path(tmp) / f"{day.isoformat()}.csv"
            if config.source_mode == "soda":
                n = soda.download_day(day, local, page_size=config.page_size,
                                      max_rows=config.max_rows_per_day, session=session)
            else:
                n = synthetic.generate_day(day, local, n_rows=config.synthetic_rows_per_day)
            if n == 0:
                empty += 1
                context.log.warning(f"[empty] {day} : aucune donnée")
                continue
            client.upload_file(str(local), s3.bucket, key)
            size = local.stat().st_size
            local.unlink()
            ingested += 1
            rows_total += n
            bytes_total += size
            context.log.info(f"[ok] {day} : {n} lignes ({size / 1e6:.1f} Mo) -> s3://{s3.bucket}/{key}")

    if ingested == 0 and skipped == 0:
        raise Failure("Aucune donnée ingérée sur la période demandée.")

    return MaterializeResult(metadata={
        "source_mode": config.source_mode,
        "period": f"{config.start_date} -> {config.end_date} (exclu)",
        "days_ingested": ingested,
        "days_skipped_already_present": skipped,
        "days_empty": empty,
        "rows_ingested": rows_total,
        "megabytes_ingested": round(bytes_total / 1e6, 2),
    })
