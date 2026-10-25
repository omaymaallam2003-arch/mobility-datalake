"""Configuration centrale : chemins du datalake et paramètres lus depuis l'environnement."""
import os

BUCKET = os.getenv("S3_BUCKET", "datalake")

# Layout médaillon dans le bucket
BRONZE_PREFIX = "bronze/chicago_taxi"          # CSV bruts, partitionnés par trip_date=YYYY-MM-DD
SILVER_TRIPS = "silver/trips"                  # Parquet, partitionné par trip_date
SILVER_REJECTED = "silver/trips_rejected"      # Quarantaine : lignes rejetées + motif
GOLD_PREFIX = "gold"                           # gold/<table>/


def s3a(key: str) -> str:
    """URI lisible par Spark (connecteur S3A)."""
    return f"s3a://{BUCKET}/{key}"


def gold_path(table: str) -> str:
    return s3a(f"{GOLD_PREFIX}/{table}")


REJECT_RATIO_THRESHOLD = float(os.getenv("REJECT_RATIO_THRESHOLD", "0.10"))
