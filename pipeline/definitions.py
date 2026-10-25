"""Point d'entrée Dagster : assets, job et ressources."""
import os

from dagster import AssetSelection, Definitions, define_asset_job, in_process_executor

from .assets.bronze import bronze_trips
from .assets.gold import (
    gold_daily_revenue,
    gold_payment_mix_daily,
    gold_top_pickup_zones,
    gold_trip_duration_by_hour,
)
from .assets.silver import silver_trips
from .config import BUCKET
from .resources import S3Resource, SparkResource

S3_ENDPOINT = os.getenv("S3_ENDPOINT_URL", "http://localhost:9000")
S3_ACCESS = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET = os.getenv("S3_SECRET_KEY", "minioadmin")

# Executor in-process : les assets Spark s'exécutent séquentiellement dans UN process
# => une seule JVM Spark par run (évite N JVM en parallèle qui saturent la mémoire).
medallion_pipeline = define_asset_job(
    name="medallion_pipeline",
    selection=AssetSelection.all(),
    executor_def=in_process_executor,
    description="Bronze (ingestion) -> Silver (nettoyage) -> Gold (agrégats BI).",
)

defs = Definitions(
    assets=[
        bronze_trips,
        silver_trips,
        gold_daily_revenue,
        gold_trip_duration_by_hour,
        gold_top_pickup_zones,
        gold_payment_mix_daily,
    ],
    jobs=[medallion_pipeline],
    resources={
        "s3": S3Resource(endpoint_url=S3_ENDPOINT, access_key=S3_ACCESS, secret_key=S3_SECRET, bucket=BUCKET),
        "spark": SparkResource(
            driver_memory=os.getenv("SPARK_DRIVER_MEMORY", "2g"),
            s3_endpoint_url=S3_ENDPOINT,
            s3_access_key=S3_ACCESS,
            s3_secret_key=S3_SECRET,
        ),
    },
)
