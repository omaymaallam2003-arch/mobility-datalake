"""GOLD : tables analytiques prêtes pour la BI, écrites en Parquet dans gold/<table>/."""
import csv
from pathlib import Path

from dagster import AssetExecutionContext, MaterializeResult, asset
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from ..config import SILVER_TRIPS, gold_path, s3a
from ..resources import SparkResource
from ..transformations import gold as G
from .silver import silver_trips

SEED_COMMUNITY_AREAS = Path(__file__).resolve().parent.parent / "seeds" / "community_areas.csv"


def _silver(spark: SparkResource) -> DataFrame:
    return spark.session().read.parquet(s3a(SILVER_TRIPS))


def _write(df: DataFrame, table: str) -> MaterializeResult:
    path = gold_path(table)
    out = df.coalesce(1).persist()   # petites tables : un seul fichier Parquet
    n = out.count()
    out.write.mode("overwrite").parquet(path)
    out.unpersist()
    return MaterializeResult(metadata={"rows": n, "path": path})


@asset(deps=[silver_trips], group_name="gold", compute_kind="pyspark",
       description="Revenus (total, fare, tips…) et nombre de trajets par jour.")
def gold_daily_revenue(context: AssetExecutionContext, spark: SparkResource):
    return _write(G.daily_revenue(_silver(spark)), "daily_revenue")


@asset(deps=[silver_trips], group_name="gold", compute_kind="pyspark",
       description="Durée moyenne/médiane et distance moyenne des trajets, par jour et par heure.")
def gold_trip_duration_by_hour(context: AssetExecutionContext, spark: SparkResource):
    return _write(G.trip_duration_by_hour(_silver(spark)), "trip_duration_by_hour")


@asset(deps=[silver_trips], group_name="gold", compute_kind="pyspark",
       description="Top 20 des zones (community areas) de prise en charge, avec nom de zone.")
def gold_top_pickup_zones(context: AssetExecutionContext, spark: SparkResource):
    sp = spark.session()
    with open(SEED_COMMUNITY_AREAS, encoding="utf-8") as f:
        rows = [(int(r["community_area"]), r["community_area_name"]) for r in csv.DictReader(f)]
    schema = StructType([StructField("community_area", IntegerType(), False),
                         StructField("community_area_name", StringType(), False)])
    areas = sp.createDataFrame(rows, schema)
    return _write(G.top_pickup_zones(_silver(spark), areas, top_n=20), "top_pickup_zones")


@asset(deps=[silver_trips], group_name="gold", compute_kind="pyspark",
       description="Répartition des moyens de paiement par jour.")
def gold_payment_mix_daily(context: AssetExecutionContext, spark: SparkResource):
    return _write(G.payment_mix_daily(_silver(spark)), "payment_mix_daily")
