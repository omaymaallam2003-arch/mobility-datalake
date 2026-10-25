"""Transformations Bronze -> Silver (fonctions pures sur DataFrames, testables sans S3).

NB : les expressions Column sont construites DANS des fonctions (pas au niveau module),
car F.col() nécessite une SparkSession active.
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def _clean_str(name: str) -> Column:
    """Trim + chaîne vide -> NULL."""
    c = F.trim(F.col(name))
    return F.when(c == "", None).otherwise(c)


def _num(name: str, dtype: str = "double") -> Column:
    return F.trim(F.col(name)).cast(dtype)


def to_typed(raw: DataFrame) -> DataFrame:
    """Parsing + typage + renommage. Tout ce qui n'est pas parsable devient NULL
    (c'est ensuite les règles de qualité qui décident du rejet)."""
    return raw.select(
        _clean_str("trip_id").alias("trip_id"),
        _clean_str("taxi_id").alias("taxi_id"),
        F.trim(F.col("trip_start_timestamp")).cast("timestamp").alias("trip_start_ts"),
        F.trim(F.col("trip_end_timestamp")).cast("timestamp").alias("trip_end_ts"),
        _num("trip_seconds", "double").cast("int").alias("trip_seconds"),
        _num("trip_miles").alias("trip_miles"),
        _num("pickup_community_area").cast("int").alias("pickup_community_area"),
        _num("dropoff_community_area").cast("int").alias("dropoff_community_area"),
        _num("fare").alias("fare"),
        _num("tips").alias("tips"),
        _num("tolls").alias("tolls"),
        _num("extras").alias("extras"),
        _num("trip_total").alias("trip_total"),
        F.coalesce(F.initcap(_clean_str("payment_type")), F.lit("Unknown")).alias("payment_type"),
        F.coalesce(_clean_str("company"), F.lit("Unknown")).alias("company"),
        _num("pickup_centroid_latitude").alias("pickup_latitude"),
        _num("pickup_centroid_longitude").alias("pickup_longitude"),
        _num("dropoff_centroid_latitude").alias("dropoff_latitude"),
        _num("dropoff_centroid_longitude").alias("dropoff_longitude"),
        F.input_file_name().alias("_source_file"),
    )


def reject_rules() -> list[tuple[str, Column]]:
    """Règles de qualité, évaluées dans l'ordre : le 1er motif qui matche est retenu."""
    c = F.col
    return [
        ("missing_trip_id", c("trip_id").isNull()),
        ("invalid_start_timestamp", c("trip_start_ts").isNull()),
        ("invalid_end_timestamp", c("trip_end_ts").isNull()),
        ("end_before_start", c("trip_end_ts") < c("trip_start_ts")),
        ("invalid_trip_seconds", c("trip_seconds").isNull() | (c("trip_seconds") <= 0) | (c("trip_seconds") > 86400)),
        ("invalid_trip_miles", (c("trip_miles") < 0) | (c("trip_miles") > 500)),
        ("invalid_amount",
         c("trip_total").isNull() | (c("trip_total") < 0) | (c("trip_total") > 5000) | (c("fare") < 0)),
    ]


def add_reject_reason(df: DataFrame) -> DataFrame:
    expr = None
    for reason, cond in reject_rules():
        expr = F.when(cond, F.lit(reason)) if expr is None else expr.when(cond, F.lit(reason))
    return df.withColumn("reject_reason", expr)


def split_valid_rejected(df_with_reason: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid = df_with_reason.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    rejected = df_with_reason.filter(F.col("reject_reason").isNotNull())
    return valid, rejected


def deduplicate(valid: DataFrame) -> DataFrame:
    return valid.dropDuplicates(["trip_id"])


def enrich(valid: DataFrame) -> DataFrame:
    """Colonnes dérivées utiles aux agrégats Gold."""
    return (
        valid
        .withColumn("trip_date", F.to_date("trip_start_ts"))
        .withColumn("trip_hour", F.hour("trip_start_ts"))
        .withColumn("duration_min", F.round(F.col("trip_seconds") / 60.0, 2))
    )
