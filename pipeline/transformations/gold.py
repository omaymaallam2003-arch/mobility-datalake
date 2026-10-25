"""Agrégats Silver -> Gold (tables prêtes pour la BI)."""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def daily_revenue(silver: DataFrame) -> DataFrame:
    return (
        silver.groupBy("trip_date")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.sum("trip_total"), 2).alias("total_revenue"),
            F.round(F.sum("fare"), 2).alias("total_fare"),
            F.round(F.sum("tips"), 2).alias("total_tips"),
            F.round(F.sum("tolls"), 2).alias("total_tolls"),
            F.round(F.sum("extras"), 2).alias("total_extras"),
            F.round(F.avg("trip_total"), 2).alias("avg_trip_total"),
        )
        .withColumn(
            "tip_rate",
            F.when(F.col("total_fare") > 0, F.round(F.col("total_tips") / F.col("total_fare"), 4)),
        )
        .orderBy("trip_date")
    )


def trip_duration_by_hour(silver: DataFrame) -> DataFrame:
    return (
        silver.groupBy("trip_date", "trip_hour")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
            F.round(F.expr("percentile_approx(duration_min, 0.5)"), 2).alias("median_duration_min"),
            F.round(F.avg("trip_miles"), 2).alias("avg_distance_miles"),
        )
        .orderBy("trip_date", "trip_hour")
    )


def top_pickup_zones(silver: DataFrame, community_areas: DataFrame, top_n: int = 20) -> DataFrame:
    """Top zones de prise en charge (jointure avec le référentiel des community areas)."""
    agg = (
        silver.filter(F.col("pickup_community_area").isNotNull())
        .groupBy("pickup_community_area")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.sum("trip_total"), 2).alias("total_revenue"),
            F.round(F.avg("trip_total"), 2).alias("avg_trip_total"),
            F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
        )
    )
    joined = agg.join(
        F.broadcast(community_areas),
        agg["pickup_community_area"] == community_areas["community_area"],
        "left",
    ).drop("community_area")
    w = Window.orderBy(F.desc("trips"), F.asc("pickup_community_area"))
    return (
        joined.withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") <= top_n)
        .select("rank", "pickup_community_area",
                F.coalesce("community_area_name", F.lit("Unknown")).alias("community_area_name"),
                "trips", "total_revenue", "avg_trip_total", "avg_duration_min")
        .orderBy("rank")
    )


def payment_mix_daily(silver: DataFrame) -> DataFrame:
    return (
        silver.groupBy("trip_date", "payment_type")
        .agg(F.count("*").alias("trips"), F.round(F.sum("trip_total"), 2).alias("total_revenue"))
        .withColumn(
            "trips_share",
            F.round(F.col("trips") / F.sum("trips").over(Window.partitionBy("trip_date")), 4),
        )
        .orderBy("trip_date", F.desc("trips"))
    )
