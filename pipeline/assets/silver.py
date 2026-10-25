"""SILVER : parsing, typage, contrôle qualité, déduplication.

- données valides  -> silver/trips (Parquet, partitionné par trip_date)
- données rejetées -> silver/trips_rejected (quarantaine avec motif, pour audit)
- asset check bloquant : le ratio de rejet doit rester sous REJECT_RATIO_THRESHOLD
"""
from dagster import AssetCheckResult, AssetCheckSpec, AssetExecutionContext, MaterializeResult, asset
from pyspark.sql import functions as F

from ..config import BRONZE_PREFIX, REJECT_RATIO_THRESHOLD, SILVER_REJECTED, SILVER_TRIPS, s3a
from ..resources import SparkResource
from ..transformations import silver as T
from .bronze import bronze_trips

CHECK_NAME = "reject_ratio_below_threshold"


@asset(
    deps=[bronze_trips],
    group_name="silver",
    compute_kind="pyspark",
    description="Trajets nettoyés, typés et dédupliqués (Parquet partitionné par jour).",
    check_specs=[AssetCheckSpec(name=CHECK_NAME, asset="silver_trips", blocking=True)],
)
def silver_trips(context: AssetExecutionContext, spark: SparkResource) -> MaterializeResult:
    sp = spark.session()

    raw = (
        sp.read.option("header", True)
        .csv(s3a(BRONZE_PREFIX))      # la partition trip_date=... est découverte comme colonne
        .drop("trip_date")            # on la recalcule depuis trip_start_ts (source de vérité)
    )
    typed = T.add_reject_reason(T.to_typed(raw)).persist()

    valid_raw, rejected = T.split_valid_rejected(typed)
    valid = T.enrich(T.deduplicate(valid_raw)).persist()

    total = typed.count()
    n_valid_before_dedup = valid_raw.count()
    n_valid = valid.count()
    n_duplicates = n_valid_before_dedup - n_valid
    n_rejected = total - n_valid_before_dedup
    reasons = {r["reject_reason"]: r["count"] for r in
               rejected.groupBy("reject_reason").count().collect()}

    valid.write.mode("overwrite").partitionBy("trip_date").parquet(s3a(SILVER_TRIPS))
    rejected.drop("_source_file").write.mode("overwrite").parquet(s3a(SILVER_REJECTED))

    typed.unpersist()
    valid.unpersist()

    ratio = (n_rejected + n_duplicates) / total if total else 1.0
    context.log.info(f"silver: total={total} valides={n_valid} rejetés={n_rejected} doublons={n_duplicates}")

    passed = bool(total > 0 and ratio <= REJECT_RATIO_THRESHOLD)
    return MaterializeResult(
        metadata={
            "rows_in": total,
            "rows_valid": n_valid,
            "rows_rejected": n_rejected,
            "rows_duplicates_dropped": n_duplicates,
            "reject_ratio": round(ratio, 4),
            "reject_reasons": {k: int(v) for k, v in reasons.items()},
        },
        check_results=[
            AssetCheckResult(
                check_name=CHECK_NAME,
                passed=passed,
                metadata={"reject_ratio": round(ratio, 4), "threshold": REJECT_RATIO_THRESHOLD},
            )
        ],
    )
