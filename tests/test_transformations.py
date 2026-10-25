"""Tests des transformations Spark (Spark local, sans S3). Ignorés si pyspark est absent."""
import unittest
from datetime import date

try:
    from pyspark.sql import SparkSession
    HAS_SPARK = True
except ImportError:  # pragma: no cover
    HAS_SPARK = False

if HAS_SPARK:
    from pipeline.transformations import gold as G
    from pipeline.transformations import silver as S

RAW_COLS = ["trip_id", "taxi_id", "trip_start_timestamp", "trip_end_timestamp", "trip_seconds",
            "trip_miles", "pickup_community_area", "dropoff_community_area", "fare", "tips", "tolls",
            "extras", "trip_total", "payment_type", "company", "pickup_centroid_latitude",
            "pickup_centroid_longitude", "dropoff_centroid_latitude", "dropoff_centroid_longitude"]


def raw_row(**kw):
    base = dict(trip_id="a", taxi_id="t", trip_start_timestamp="2023-01-01T08:00:00.000",
                trip_end_timestamp="2023-01-01T08:10:00.000", trip_seconds="600", trip_miles="2.5",
                pickup_community_area="8", dropoff_community_area="32", fare="10.0", tips="2.0",
                tolls="0", extras="1", trip_total="13.0", payment_type="credit card", company=" Flash Cab ",
                pickup_centroid_latitude="41.9", pickup_centroid_longitude="-87.6",
                dropoff_centroid_latitude="41.8", dropoff_centroid_longitude="-87.7")
    base.update(kw)
    return tuple(base[c] for c in RAW_COLS)


@unittest.skipUnless(HAS_SPARK, "pyspark non installé")
class TestSparkTransformations(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[1]").appName("tests")
                     .config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false")
                     .config("spark.sql.session.timeZone", "UTC").getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def _pipeline(self, rows):
        raw = self.spark.createDataFrame(rows, RAW_COLS)
        with_reason = S.add_reject_reason(S.to_typed(raw))
        valid, rejected = S.split_valid_rejected(with_reason)
        return S.enrich(S.deduplicate(valid)), rejected

    def test_rejection_reasons(self):
        rows = [
            raw_row(trip_id="ok"),
            raw_row(trip_id=""),
            raw_row(trip_id="neg", fare="-1", trip_total="-1"),
            raw_row(trip_id="rev", trip_end_timestamp="2023-01-01T07:00:00.000"),
            raw_row(trip_id="zero", trip_seconds="0"),
            raw_row(trip_id="badts", trip_start_timestamp="garbage"),
        ]
        valid, rejected = self._pipeline(rows)
        self.assertEqual([r.trip_id for r in valid.collect()], ["ok"])
        reasons = {r.trip_id: r.reject_reason for r in rejected.collect()}
        self.assertEqual(reasons["neg"], "invalid_amount")
        self.assertEqual(reasons["rev"], "end_before_start")
        self.assertEqual(reasons["zero"], "invalid_trip_seconds")
        self.assertEqual(reasons["badts"], "invalid_start_timestamp")
        self.assertEqual(rejected.filter("reject_reason = 'missing_trip_id'").count(), 1)

    def test_dedup_and_normalization(self):
        valid, _ = self._pipeline([raw_row(trip_id="x"), raw_row(trip_id="x")])
        rows = valid.collect()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].payment_type, "Credit Card")
        self.assertEqual(rows[0].company, "Flash Cab")
        self.assertEqual(rows[0].trip_date, date(2023, 1, 1))
        self.assertEqual(rows[0].trip_hour, 8)
        self.assertEqual(rows[0].duration_min, 10.0)

    def test_gold_daily_revenue_and_top_zones(self):
        rows = [raw_row(trip_id="1"), raw_row(trip_id="2", pickup_community_area="32"),
                raw_row(trip_id="3", pickup_community_area="32")]
        valid, _ = self._pipeline(rows)
        d = G.daily_revenue(valid).collect()[0]
        self.assertEqual(d.trips, 3)
        self.assertAlmostEqual(d.total_revenue, 39.0)
        areas = self.spark.createDataFrame([(8, "Near North Side"), (32, "Loop")],
                                           "community_area int, community_area_name string")
        top = G.top_pickup_zones(valid, areas, top_n=5).collect()
        self.assertEqual((top[0].rank, top[0].community_area_name, top[0].trips), (1, "Loop", 2))
        self.assertEqual(top[1].community_area_name, "Near North Side")


if __name__ == "__main__":
    unittest.main()
