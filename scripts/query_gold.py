#!/usr/bin/env python3
"""Couche d'exposition : interroge les tables Gold en SQL (DuckDB lit le Parquet directement sur MinIO)."""
import os

import duckdb

endpoint = os.getenv("S3_ENDPOINT_URL", "http://localhost:9000")
bucket = os.getenv("S3_BUCKET", "datalake")

con = duckdb.connect()
con.sql("LOAD httpfs")
con.sql(f"SET s3_endpoint='{endpoint.replace('http://', '').replace('https://', '')}'")
con.sql(f"SET s3_use_ssl={'true' if endpoint.startswith('https') else 'false'}")
con.sql("SET s3_url_style='path'")
con.sql(f"SET s3_access_key_id='{os.getenv('S3_ACCESS_KEY', 'minioadmin')}'")
con.sql(f"SET s3_secret_access_key='{os.getenv('S3_SECRET_KEY', 'minioadmin')}'")

for t in ("daily_revenue", "trip_duration_by_hour", "top_pickup_zones", "payment_mix_daily"):
    con.sql(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('s3://{bucket}/gold/{t}/*.parquet')")

QUERIES = {
    "Revenus par jour": "SELECT * FROM daily_revenue ORDER BY trip_date",
    "Durée moyenne des trajets par jour": """
        SELECT trip_date, round(sum(avg_duration_min * trips) / sum(trips), 2) AS avg_duration_min, sum(trips) AS trips
        FROM trip_duration_by_hour GROUP BY trip_date ORDER BY trip_date""",
    "Top 10 zones de prise en charge": "SELECT * FROM top_pickup_zones ORDER BY rank LIMIT 10",
    "Mix de paiement (période)": """
        SELECT payment_type, sum(trips) AS trips, round(sum(total_revenue), 2) AS revenue
        FROM payment_mix_daily GROUP BY payment_type ORDER BY trips DESC""",
}

for title, q in QUERIES.items():
    print(f"\n=== {title} ===")
    con.sql(q).show(max_rows=20, max_width=200)
