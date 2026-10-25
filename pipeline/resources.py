"""Ressources Dagster : accès S3 (MinIO) et session Spark."""
from __future__ import annotations

from dagster import ConfigurableResource


class S3Resource(ConfigurableResource):
    endpoint_url: str
    access_key: str
    secret_key: str
    bucket: str

    def client(self):
        import boto3
        from botocore.config import Config

        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name="us-east-1",
            config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 5}),
        )

    def ensure_bucket(self) -> None:
        from botocore.exceptions import ClientError

        c = self.client()
        try:
            c.head_bucket(Bucket=self.bucket)
        except ClientError:
            c.create_bucket(Bucket=self.bucket)

    def exists(self, key: str) -> bool:
        from botocore.exceptions import ClientError

        try:
            self.client().head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False


class SparkResource(ConfigurableResource):
    master: str = "local[*]"
    driver_memory: str = "2g"
    shuffle_partitions: int = 8
    s3_endpoint_url: str
    s3_access_key: str
    s3_secret_key: str

    def session(self):
        """Retourne la SparkSession (singleton du process).

        Le job utilise l'executor in-process : une seule JVM Spark par run.
        La JVM est libérée à la fin du process du run, on ne stoppe pas la session entre assets.
        """
        from pyspark.sql import SparkSession

        endpoint = self.s3_endpoint_url.replace("http://", "").replace("https://", "")
        ssl = self.s3_endpoint_url.startswith("https://")
        spark = (
            SparkSession.builder.appName("mobility-datalake")
            .master(self.master)
            .config("spark.driver.memory", self.driver_memory)
            .config("spark.sql.shuffle.partitions", str(self.shuffle_partitions))
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.ui.showConsoleProgress", "false")
            # --- S3A -> MinIO ---
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.access.key", self.s3_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", self.s3_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(ssl).lower())
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        return spark
