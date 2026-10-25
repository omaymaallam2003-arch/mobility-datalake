FROM python:3.11-slim-bookworm

# Java (requis par Spark) + utilitaires
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/app
ENV PYTHONPATH=/opt/app \
    PYTHONUNBUFFERED=1 \
    DAGSTER_HOME=/opt/dagster_home

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Connecteur S3A (Spark <-> MinIO). Versions alignées sur Hadoop 3.3.4, embarqué par PySpark 3.5.1.
RUN SPARK_JARS="$(python -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')" \
 && curl -fsSL -o "$SPARK_JARS/hadoop-aws-3.3.4.jar" \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
 && curl -fsSL -o "$SPARK_JARS/aws-java-sdk-bundle-1.12.262.jar" \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Extension DuckDB pour interroger le Gold en SQL (couche d'exposition)
RUN python -c "import duckdb; duckdb.sql('INSTALL httpfs')"

COPY . .
RUN mkdir -p "$DAGSTER_HOME" && cp dagster.yaml "$DAGSTER_HOME/dagster.yaml"

EXPOSE 3000
CMD ["dagster", "dev", "-w", "workspace.yaml", "-h", "0.0.0.0", "-p", "3000"]
