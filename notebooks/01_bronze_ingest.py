# Databricks notebook source
# Bronze ingestion via Auto Loader: append-only + idempotent.
# Parameterized by catalog + schema_prefix so the same code runs in any
# environment and any developer's sandbox namespace.

import re
import pyspark.sql.functions as F

dbutils.widgets.text("catalog", "spotify_dev")
dbutils.widgets.text("schema_prefix", "")

CATALOG = dbutils.widgets.get("catalog")
prefix  = dbutils.widgets.get("schema_prefix").strip()
SCHEMA  = re.sub(r"\W+", "_", (f"{prefix}_bronze" if prefix else "bronze")).lower()

# Self-provision the target schema + checkpoint volume (idempotent).
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")

BRONZE_SCHEMA   = f"{CATALOG}.{SCHEMA}"
VOLUME_PATH     = f"/Volumes/{CATALOG}/bronze/raw_files"      # shared raw source
CHECKPOINT_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

SOURCES = {"artists.csv": "artists_raw", "tracks.csv": "tracks_raw", "plays.csv": "plays_raw"}

# COMMAND ----------

def ingest_to_bronze(file_name: str, table_name: str) -> None:
    schema_loc = f"{CHECKPOINT_BASE}/{table_name}/schema"
    checkpoint = f"{CHECKPOINT_BASE}/{table_name}/checkpoint"
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_loc)
        .option("cloudFiles.inferColumnTypes", "false")   # all STRING (lossless bronze)
        .option("header", "true")
        .option("pathGlobFilter", file_name)
        .load(VOLUME_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
    (
        df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .toTable(f"{BRONZE_SCHEMA}.{table_name}")
        .awaitTermination()
    )
    print(f"Ingested {file_name} -> {BRONZE_SCHEMA}.{table_name}")

# COMMAND ----------

for f, t in SOURCES.items():
    ingest_to_bronze(f, t)

# COMMAND ----------

for t in SOURCES.values():
    print(f"{BRONZE_SCHEMA}.{t}: {spark.table(f'{BRONZE_SCHEMA}.{t}').count()} rows")