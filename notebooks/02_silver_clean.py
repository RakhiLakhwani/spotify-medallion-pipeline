# Databricks notebook source
# Silver layer: type, deduplicate (SCD Type 1), validate, quarantine, MERGE.
#
# Reads bronze (all-string) tables and produces typed, cleaned silver tables.
# - Dedup is SCD1: keep the most-recently-ingested row per natural key.
# - Bad rows are not dropped; they go to *_quarantine tables with a reason.
# - Writes are idempotent MERGE upserts keyed on the primary key.

import re
from delta.tables import DeltaTable
from pyspark.sql import functions as F, DataFrame
from pyspark.sql.window import Window

dbutils.widgets.text("catalog", "spotify_dev")
dbutils.widgets.text("schema_prefix", "")
CATALOG = dbutils.widgets.get("catalog")
prefix  = dbutils.widgets.get("schema_prefix").strip()

def _schema(layer: str) -> str:
    return re.sub(r"\W+", "_", (f"{prefix}_{layer}" if prefix else layer)).lower()

BRONZE = f"{CATALOG}.{_schema('bronze')}"
SILVER = f"{CATALOG}.{_schema('silver')}"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER}")

# COMMAND ----------

def dedup_latest(df: DataFrame, key: str, order_col: str = "_ingested_at") -> DataFrame:
    """Keep one row per key — the most recently ingested (SCD Type 1 'current').

    Deterministic: ranks rows within each key by recency, keeps rank 1.
    Handles exact duplicates within a batch AND a key re-arriving later with
    updated attributes — the newest ingest always wins.
    """
    w = Window.partitionBy(key).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )


def upsert(df: DataFrame, table: str, key: str) -> None:
    """Idempotent SCD Type 1 write: create on first run, else MERGE on the key.

    whenMatchedUpdateAll overwrites attributes in place (SCD1 — current only,
    no history). Re-running silver therefore never duplicates rows.
    """
    if not spark.catalog.tableExists(table):
        df.write.format("delta").saveAsTable(table)
    else:
        (DeltaTable.forName(spark, table).alias("t")
            .merge(df.alias("s"), f"t.{key} = s.{key}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())


def split_valid(df: DataFrame, checks: list):
    """Tag rows with failed-check reasons, then split into (valid, quarantine).

    `checks` is a list of (condition, reason); condition True means BAD.
    array_compact keeps only the reasons that fired. Valid rows have an empty
    reason array; quarantined rows retain their reasons for auditing.
    """
    reason = F.array_compact(F.array(
        *[F.when(cond, F.lit(r)) for cond, r in checks]
    ))
    tagged = df.withColumn("dq_reason", reason)
    valid = tagged.filter(F.size("dq_reason") == 0).drop("dq_reason")
    quarantine = tagged.filter(F.size("dq_reason") > 0)
    return valid, quarantine

# COMMAND ----------
# ----- ARTISTS: clean dimension (SCD1 dedup + passthrough) -----
artists = (
    dedup_latest(spark.table(f"{BRONZE}.artists_raw"), key="artist_id")
    .select("artist_id", "name", "genre", "country")
)
upsert(artists, f"{SILVER}.artists", key="artist_id")
print(f"{SILVER}.artists: {spark.table(f'{SILVER}.artists').count()} rows")

# COMMAND ----------
# ----- TRACKS: SCD1 dedup -> cast -> validate -> quarantine -----
tracks_typed = (
    dedup_latest(spark.table(f"{BRONZE}.tracks_raw"), key="track_id")
    .withColumn("duration_ms", F.col("duration_ms").cast("int"))
    .withColumn("popularity",  F.col("popularity").cast("int"))
    .withColumn("release_date", F.to_date("release_date"))
)

tracks_valid, tracks_bad = split_valid(tracks_typed, [
    ((F.col("artist_id").isNull()) | (F.col("artist_id") == ""), "null_artist_id"),
    (F.col("duration_ms") <= 0,                                  "non_positive_duration"),
    ((F.col("popularity") < 0) | (F.col("popularity") > 100),    "popularity_out_of_range"),
])

upsert(
    tracks_valid.select("track_id", "artist_id", "title",
                        "duration_ms", "popularity", "release_date"),
    f"{SILVER}.tracks", key="track_id",
)
upsert(tracks_bad, f"{SILVER}.tracks_quarantine", key="track_id")
print(f"{SILVER}.tracks: {spark.table(f'{SILVER}.tracks').count()} | "
      f"quarantine: {spark.table(f'{SILVER}.tracks_quarantine').count()}")

# COMMAND ----------
# ----- PLAYS: SCD1 dedup -> cast -> validate -> quarantine -----
plays_typed = (
    dedup_latest(spark.table(f"{BRONZE}.plays_raw"), key="play_id")
    .withColumn("played_at", F.to_timestamp("played_at"))   # malformed -> null
    .withColumn("ms_played", F.col("ms_played").cast("int"))
)

plays_valid, plays_bad = split_valid(plays_typed, [
    (F.col("played_at").isNull(), "invalid_timestamp"),
    (F.col("track_id").isNull(),  "null_track_id"),
])

upsert(
    plays_valid.select("play_id", "track_id", "user_id", "played_at", "ms_played"),
    f"{SILVER}.plays", key="play_id",
)
upsert(plays_bad, f"{SILVER}.plays_quarantine", key="play_id")
print(f"{SILVER}.plays: {spark.table(f'{SILVER}.plays').count()} | "
      f"quarantine: {spark.table(f'{SILVER}.plays_quarantine').count()}")