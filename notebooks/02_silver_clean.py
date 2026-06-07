# Databricks notebook source
# Silver layer: type (lenient), SCD1 dedup, validate, quarantine, MERGE.
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
    """SCD Type 1: keep the most-recently-ingested row per key (deterministic)."""
    w = Window.partitionBy(key).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def upsert(df: DataFrame, table: str, key: str) -> None:
    """Idempotent SCD1 write: create on first run, else MERGE-overwrite on the key."""
    if not spark.catalog.tableExists(table):
        df.write.format("delta").saveAsTable(table)
    else:
        (DeltaTable.forName(spark, table).alias("t")
            .merge(df.alias("s"), f"t.{key} = s.{key}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())


def split_valid(df: DataFrame, checks: list):
    """Split into (valid, quarantine). `checks` = list of (condition, reason);
    condition True means BAD. concat_ws skips null args, so an empty reason
    string == all checks passed."""
    reason = F.concat_ws(",", *[F.when(cond, F.lit(r)) for cond, r in checks])
    tagged = df.withColumn("dq_reason", reason)
    valid = tagged.filter(F.col("dq_reason") == "").drop("dq_reason")
    quarantine = tagged.filter(F.col("dq_reason") != "")
    return valid, quarantine

# COMMAND ----------
# ----- ARTISTS -----
artists = dedup_latest(spark.table(f"{BRONZE}.artists_raw"), "artist_id") \
    .select("artist_id", "name", "genre", "country")
upsert(artists, f"{SILVER}.artists", "artist_id")
print(f"{SILVER}.artists: {spark.table(f'{SILVER}.artists').count()} rows")

# COMMAND ----------
# ----- TRACKS -----  (try_cast: bad values -> null, then quarantined)
tracks_typed = (
    dedup_latest(spark.table(f"{BRONZE}.tracks_raw"), "track_id")
    .withColumn("duration_ms", F.col("duration_ms").cast("double").cast("int"))
    .withColumn("popularity",   F.expr("try_cast(popularity as int)"))
    .withColumn("release_date", F.expr("try_cast(release_date as date)"))
)
tracks_valid, tracks_bad = split_valid(tracks_typed, [
    ((F.col("artist_id").isNull()) | (F.col("artist_id") == ""), "null_artist_id"),
    ((F.col("duration_ms").isNull()) | (F.col("duration_ms") <= 0), "bad_duration"),
    ((F.col("popularity").isNull()) | (F.col("popularity") < 0) | (F.col("popularity") > 100), "popularity_out_of_range"),
])
upsert(tracks_valid.select("track_id","artist_id","title","duration_ms","popularity","release_date"),
       f"{SILVER}.tracks", "track_id")
upsert(tracks_bad, f"{SILVER}.tracks_quarantine", "track_id")
print(f"{SILVER}.tracks: {spark.table(f'{SILVER}.tracks').count()} | "
      f"quarantine: {spark.table(f'{SILVER}.tracks_quarantine').count()}")

# COMMAND ----------
# ----- PLAYS -----  (try_cast handles the 'not_a_date' timestamps)
plays_typed = (
    dedup_latest(spark.table(f"{BRONZE}.plays_raw"), "play_id")
    .withColumn("played_at", F.expr("try_cast(played_at as timestamp)"))
    .withColumn("ms_played", F.expr("try_cast(ms_played as int)"))
)
plays_valid, plays_bad = split_valid(plays_typed, [
    (F.col("played_at").isNull(), "invalid_timestamp"),
    ((F.col("track_id").isNull()) | (F.col("track_id") == ""), "null_track_id"),
])
upsert(plays_valid.select("play_id","track_id","user_id","played_at","ms_played"),
       f"{SILVER}.plays", "play_id")
upsert(plays_bad, f"{SILVER}.plays_quarantine", "play_id")
print(f"{SILVER}.plays: {spark.table(f'{SILVER}.plays').count()} | "
      f"quarantine: {spark.table(f'{SILVER}.plays_quarantine').count()}")