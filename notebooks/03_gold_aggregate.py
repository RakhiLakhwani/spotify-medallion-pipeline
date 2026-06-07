# Databricks notebook source
# Gold layer: date-grained, analytics-ready aggregates from silver.
#
# Design choices:
# - Every fact is grained by play_date (+ entity), so you can query a single
#   day OR a date range, and roll up to all-time by summing over dates.
# - Writes are KEYED upserts (MERGE on the grain), so days update in place,
#   history is preserved, and recompute can be incremental.
# - We store ADDITIVE components (counts, sums) so range aggregation is correct.
#   Averages are derived as SUM(sum_x)/SUM(count) — never AVG of daily AVGs.
#   NOTE: distinct counts (unique listeners) are NOT range-additive — a per-day
#   value is exact for that day, but a range must be recomputed from silver
#   (or use HLL sketches at scale). We keep per-day exact counts here.

import re
from delta.tables import DeltaTable
from pyspark.sql import functions as F, DataFrame

dbutils.widgets.text("catalog", "spotify_dev")
dbutils.widgets.text("schema_prefix", "")
CATALOG = dbutils.widgets.get("catalog")
prefix  = dbutils.widgets.get("schema_prefix").strip()

def _schema(layer: str) -> str:
    return re.sub(r"\W+", "_", (f"{prefix}_{layer}" if prefix else layer)).lower()

SILVER = f"{CATALOG}.{_schema('silver')}"
GOLD   = f"{CATALOG}.{_schema('gold')}"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD}")

# COMMAND ----------

def upsert_gold(df: DataFrame, name: str, keys: list) -> None:
    """Keyed upsert on one or more grain columns (e.g. [play_date, track_id]).

    Updates matching rows, inserts new ones, leaves untouched keys intact —
    so gold is incrementally maintainable, not rebuilt every run.
    """
    table = f"{GOLD}.{name}"
    cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    if not spark.catalog.tableExists(table):
        df.write.format("delta").saveAsTable(table)
    else:
        (DeltaTable.forName(spark, table).alias("t")
            .merge(df.alias("s"), cond)
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    print(f"{table}: {spark.table(table).count()} rows")

# COMMAND ----------
# Enriched fact: plays + track + artist, with completion_rate and play_date.
plays   = spark.table(f"{SILVER}.plays")
tracks  = spark.table(f"{SILVER}.tracks")
artists = spark.table(f"{SILVER}.artists")

pe = (
    plays.join(tracks, "track_id", "inner")
         .join(artists, "artist_id", "inner")
         .withColumn("completion_rate",
                     F.least(F.col("ms_played") / F.col("duration_ms"), F.lit(1.0)))
         .withColumn("play_date", F.to_date("played_at"))
)

# COMMAND ----------
# ---- daily_top_tracks: grain (play_date, track_id) ----
daily_top_tracks = (
    pe.groupBy("play_date", "track_id", "title", F.col("name").alias("artist_name"))
      .agg(
          F.count("*").alias("total_plays"),                                  # additive
          F.round(F.sum("ms_played") / 3_600_000, 2).alias("listening_hours"),# additive
          F.round(F.sum("completion_rate"), 3).alias("sum_completion"),       # component
      )
      # per-day convenience metric; for ranges use SUM(sum_completion)/SUM(total_plays)
      .withColumn("avg_completion_rate", F.round(F.col("sum_completion") / F.col("total_plays"), 3))
)
upsert_gold(daily_top_tracks, "daily_top_tracks", ["play_date", "track_id"])

# COMMAND ----------
# ---- daily_top_artists: grain (play_date, artist_id) ----
daily_top_artists = (
    pe.groupBy("play_date", "artist_id", F.col("name").alias("artist_name"), "genre", "country")
      .agg(
          F.count("*").alias("total_plays"),                                  # additive
          F.round(F.sum("ms_played") / 3_600_000, 2).alias("listening_hours"),# additive
          F.round(F.sum("completion_rate"), 3).alias("sum_completion"),       # component
          F.countDistinct("user_id").alias("unique_listeners"),              # per-day exact (NOT range-additive)
      )
      .withColumn("avg_completion_rate", F.round(F.col("sum_completion") / F.col("total_plays"), 3))
)
upsert_gold(daily_top_artists, "daily_top_artists", ["play_date", "artist_id"])

# COMMAND ----------
# ---- daily_listening: grain (play_date) ----
daily_listening = (
    pe.groupBy("play_date")
      .agg(
          F.count("*").alias("total_plays"),                                  # additive
          F.round(F.sum("ms_played") / 3_600_000, 2).alias("listening_hours"),# additive
          F.countDistinct("user_id").alias("daily_active_users"),            # per-day exact
      )
)
upsert_gold(daily_listening, "daily_listening", ["play_date"])

# COMMAND ----------
# ---- daily_genre: grain (play_date, genre) ----
daily_genre = (
    pe.groupBy("play_date", "genre")
      .agg(
          F.count("*").alias("total_plays"),
          F.round(F.sum("completion_rate"), 3).alias("sum_completion"),
          F.countDistinct("user_id").alias("unique_listeners"),
      )
      .withColumn("avg_completion_rate", F.round(F.col("sum_completion") / F.col("total_plays"), 3))
)
upsert_gold(daily_genre, "daily_genre", ["play_date", "genre"])