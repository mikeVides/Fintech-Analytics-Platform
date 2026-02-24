# Databricks notebook source
from datetime import date

SILVER_PATH = "/Volumes/workspace/default/fintech_raw_data/silver"
LOAD_DATE   = str(date.today())

print(f"Silver path: {SILVER_PATH}")
print(f"Load date:   {LOAD_DATE}")

# COMMAND ----------

from pyspark.sql.functions import col

df_events = spark.read.format("delta").load(f"{SILVER_PATH}/events_raw")

df_events = df_events.filter(col("is_bot") == False)

print(f"Total events loaded: {df_events.count():,}")
print(f"Columns: {df_events.columns}")
df_events.show(5)

# COMMAND ----------

from pyspark.sql.functions import lag, unix_timestamp, col
from pyspark.sql.window import Window

user_time_window = Window \
    .partitionBy("customer_id") \
    .orderBy("timestamp")

df_gaps = df_events \
    .withColumn("prev_timestamp",
        lag(col("timestamp")).over(user_time_window)) \
    .withColumn("time_gap_seconds",
        unix_timestamp(col("timestamp")) - unix_timestamp(col("prev_timestamp")))

print("Time gaps calculated.")
print("\nSample of time gaps:")
df_gaps.select("customer_id", "timestamp", "prev_timestamp", "time_gap_seconds") \
       .filter(col("customer_id").isNotNull()) \
       .show(10)

# COMMAND ----------

from pyspark.sql.functions import when, sum as spark_sum, concat, lpad

SESSION_TIMEOUT = 1800

df_boundaries = df_gaps \
    .withColumn("is_new_session",
        when(
            (col("time_gap_seconds") > SESSION_TIMEOUT) |
            (col("prev_timestamp").isNull()), True)
        .otherwise(False))

df_sessions = df_boundaries \
    .withColumn("session_number",
        spark_sum(col("is_new_session").cast("int"))
        .over(user_time_window)) \
    .withColumn("true_session_id",
        concat(col("customer_id"),
               lpad(col("session_number").cast("string"), 4, "0")))

print("Session boundaries assigned.")
print("\nSample session assignments:")
df_sessions.select("customer_id", "timestamp", "time_gap_seconds", "is_new_session", "true_session_id") \
           .filter(col("customer_id").isNotNull()) \
           .show(10)

# COMMAND ----------

from pyspark.sql.functions import min, max, count, first, last

df_session_agg = df_sessions \
    .groupBy("true_session_id", "customer_id") \
    .agg(
        min("timestamp").alias("session_start"),
        max("timestamp").alias("session_end"),
        count("event_id").alias("event_count"),
        first("page").alias("entry_page"),
        last("page").alias("exit_page"),
        max(when(col("event_type") == "conversion", True)
            .otherwise(False)).alias("converted"),
        first("device").alias("device"),
        first("utm_source").alias("utm_source"),
        first("utm_campaign").alias("utm_campaign")
    ) \
    .withColumn("session_duration_seconds",
        unix_timestamp(col("session_end")) - unix_timestamp(col("session_start")))

print(f"Total sessions created: {df_session_agg.count():,}")
print("\nSample session aggregations:")
df_session_agg.show(5)

# COMMAND ----------

from pyspark.sql.functions import lit

df_session_agg = df_session_agg.withColumn("silver_load_date", lit(LOAD_DATE))

df_session_agg.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("silver_load_date") \
    .option("overwriteSchema", "true") \
    .save(f"{SILVER_PATH}/sessions")

row_count = spark.read.format("delta").load(f"{SILVER_PATH}/sessions").count()

print(f"Silver sessions table written successfully.")
print(f"Total rows: {row_count:,}")
print(f"Columns: {spark.read.format('delta').load(f'{SILVER_PATH}/sessions').columns}")