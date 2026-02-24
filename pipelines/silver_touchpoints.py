# Databricks notebook source
from datetime import date

SILVER_PATH = "/Volumes/workspace/default/fintech_raw_data/silver"
LOAD_DATE   = str(date.today())

print(f"Silver path: {SILVER_PATH}")
print(f"Load date:   {LOAD_DATE}")

# COMMAND ----------

df_touches   = spark.read.format("delta").load(f"{SILVER_PATH}/marketing_touches")
df_sessions  = spark.read.format("delta").load(f"{SILVER_PATH}/sessions")
df_customers = spark.read.format("delta").load(f"{SILVER_PATH}/customers")

print(f"Marketing touches loaded: {df_touches.count():,} rows")
print(f"Sessions loaded:          {df_sessions.count():,} rows")
print(f"Customers loaded:         {df_customers.count():,} rows")

# COMMAND ----------

from pyspark.sql.functions import col

df_touches_enriched = df_touches.join(
    df_customers.select(
        "customer_id",
        "credit_tier",
        "region",
        "acquisition_channel"
    ),
    on="customer_id",
    how="left"
)

print(f"Touches after customer join: {df_touches_enriched.count():,} rows")
print("\nSample enriched touches:")
df_touches_enriched.select(
    "touch_id", "customer_id", "channel", "campaign",
    "credit_tier", "region", "acquisition_channel"
).show(5)

# COMMAND ----------

from pyspark.sql.functions import col, datediff

df_touchpoints = df_touches_enriched.join(
    df_sessions.select(
        "true_session_id",
        "customer_id",
        "session_start",
        "session_end",
        col("converted").alias("session_converted"),
        "entry_page",
        "exit_page"
    ),
    on="customer_id",
    how="left"
) \
.filter(
    (col("touchpoint_date") <= col("session_start")) &
    (datediff(col("session_start"), col("touchpoint_date")) <= 30)
) \
.withColumn("days_before_session",
    datediff(col("session_start"), col("touchpoint_date")))

print(f"Touchpoints created: {df_touchpoints.count():,} rows")
print("\nSample touchpoints:")
df_touchpoints.select(
    "touch_id", "customer_id", "channel", "campaign",
    "touchpoint_date", "true_session_id", "session_start",
    "session_converted", "days_before_session"
).show(5)

# COMMAND ----------

from pyspark.sql.functions import lit

df_touchpoints_final = df_touchpoints.withColumn("silver_load_date", lit(LOAD_DATE))

df_touchpoints_final.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("silver_load_date") \
    .option("overwriteSchema", "true") \
    .save(f"{SILVER_PATH}/touchpoints")

row_count = spark.read.format("delta").load(f"{SILVER_PATH}/touchpoints").count()

print(f"Silver touchpoints table written successfully.")
print(f"Total rows: {row_count:,}")
print(f"Columns: {spark.read.format('delta').load(f'{SILVER_PATH}/touchpoints').columns}")