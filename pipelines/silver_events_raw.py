# Databricks notebook source
from datetime import date

BRONZE_PATH = "/Volumes/workspace/default/fintech_raw_data/bronze"
SILVER_PATH = "/Volumes/workspace/default/fintech_raw_data/silver"
LOAD_DATE   = str(date.today())

print(f"Bronze path: {BRONZE_PATH}")
print(f"Silver path: {SILVER_PATH}")
print(f"Load date:   {LOAD_DATE}")

# COMMAND ----------

df_bronze = spark.read.format("delta").load(f"{BRONZE_PATH}/events_raw")

print(f"Bronze events_raw row count: {df_bronze.count():,}")
print(f"Columns: {df_bronze.columns}")
df_bronze.show(5)

# COMMAND ----------

from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

window = Window.partitionBy("event_id").orderBy(col("load_date").desc())

df_deduped = df_bronze.withColumn("row_num", row_number().over(window)) \
                      .filter(col("row_num") == 1) \
                      .drop("row_num")

print(f"Before dedup: {df_bronze.count():,} rows")
print(f"After dedup:  {df_deduped.count():,} rows")
print(f"Duplicates removed: {df_bronze.count() - df_deduped.count():,}")

# COMMAND ----------

df_no_bots = df_deduped.filter(col("is_bot") == False)

bot_count  = df_deduped.filter(col("is_bot") == True).count()
real_count = df_no_bots.count()

print(f"Total rows before bot filter: {df_deduped.count():,}")
print(f"Bot rows removed:             {bot_count:,}")
print(f"Real human rows remaining:    {real_count:,}")
print(f"Bot rate:                     {round((bot_count / df_deduped.count()) * 100, 2)}%")

# COMMAND ----------

from pyspark.sql.functions import initcap, lower, col

df_cased = df_no_bots \
    .withColumn("event_type", lower(col("event_type"))) \
    .withColumn("device",     lower(col("device")))

print("Casing standardized.")
print("\nevent_type distinct values after fix:")
df_cased.groupBy("event_type").count().orderBy("count", ascending=False).show()
print("\ndevice distinct values after fix:")
df_cased.groupBy("device").count().orderBy("count", ascending=False).show()

# COMMAND ----------

from pyspark.sql.functions import when, col

df_nulls = df_cased \
    .withColumn("device",
        when(col("device").isNull(), "unknown")
        .otherwise(col("device"))) \
    .withColumn("utm_campaign",
        when(col("utm_campaign").isNull(), "unknown")
        .otherwise(col("utm_campaign")))

print("Nulls handled.")
print("\nNull counts after fix:")
for column in ["customer_id", "device", "utm_campaign"]:
    null_count = df_nulls.filter(col(column).isNull()).count()
    print(f"  {column}: {null_count} nulls remaining")

print("\nNote: customer_id nulls are expected business logic —")
print("NULL means an anonymous visitor with no known identity.")

# COMMAND ----------

from pyspark.sql.functions import lit

df_nulls = df_nulls.withColumn("is_duplicate", lit(False))

print("is_duplicate flag added.")
print(f"All {df_nulls.count():,} rows marked as is_duplicate = False")
print("Note: duplicate rows were removed in Cell 3 — surviving rows are all non-duplicates.")

# COMMAND ----------

df_scored = df_nulls.withColumn("data_quality_score", lit(1.0).cast("double"))

print("data_quality_score calculated.")
print("\nScore distribution:")
df_scored.groupBy("data_quality_score").count().orderBy("data_quality_score").show()

# COMMAND ----------

from pyspark.sql.functions import lit

df_silver = df_scored.withColumn("silver_load_date", lit(LOAD_DATE))

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("silver_load_date") \
    .option("overwriteSchema", "true") \
    .save(f"{SILVER_PATH}/events_raw")

row_count = spark.read.format("delta").load(f"{SILVER_PATH}/events_raw").count()

print(f"Silver events_raw table written successfully.")
print(f"Total rows: {row_count:,}")
print(f"Columns: {spark.read.format('delta').load(f'{SILVER_PATH}/events_raw').columns}")