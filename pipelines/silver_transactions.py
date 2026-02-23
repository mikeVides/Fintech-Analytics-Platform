# Databricks notebook source
from datetime import date

BRONZE_PATH = "/Volumes/workspace/default/fintech_raw_data/bronze"
SILVER_PATH = "/Volumes/workspace/default/fintech_raw_data/silver"
LOAD_DATE   = str(date.today())

print(f"Bronze path: {BRONZE_PATH}")
print(f"Silver path: {SILVER_PATH}")
print(f"Load date:   {LOAD_DATE}")

# COMMAND ----------

df_bronze = spark.read.format("delta").load(f"{BRONZE_PATH}/transactions")

print(f"Bronze transactions row count: {df_bronze.count():,}")
print(f"Columns: {df_bronze.columns}")
df_bronze.show(5)

# COMMAND ----------

from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

window = Window.partitionBy("transaction_id").orderBy(col("load_date").desc())

df_deduped = df_bronze.withColumn("row_num", row_number().over(window)) \
                      .filter(col("row_num") == 1) \
                      .drop("row_num")

print(f"Before dedup: {df_bronze.count():,} rows")
print(f"After dedup:  {df_deduped.count():,} rows")
print(f"Duplicates removed: {df_bronze.count() - df_deduped.count():,}")

# COMMAND ----------

from pyspark.sql.functions import initcap, lower

df_cased = df_deduped \
    .withColumn("product_type",      initcap(lower(col("product_type")))) \
    .withColumn("status",            initcap(lower(col("status")))) \
    .withColumn("merchant_category", initcap(lower(col("merchant_category"))))

print("Casing standardized.")
print("\nproduct_type distinct values after fix:")
df_cased.groupBy("product_type").count().orderBy("count", ascending=False).show()
print("\nstatus distinct values after fix:")
df_cased.groupBy("status").count().orderBy("count", ascending=False).show()

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, coalesce, expr

df_dated = df_cased \
    .withColumn("timestamp",
        coalesce(
            expr("try_to_timestamp(timestamp, 'yyyy-MM-dd HH:mm:ss')"),
            expr("try_to_timestamp(timestamp, 'MM/dd/yyyy HH:mm:ss')"),
            expr("try_to_timestamp(timestamp, 'MMMM dd yyyy HH:mm:ss')")
        )
    )

print("Timestamp standardized.")
print("\nSample timestamp values after fix:")
df_dated.select("timestamp").show(10)

# COMMAND ----------

from pyspark.sql.functions import when, col

df_nulls = df_dated \
    .withColumn("status",
        when(col("status").isNull(), "Unknown")
        .otherwise(col("status"))) \
    .withColumn("merchant_category",
        when(col("merchant_category").isNull(), "Unknown")
        .otherwise(col("merchant_category")))

print("Nulls handled.")
print("\nNull counts after fix:")
for column in ["status", "merchant_category"]:
    null_count = df_nulls.filter(col(column).isNull()).count()
    print(f"  {column}: {null_count} nulls remaining")

# COMMAND ----------

from pyspark.sql.functions import mean, stddev, round

amount_stats = df_nulls.select(
    mean(col("amount")).alias("mean"),
    stddev(col("amount")).alias("stddev")
).collect()[0]

amount_mean  = amount_stats["mean"]
amount_std   = amount_stats["stddev"]
upper_bound  = amount_mean + (3 * amount_std)

df_flagged = df_nulls.withColumn("is_amount_outlier",
    when(col("amount") > upper_bound, True)
    .otherwise(False))

outlier_count = df_flagged.filter(col("is_amount_outlier") == True).count()

print(f"Amount mean:      ${amount_mean:,.2f}")
print(f"Amount std dev:   ${amount_std:,.2f}")
print(f"Upper bound:      ${upper_bound:,.2f}")
print(f"Outliers flagged: {outlier_count:,}")

# COMMAND ----------

from pyspark.sql.functions import lit

df_flagged = df_flagged.withColumn("is_duplicate", lit(False))

print("is_duplicate flag added.")
print(f"All {df_flagged.count():,} rows marked as is_duplicate = False")
print("Note: duplicate rows were removed in Cell 3 — surviving rows are all non-duplicates.")

# COMMAND ----------

from pyspark.sql.functions import when, col

df_scored = df_flagged \
    .withColumn("data_quality_score",
        (1.0
        - when(col("is_amount_outlier") == True, 0.2).otherwise(0.0)
        ).cast("double"))

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
    .save(f"{SILVER_PATH}/transactions")

row_count = spark.read.format("delta").load(f"{SILVER_PATH}/transactions").count()

print(f"Silver transactions table written successfully.")
print(f"Total rows: {row_count:,}")
print(f"Columns: {spark.read.format('delta').load(f'{SILVER_PATH}/transactions').columns}")