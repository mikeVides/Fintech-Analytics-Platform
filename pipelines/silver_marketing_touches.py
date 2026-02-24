# Databricks notebook source
from datetime import date

BRONZE_PATH = "/Volumes/workspace/default/fintech_raw_data/bronze"
SILVER_PATH = "/Volumes/workspace/default/fintech_raw_data/silver"
LOAD_DATE   = str(date.today())

print(f"Bronze path: {BRONZE_PATH}")
print(f"Silver path: {SILVER_PATH}")
print(f"Load date:   {LOAD_DATE}")

# COMMAND ----------

df_bronze = spark.read.format("delta").load(f"{BRONZE_PATH}/marketing_touches")

print(f"Bronze marketing_touches row count: {df_bronze.count():,}")
print(f"Columns: {df_bronze.columns}")
df_bronze.show(5)

# COMMAND ----------

from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

window = Window.partitionBy("touch_id").orderBy(col("load_date").desc())

df_deduped = df_bronze.withColumn("row_num", row_number().over(window)) \
                      .filter(col("row_num") == 1) \
                      .drop("row_num")

print(f"Before dedup: {df_bronze.count():,} rows")
print(f"After dedup:  {df_deduped.count():,} rows")
print(f"Duplicates removed: {df_bronze.count() - df_deduped.count():,}")

# COMMAND ----------

from pyspark.sql.functions import initcap, lower, col

df_cased = df_deduped \
    .withColumn("channel", initcap(lower(col("channel"))))

print("Casing standardized.")
print("\nchannel distinct values after fix:")
df_cased.groupBy("channel").count().orderBy("count", ascending=False).show()

# COMMAND ----------

from pyspark.sql.functions import coalesce, expr

df_dated = df_cased \
    .withColumn("touchpoint_date",
        coalesce(
            expr("try_to_date(touchpoint_date, 'yyyy-MM-dd')"),
            expr("try_to_date(touchpoint_date, 'MM/dd/yyyy')"),
            expr("try_to_date(touchpoint_date, 'MMMM dd yyyy')")
        )
    )

print("Date format standardized.")
print("\nSample touchpoint_date values after fix:")
df_dated.select("touchpoint_date").show(10)

# COMMAND ----------

from pyspark.sql.functions import when, col

df_nulls = df_dated \
    .withColumn("cost",
        when(col("cost").isNull(), 0.0)
        .otherwise(col("cost"))) \
    .withColumn("utm_source",
        when(col("utm_source").isNull(), "unknown")
        .otherwise(col("utm_source"))) \
    .withColumn("utm_campaign",
        when(col("utm_campaign").isNull(), "unknown")
        .otherwise(col("utm_campaign")))

print("Nulls handled.")
print("\nNull counts after fix:")
for column in ["cost", "utm_source", "utm_campaign"]:
    null_count = df_nulls.filter(col(column).isNull()).count()
    print(f"  {column}: {null_count} nulls remaining")

# COMMAND ----------

from pyspark.sql.functions import mean, stddev

cost_stats = df_nulls.select(
    mean(col("cost")).alias("mean"),
    stddev(col("cost")).alias("stddev")
).collect()[0]

cost_mean  = cost_stats["mean"]
cost_std   = cost_stats["stddev"]
upper_bound = cost_mean + (3 * cost_std)

df_flagged = df_nulls.withColumn("is_cost_outlier",
    when(col("cost") > upper_bound, True)
    .otherwise(False))

outlier_count = df_flagged.filter(col("is_cost_outlier") == True).count()

print(f"Cost mean:        ${cost_mean:,.2f}")
print(f"Cost std dev:     ${cost_std:,.2f}")
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
        - when(col("is_cost_outlier") == True, 0.2).otherwise(0.0)
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
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.silver_marketing_touches")

row_count = spark.read.table("workspace.default.silver_marketing_touches").count()

print(f"Silver marketing_touches table written successfully.")
print(f"Total rows: {row_count:,}")
print(f"Columns: {spark.read.table('workspace.default.silver_marketing_touches').columns}")