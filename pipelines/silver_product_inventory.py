# Databricks notebook source
from datetime import date

BRONZE_PATH = "/Volumes/workspace/default/fintech_raw_data/bronze"
SILVER_PATH = "/Volumes/workspace/default/fintech_raw_data/silver"
LOAD_DATE   = str(date.today())

print(f"Bronze path: {BRONZE_PATH}")
print(f"Silver path: {SILVER_PATH}")
print(f"Load date:   {LOAD_DATE}")

# COMMAND ----------

df_bronze = spark.read.format("delta").load(f"{BRONZE_PATH}/product_inventory")

print(f"Bronze product_inventory row count: {df_bronze.count():,}")
print(f"Columns: {df_bronze.columns}")
df_bronze.show(5)

# COMMAND ----------

from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

window = Window.partitionBy("product_id").orderBy(col("load_date").desc())

df_deduped = df_bronze.withColumn("row_num", row_number().over(window)) \
                      .filter(col("row_num") == 1) \
                      .drop("row_num")

print(f"Before dedup: {df_bronze.count():,} rows")
print(f"After dedup:  {df_deduped.count():,} rows")
print(f"Duplicates removed: {df_bronze.count() - df_deduped.count():,}")

# COMMAND ----------

from pyspark.sql.functions import when, col, mean

avg_selling_price = df_deduped.select(mean(col("selling_price"))).collect()[0][0]

df_nulls = df_deduped \
    .withColumn("selling_price",
        when(col("selling_price").isNull(), round(avg_selling_price, 2))
        .otherwise(col("selling_price"))) \
    .withColumn("supplier",
        when(col("supplier").isNull(), "Unknown")
        .otherwise(col("supplier")))

print(f"Average selling price used for null fill: ${avg_selling_price:,.2f}")
print("\nNull counts after fix:")
for column in ["selling_price", "supplier"]:
    null_count = df_nulls.filter(col(column).isNull()).count()
    print(f"  {column}: {null_count} nulls remaining")

# COMMAND ----------

from pyspark.sql.functions import mean, stddev, when, col

unit_cost_stats = df_nulls.select(
    mean(col("unit_cost")).alias("mean"),
    stddev(col("unit_cost")).alias("stddev")
).collect()[0]

selling_price_stats = df_nulls.select(
    mean(col("selling_price")).alias("mean"),
    stddev(col("selling_price")).alias("stddev")
).collect()[0]

unit_cost_mean   = unit_cost_stats["mean"]
unit_cost_std    = unit_cost_stats["stddev"]
unit_cost_bound  = unit_cost_mean + (3 * unit_cost_std)

selling_price_mean  = selling_price_stats["mean"]
selling_price_std   = selling_price_stats["stddev"]
selling_price_bound = selling_price_mean + (3 * selling_price_std)

df_flagged = df_nulls \
    .withColumn("is_unit_cost_outlier",
        when(col("unit_cost") > unit_cost_bound, True)
        .otherwise(False)) \
    .withColumn("is_selling_price_outlier",
        when(col("selling_price") > selling_price_bound, True)
        .otherwise(False))

unit_cost_outliers     = df_flagged.filter(col("is_unit_cost_outlier") == True).count()
selling_price_outliers = df_flagged.filter(col("is_selling_price_outlier") == True).count()

print(f"Unit cost mean:           ${unit_cost_mean:,.2f}")
print(f"Unit cost std dev:        ${unit_cost_std:,.2f}")
print(f"Unit cost bound:          ${unit_cost_bound:,.2f}")
print(f"Unit cost outliers:       {unit_cost_outliers:,}")
print()
print(f"Selling price mean:       ${selling_price_mean:,.2f}")
print(f"Selling price std dev:    ${selling_price_std:,.2f}")
print(f"Selling price bound:      ${selling_price_bound:,.2f}")
print(f"Selling price outliers:   {selling_price_outliers:,}")

# COMMAND ----------

from pyspark.sql.functions import lit

df_silver = df_flagged.withColumn("silver_load_date", lit(LOAD_DATE))

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("silver_load_date") \
    .option("overwriteSchema", "true") \
    .save(f"{SILVER_PATH}/product_inventory")

row_count = spark.read.format("delta").load(f"{SILVER_PATH}/product_inventory").count()

print(f"Silver product_inventory table written successfully.")
print(f"Total rows: {row_count:,}")
print(f"Columns: {spark.read.format('delta').load(f'{SILVER_PATH}/product_inventory').columns}")