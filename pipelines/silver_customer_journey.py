# Databricks notebook source
from datetime import date

SILVER_PATH = "/Volumes/workspace/default/fintech_raw_data/silver"
LOAD_DATE   = str(date.today())

print(f"Silver path: {SILVER_PATH}")
print(f"Load date:   {LOAD_DATE}")

# COMMAND ----------

df_customers     = spark.read.table("workspace.default.silver_customers")
df_transactions  = spark.read.table("workspace.default.silver_transactions")
df_touches       = spark.read.table("workspace.default.silver_marketing_touches")

print(f"Customers loaded:    {df_customers.count():,} rows")
print(f"Transactions loaded: {df_transactions.count():,} rows")
print(f"Touches loaded:      {df_touches.count():,} rows")

# COMMAND ----------

from pyspark.sql.functions import col, count, sum as spark_sum, avg, round, first

df_txn_summary = df_transactions \
    .filter(col("status") == "Completed") \
    .filter(col("is_amount_outlier") == False) \
    .groupBy("customer_id") \
    .agg(
        count("transaction_id").alias("total_transactions"),
        round(spark_sum("amount"), 2).alias("total_spend"),
        round(avg("amount"), 2).alias("avg_transaction_amount"),
        first("product_type").alias("most_used_product")
    )

print(f"Transaction summary created: {df_txn_summary.count():,} customers with transactions")
print("\nSample transaction summary:")
df_txn_summary.show(5)

# COMMAND ----------

from pyspark.sql.functions import col, count, round, avg, sum as spark_sum

df_touch_summary = df_touches \
    .filter(col("is_cost_outlier") == False) \
    .groupBy("customer_id") \
    .agg(
        count("touch_id").alias("total_touches"),
        round(spark_sum("cost"), 2).alias("total_marketing_cost"),
        round(avg("cost"), 2).alias("avg_touch_cost"),
        round(avg(col("converted").cast("double")), 4).alias("touch_conversion_rate")
    )

print(f"Touch summary created: {df_touch_summary.count():,} customers with touches")
print("\nSample touch summary:")
df_touch_summary.show(5)

# COMMAND ----------

from pyspark.sql.functions import col

df_journey = df_customers.select(
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "gender",
    "region",
    "acquisition_channel",
    "signup_date",
    "credit_tier",
    "credit_score",
    "annual_income",
    "is_active"
) \
.join(df_txn_summary, on="customer_id", how="left") \
.join(df_touch_summary, on="customer_id", how="left")

print(f"Customer journey table created: {df_journey.count():,} rows")
print("\nSample customer journey:")
df_journey.select(
    "customer_id", "credit_tier", "total_transactions",
    "total_spend", "total_touches", "touch_conversion_rate"
).show(5)

# COMMAND ----------

from pyspark.sql.functions import lit

df_journey_final = df_journey.withColumn("silver_load_date", lit(LOAD_DATE))

df_journey_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.silver_customer_journey")

row_count = spark.read.table("workspace.default.silver_customer_journey").count()

print(f"Silver customer_journey table written successfully.")
print(f"Total rows: {row_count:,}")
print(f"Columns: {spark.read.table('workspace.default.silver_customer_journey').columns}")