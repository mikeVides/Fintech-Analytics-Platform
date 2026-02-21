# Databricks notebook source
# ── Bronze Ingest Configuration ──

# Where the raw CSV files live in the volume we created
VOLUME_PATH = "/Volumes/workspace/default/fintech_raw_data"

# Where Bronze Delta tables will be written
BRONZE_PATH = "/Volumes/workspace/default/fintech_raw_data/bronze"

# Today's date used for partitioning — records when this data was loaded
from datetime import date
LOAD_DATE = str(date.today())

print(f"Volume path: {VOLUME_PATH}")
print(f"Bronze path: {BRONZE_PATH}")
print(f"Load date:   {LOAD_DATE}")

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType,
    BooleanType, DateType, TimestampType
)

schemas = {
    "customers": StructType([
        StructField("customer_id",         StringType(),    True),
        StructField("first_name",          StringType(),    True),
        StructField("last_name",           StringType(),    True),
        StructField("email",               StringType(),    True),
        StructField("date_of_birth",       StringType(),    True),
        StructField("gender",              StringType(),    True),
        StructField("region",              StringType(),    True),
        StructField("acquisition_channel", StringType(),    True),
        StructField("signup_date",         StringType(),    True),
        StructField("credit_tier",         StringType(),    True),
        StructField("credit_score",        IntegerType(),   True),
        StructField("annual_income",       FloatType(),     True),
        StructField("is_active",           BooleanType(),   True),
    ]),
    "transactions": StructType([
        StructField("transaction_id",    StringType(),    True),
        StructField("customer_id",       StringType(),    True),
        StructField("amount",            FloatType(),     True),
        StructField("product_type",      StringType(),    True),
        StructField("status",            StringType(),    True),
        StructField("timestamp",         StringType(),    True),
        StructField("merchant_category", StringType(),    True),
    ]),
    "loan_applications": StructType([
        StructField("application_id",   StringType(),    True),
        StructField("customer_id",      StringType(),    True),
        StructField("application_date", StringType(),    True),
        StructField("requested_amount", FloatType(),     True),
        StructField("approval_status",  StringType(),    True),
        StructField("loan_amount",      FloatType(),     True),
        StructField("interest_rate",    FloatType(),     True),
        StructField("loan_term_months", IntegerType(),   True),
        StructField("default_flag",     BooleanType(),   True),
        StructField("purpose",          StringType(),    True),
    ]),
    "marketing_touches": StructType([
        StructField("touch_id",        StringType(),    True),
        StructField("customer_id",     StringType(),    True),
        StructField("channel",         StringType(),    True),
        StructField("campaign",        StringType(),    True),
        StructField("touchpoint_date", StringType(),    True),
        StructField("cost",            FloatType(),     True),
        StructField("converted",       BooleanType(),   True),
        StructField("utm_source",      StringType(),    True),
        StructField("utm_campaign",    StringType(),    True),
    ]),
    "product_inventory": StructType([
        StructField("product_id",       StringType(),    True),
        StructField("sku",              StringType(),    True),
        StructField("product_name",     StringType(),    True),
        StructField("category",         StringType(),    True),
        StructField("unit_cost",        FloatType(),     True),
        StructField("selling_price",    FloatType(),     True),
        StructField("stock_level",      IntegerType(),   True),
        StructField("reorder_point",    IntegerType(),   True),
        StructField("reorder_quantity", IntegerType(),   True),
        StructField("lead_time_days",   IntegerType(),   True),
        StructField("supplier",         StringType(),    True),
        StructField("is_active",        BooleanType(),   True),
    ]),
    "events_raw": StructType([
        StructField("event_id",      StringType(),    True),
        StructField("session_id",    StringType(),    True),
        StructField("customer_id",   StringType(),    True),
        StructField("event_type",    StringType(),    True),
        StructField("page",          StringType(),    True),
        StructField("timestamp",     StringType(),    True),
        StructField("device",        StringType(),    True),
        StructField("utm_source",    StringType(),    True),
        StructField("utm_campaign",  StringType(),    True),
        StructField("is_bot",        BooleanType(),   True),
    ]),
}

print(f"Schemas defined for {len(schemas)} tables")

# COMMAND ----------

from pyspark.sql.functions import lit

def ingest_bronze(table_name, schema):
    print(f"Ingesting {table_name}...")

    # ── Step 1: Read the raw CSV from the volume ──
    df = (spark.read
          .format("csv")
          .option("header", "true")
          .option("mode", "PERMISSIVE")
          .schema(schema)
          .load(f"{VOLUME_PATH}/{table_name}.csv"))

    # ── Step 2: Add load metadata columns ──
    df = df.withColumn("load_date", lit(LOAD_DATE))
    df = df.withColumn("source_file", lit(f"{table_name}.csv"))

    # ── Step 3: Write to Delta table ──
    (df.write
       .format("delta")
       .mode("overwrite")
       .partitionBy("load_date")
       .option("overwriteSchema", "true")
       .save(f"{BRONZE_PATH}/{table_name}"))

    # ── Step 4: Confirm row count ──
    row_count = spark.read.format("delta").load(f"{BRONZE_PATH}/{table_name}").count()
    print(f"  ✓ {table_name} — {row_count:,} rows loaded to Bronze")

print("Bronze ingest function defined")

# COMMAND ----------

tables = [
    "customers",
    "transactions",
    "loan_applications",
    "marketing_touches",
    "product_inventory",
    "events_raw",
]

print("Starting Bronze ingestion...\n")

for table_name in tables:
    ingest_bronze(table_name, schemas[table_name])

print("\nBronge ingestion complete!")