# Databricks notebook source
from datetime import date

BRONZE_PATH = "/Volumes/workspace/default/fintech_raw_data/bronze"
SILVER_PATH = "/Volumes/workspace/default/fintech_raw_data/silver"
LOAD_DATE   = str(date.today())

print(f"Bronze path: {BRONZE_PATH}")
print(f"Silver path: {SILVER_PATH}")
print(f"Load date:   {LOAD_DATE}")

# COMMAND ----------

df_bronze = spark.read.format("delta").load(f"{BRONZE_PATH}/loan_applications")

print(f"Bronze loan_applications row count: {df_bronze.count():,}")
print(f"Columns: {df_bronze.columns}")
df_bronze.show(5)

# COMMAND ----------

from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

window = Window.partitionBy("application_id").orderBy(col("load_date").desc())

df_deduped = df_bronze.withColumn("row_num", row_number().over(window)) \
                      .filter(col("row_num") == 1) \
                      .drop("row_num")

print(f"Before dedup: {df_bronze.count():,} rows")
print(f"After dedup:  {df_deduped.count():,} rows")
print(f"Duplicates removed: {df_bronze.count() - df_deduped.count():,}")

# COMMAND ----------

from pyspark.sql.functions import initcap, lower, col

df_cased = df_deduped \
    .withColumn("approval_status", initcap(lower(col("approval_status")))) \
    .withColumn("purpose",         initcap(lower(col("purpose"))))

print("Casing standardized.")
print("\napproval_status distinct values after fix:")
df_cased.groupBy("approval_status").count().orderBy("count", ascending=False).show()
print("\npurpose distinct values after fix:")
df_cased.groupBy("purpose").count().orderBy("count", ascending=False).show()

# COMMAND ----------

from pyspark.sql.functions import coalesce, expr

df_dated = df_cased \
    .withColumn("application_date",
        coalesce(
            expr("try_to_date(application_date, 'yyyy-MM-dd')"),
            expr("try_to_date(application_date, 'MM/dd/yyyy')"),
            expr("try_to_date(application_date, 'MMMM dd yyyy')")
        )
    )

print("Date format standardized.")
print("\nSample application_date values after fix:")
df_dated.select("application_date").show(10)

# COMMAND ----------

from pyspark.sql.functions import when, col

df_nulls = df_dated \
    .withColumn("purpose",
        when(col("purpose").isNull(), "Unknown")
        .otherwise(col("purpose")))

print("Nulls handled.")
print("\nNull counts after fix:")
for column in ["purpose", "interest_rate"]:
    null_count = df_nulls.filter(col(column).isNull()).count()
    print(f"  {column}: {null_count} nulls remaining")

print("\nNote: interest_rate nulls are expected business logic —")
print("NULL means the loan was Declined or Under Review, not a data error.")

# COMMAND ----------

from pyspark.sql.functions import mean, stddev

requested_stats = df_nulls.select(
    mean(col("requested_amount")).alias("mean"),
    stddev(col("requested_amount")).alias("stddev")
).collect()[0]

loan_stats = df_nulls.select(
    mean(col("loan_amount")).alias("mean"),
    stddev(col("loan_amount")).alias("stddev")
).collect()[0]

requested_mean  = requested_stats["mean"]
requested_std   = requested_stats["stddev"]
requested_bound = requested_mean + (3 * requested_std)

loan_mean  = loan_stats["mean"]
loan_std   = loan_stats["stddev"]
loan_bound = loan_mean + (3 * loan_std)

df_flagged = df_nulls \
    .withColumn("is_requested_amount_outlier",
        when(col("requested_amount") > requested_bound, True)
        .otherwise(False)) \
    .withColumn("is_loan_amount_outlier",
        when(col("loan_amount") > loan_bound, True)
        .otherwise(False))

requested_outliers = df_flagged.filter(col("is_requested_amount_outlier") == True).count()
loan_outliers      = df_flagged.filter(col("is_loan_amount_outlier") == True).count()

print(f"Requested amount mean:      ${requested_mean:,.2f}")
print(f"Requested amount std dev:   ${requested_std:,.2f}")
print(f"Requested amount bound:     ${requested_bound:,.2f}")
print(f"Requested outliers flagged: {requested_outliers:,}")
print()
print(f"Loan amount mean:           ${loan_mean:,.2f}")
print(f"Loan amount std dev:        ${loan_std:,.2f}")
print(f"Loan amount bound:          ${loan_bound:,.2f}")
print(f"Loan outliers flagged:      {loan_outliers:,}")

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
        - when(col("is_requested_amount_outlier") == True, 0.1).otherwise(0.0)
        - when(col("is_loan_amount_outlier") == True, 0.1).otherwise(0.0)
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
    .save(f"{SILVER_PATH}/loan_applications")

row_count = spark.read.format("delta").load(f"{SILVER_PATH}/loan_applications").count()

print(f"Silver loan_applications table written successfully.")
print(f"Total rows: {row_count:,}")
print(f"Columns: {spark.read.format('delta').load(f'{SILVER_PATH}/loan_applications').columns}")