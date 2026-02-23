# Databricks notebook source
from datetime import date

BRONZE_PATH = "/Volumes/workspace/default/fintech_raw_data/bronze"
SILVER_PATH = "/Volumes/workspace/default/fintech_raw_data/silver"
LOAD_DATE   = str(date.today())

print(f"Bronze path: {BRONZE_PATH}")
print(f"Silver path: {SILVER_PATH}")
print(f"Load date:   {LOAD_DATE}")

# COMMAND ----------

df_bronze = spark.read.format("delta").load(f"{BRONZE_PATH}/customers")

print(f"Bronze customers row count: {df_bronze.count():,}")
print(f"Columns: {df_bronze.columns}")
df_bronze.show(5)

# COMMAND ----------

from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

window = Window.partitionBy("customer_id").orderBy(col("load_date").desc())

df_deduped = df_bronze.withColumn("row_num", row_number().over(window)) \
                      .filter(col("row_num") == 1) \
                      .drop("row_num")

print(f"Before dedup: {df_bronze.count():,} rows")
print(f"After dedup:  {df_deduped.count():,} rows")
print(f"Duplicates removed: {df_bronze.count() - df_deduped.count():,}")

# COMMAND ----------

from pyspark.sql.functions import initcap, lower, upper, when, trim

df_cased = df_deduped \
    .withColumn("gender",              initcap(lower(col("gender")))) \
    .withColumn("region",              initcap(lower(col("region")))) \
    .withColumn("acquisition_channel", initcap(lower(col("acquisition_channel")))) \
    .withColumn("credit_tier",         initcap(lower(col("credit_tier"))))

print("Casing standardized.")
print("\ngender distinct values after fix:")
df_cased.groupBy("gender").count().orderBy("count", ascending=False).show()
print("\nregion distinct values after fix:")
df_cased.groupBy("region").count().orderBy("count", ascending=False).show()

# COMMAND ----------

from pyspark.sql.functions import coalesce, expr

df_dated = df_cased \
    .withColumn("signup_date",
        coalesce(
            expr("try_to_date(signup_date, 'yyyy-MM-dd')"),
            expr("try_to_date(signup_date, 'MM/dd/yyyy')"),
            expr("try_to_date(signup_date, 'MMMM dd yyyy')")
        )
    ) \
    .withColumn("date_of_birth",
        coalesce(
            expr("try_to_date(date_of_birth, 'yyyy-MM-dd')"),
            expr("try_to_date(date_of_birth, 'MM/dd/yyyy')"),
            expr("try_to_date(date_of_birth, 'MMMM dd yyyy')")
        )
    )

print("Date formats standardized.")
print("\nSample signup_date values after fix:")
df_dated.select("signup_date").show(10)

# COMMAND ----------

from pyspark.sql.functions import mean

avg_income = df_dated.select(mean(col("annual_income"))).collect()[0][0]

df_nulls = df_dated \
    .withColumn("email",
        when(col("email").isNull(), "unknown@unknown.com")
        .otherwise(col("email"))) \
    .withColumn("gender",
        when(col("gender").isNull(), "Unknown")
        .otherwise(col("gender"))) \
    .withColumn("annual_income",
        when(col("annual_income").isNull(), round(avg_income, 2))
        .otherwise(col("annual_income")))

print(f"Average income used for null fill: ${avg_income:,.2f}")
print("\nNull counts after fix:")
for column in ["email", "gender", "annual_income"]:
    null_count = df_nulls.filter(col(column).isNull()).count()
    print(f"  {column}: {null_count} nulls remaining")

# COMMAND ----------

from pyspark.sql.functions import stddev

income_stats = df_nulls.select(
    mean(col("annual_income")).alias("mean"),
    stddev(col("annual_income")).alias("stddev")
).collect()[0]

income_mean = income_stats["mean"]
income_std  = income_stats["stddev"]
upper_bound = income_mean + (3 * income_std)

df_flagged = df_nulls.withColumn("is_income_outlier",
    when(col("annual_income") > upper_bound, True)
    .otherwise(False))

outlier_count = df_flagged.filter(col("is_income_outlier") == True).count()

print(f"Income mean:    ${income_mean:,.2f}")
print(f"Income std dev: ${income_std:,.2f}")
print(f"Upper bound:    ${upper_bound:,.2f}")
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
        - when(col("is_income_outlier") == True, 0.2).otherwise(0.0)
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
    .save(f"{SILVER_PATH}/customers")

row_count = spark.read.format("delta").load(f"{SILVER_PATH}/customers").count()

print(f"Silver customers table written successfully.")
print(f"Total rows: {row_count:,}")
print(f"Columns: {spark.read.format('delta').load(f'{SILVER_PATH}/customers').columns}")