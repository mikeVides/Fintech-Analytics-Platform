# Databricks notebook source
BRONZE_PATH = "/Volumes/workspace/default/fintech_raw_data/bronze"

from datetime import date
LOAD_DATE = str(date.today())

tables = [
    "customers",
    "transactions",
    "loan_applications",
    "marketing_touches",
    "product_inventory",
    "events_raw",
]

print(f"Bronze path: {BRONZE_PATH}")
print(f"Load date:   {LOAD_DATE}")
print(f"Tables to profile: {len(tables)}")

# COMMAND ----------

from pyspark.sql.functions import col, isnan, when, count, round as spark_round

print("=" * 60)
print("NULL VALUE ANALYSIS")
print("=" * 60)

for table_name in tables:
    df = spark.read.format("delta").load(f"{BRONZE_PATH}/{table_name}")
    total_rows = df.count()
    
    print(f"\n{table_name.upper()} ({total_rows:,} rows)")
    print("-" * 40)
    
    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        null_rate = round((null_count / total_rows) * 100, 2)
        if null_count > 0:
            print(f"  {column:<25} {null_count:>6} nulls ({null_rate}%)")
    
print("\nNull analysis complete!")


# COMMAND ----------

print("=" * 60)
print("DUPLICATE ANALYSIS")
print("=" * 60)

for table_name in tables:
    df = spark.read.format("delta").load(f"{BRONZE_PATH}/{table_name}")
    total_rows = df.count()
    distinct_rows = df.distinct().count()
    duplicate_count = total_rows - distinct_rows
    duplicate_rate = round((duplicate_count / total_rows) * 100, 2)
    
    print(f"\n{table_name.upper()}")
    print(f"  Total rows:      {total_rows:>7,}")
    print(f"  Distinct rows:   {distinct_rows:>7,}")
    print(f"  Duplicates:      {duplicate_count:>7,} ({duplicate_rate}%)")

print("\nDuplicate analysis complete!")

# COMMAND ----------

from pyspark.sql.functions import min, max, avg, stddev, col
from pyspark.sql.types import NumericType

print("=" * 60)
print("NUMERIC COLUMN STATISTICS & OUTLIER DETECTION")
print("=" * 60)

for table_name in tables:
    df = spark.read.format("delta").load(f"{BRONZE_PATH}/{table_name}")
    
    numeric_cols = [f.name for f in df.schema.fields 
                    if isinstance(f.dataType, NumericType)]
    
    if not numeric_cols:
        continue
        
    print(f"\n{table_name.upper()}")
    print("-" * 40)
    
    for column in numeric_cols:
        stats = df.select(
            min(col(column)).alias("min"),
            max(col(column)).alias("max"),
            avg(col(column)).alias("avg"),
            stddev(col(column)).alias("stddev")
        ).collect()[0]
        
        if stats["avg"] is None:
            continue
            
        mean = stats["avg"]
        std  = stats["stddev"]
        
        outlier_count = df.filter(
            col(column) > (mean + 3 * std)
        ).count()
        
        print(f"  {column}")
        print(f"    Min:      {stats['min']:>12,.2f}")
        print(f"    Max:      {stats['max']:>12,.2f}")
        print(f"    Average:  {mean:>12,.2f}")
        print(f"    Outliers: {outlier_count:>12,} rows beyond 3 std devs")

print("\nNumeric statistics complete!")

# COMMAND ----------

print("=" * 60)
print("VALUE DISTRIBUTION ANALYSIS")
print("=" * 60)

categorical_cols = {
    "customers":          ["gender", "region", "acquisition_channel", "credit_tier", "is_active"],
    "transactions":       ["product_type", "status", "merchant_category"],
    "loan_applications":  ["approval_status", "purpose", "default_flag"],
    "marketing_touches":  ["channel", "campaign", "converted"],
    "product_inventory":  ["category", "is_active"],
    "events_raw":         ["event_type", "page", "device", "utm_source", "is_bot"],
}

for table_name in tables:
    df = spark.read.format("delta").load(f"{BRONZE_PATH}/{table_name}")
    total_rows = df.count()
    
    print(f"\n{table_name.upper()} ({total_rows:,} rows)")
    print("-" * 40)
    
    for column in categorical_cols[table_name]:
        print(f"\n  {column}:")
        dist = (df.groupBy(column)
                  .count()
                  .orderBy("count", ascending=False))
        
        for row in dist.collect():
            value = str(row[column]) if row[column] is not None else "NULL"
            count_val = row["count"]
            pct = round((count_val / total_rows) * 100, 1)
            print(f"    {str(value):<25} {count_val:>6,} ({pct}%)")

print("\nValue distribution analysis complete!")

# COMMAND ----------

print("=" * 60)
print("DATA QUALITY SUMMARY REPORT")
print(f"Generated: {LOAD_DATE}")
print("=" * 60)

summary = {
    "customers": {
        "total_rows": 1020,
        "intentional_nulls": ["email (5%)", "gender (5%)", "annual_income (5%)"],
        "intentional_dupes": "~2% duplicate rows",
        "casing_issues": ["gender", "region"],
        "outliers": "annual_income — 2 rows beyond 3 std devs",
        "business_logic_nulls": "None",
    },
    "transactions": {
        "total_rows": 10200,
        "intentional_nulls": ["status (5%)", "merchant_category (5%)"],
        "intentional_dupes": "~2% duplicate rows",
        "casing_issues": ["product_type", "status"],
        "outliers": "amount — 100 rows beyond 3 std devs (intentional)",
        "business_logic_nulls": "None",
    },
    "loan_applications": {
        "total_rows": 2040,
        "intentional_nulls": ["purpose (5%)"],
        "intentional_dupes": "~2% duplicate rows",
        "casing_issues": ["approval_status", "purpose"],
        "outliers": "requested_amount — 20 rows beyond 3 std devs (intentional)",
        "business_logic_nulls": "interest_rate (44%) — NULL for Declined and Under Review loans by design",
    },
    "marketing_touches": {
        "total_rows": 5100,
        "intentional_nulls": ["cost (5%)", "utm_source (5%)", "utm_campaign (5%)"],
        "intentional_dupes": "~2% duplicate rows",
        "casing_issues": ["channel"],
        "outliers": "cost — 50 rows beyond 3 std devs (intentional)",
        "business_logic_nulls": "None",
    },
    "product_inventory": {
        "total_rows": 102,
        "intentional_nulls": ["selling_price (5%)", "supplier (5%)"],
        "intentional_dupes": "~2% duplicate rows",
        "casing_issues": "None",
        "outliers": "unit_cost, selling_price — 2 rows each",
        "business_logic_nulls": "None",
    },
    "events_raw": {
        "total_rows": 15300,
        "intentional_nulls": ["device (5%)", "utm_campaign (5%)"],
        "intentional_dupes": "~2% duplicate rows",
        "casing_issues": ["event_type", "device"],
        "outliers": "None",
        "business_logic_nulls": "customer_id (33%) — NULL for anonymous visitors by design",
    },
}

for table_name, details in summary.items():
    print(f"\n{table_name.upper()}")
    print(f"  Total Rows:          {details['total_rows']:,}")
    print(f"  Intentional Nulls:   {', '.join(details['intentional_nulls'])}")
    print(f"  Duplicates:          {details['intentional_dupes']}")
    print(f"  Casing Issues:       {details['casing_issues']}")
    print(f"  Outliers:            {details['outliers']}")
    print(f"  Business Logic Nulls:{details['business_logic_nulls']}")

print("\n" + "=" * 60)
print("SILVER TRANSFORM PRIORITIES")
print("=" * 60)
print("""
CRITICAL (fix before any analysis):
  1. Standardize text casing across all affected columns
  2. Remove duplicate rows using window functions
  3. Flag and isolate outlier values

MODERATE (fix before dashboards):
  4. Fill or flag NULL values in non-business-logic columns
  5. Standardize date formats to ISO-8601

EXPECTED (document but do not fix):
  6. interest_rate NULLs — business logic for non-approved loans
  7. customer_id NULLs in events_raw — anonymous visitors by design
""")

print("Data quality summary complete!")