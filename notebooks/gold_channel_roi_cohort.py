# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd

pd.set_option('display.float_format', '{:.2f}'.format)
print("Setup complete.")

# COMMAND ----------

cac_df = spark.sql("""
    SELECT
        channel,
        COUNT(DISTINCT customer_id)                    AS total_customers_touched,
        SUM(is_converted)                              AS conversions,
        ROUND(SUM(touch_cost), 2)                      AS total_spend,
        ROUND(
            SUM(CASE WHEN is_converted = 1 THEN touch_cost ELSE 0 END)
            / NULLIF(SUM(is_converted), 0),
        2) AS last_click_cac,
        ROUND(
            SUM(touch_cost) / NULLIF(SUM(is_converted), 0),
        2) AS blended_cac,
        ROUND(AVG(is_converted) * 100, 2) AS conversion_rate_pct
    FROM default.fct_marketing_spend
    GROUP BY channel
    ORDER BY blended_cac ASC
""")

display(cac_df)

# COMMAND ----------

cac_gap_df = spark.sql("""
    WITH channel_cac AS (
        SELECT
            channel,
            SUM(is_converted) AS conversions,
            ROUND(SUM(CASE WHEN is_converted=1 THEN touch_cost ELSE 0 END) / NULLIF(SUM(is_converted),0), 2) AS last_click_cac,
            ROUND(SUM(touch_cost) / NULLIF(SUM(is_converted),0), 2) AS blended_cac
        FROM default.fct_marketing_spend
        GROUP BY channel
    )
    SELECT
        channel,
        conversions,
        last_click_cac,
        blended_cac,
        ROUND(blended_cac - last_click_cac, 2) AS cac_gap,
        ROUND((blended_cac / NULLIF(last_click_cac, 0) - 1) * 100, 1) AS pct_underestimate
    FROM channel_cac
    ORDER BY pct_underestimate DESC
""")

display(cac_gap_df)

# COMMAND ----------

attribution_df = spark.sql("""
    SELECT
        channel,
        MAX(CASE WHEN attribution_model = 'last_click'     THEN conversion_share_pct END) AS last_click_pct,
        MAX(CASE WHEN attribution_model = 'first_click'    THEN conversion_share_pct END) AS first_click_pct,
        MAX(CASE WHEN attribution_model = 'linear'         THEN conversion_share_pct END) AS linear_pct,
        MAX(CASE WHEN attribution_model = 'time_decay'     THEN conversion_share_pct END) AS time_decay_pct,
        MAX(CASE WHEN attribution_model = 'position_based' THEN conversion_share_pct END) AS position_based_pct,
        ROUND(
            MAX(conversion_share_pct) - MIN(conversion_share_pct), 1
        ) AS variance_across_models
    FROM default.mart_attribution_comparison
    GROUP BY channel
    ORDER BY linear_pct DESC
""")

display(attribution_df)

# COMMAND ----------

reallocation_df = spark.sql("""
    SELECT
        channel,
        spend_share_pct,
        conversion_share_pct,
        reallocation_signal,
        CASE
            WHEN reallocation_signal >= 5  THEN 'Increase budget'
            WHEN reallocation_signal <= -5 THEN 'Reduce budget'
            ELSE 'Maintain budget'
        END AS recommendation
    FROM default.mart_attribution_comparison
    WHERE attribution_model = 'linear'
    ORDER BY reallocation_signal DESC
""")

display(reallocation_df)

# COMMAND ----------

cohort_df = spark.sql("""
    WITH customer_cohorts AS (
        SELECT
            customer_id,
            DATE_FORMAT(signup_date, 'yyyy-MM') AS cohort_month,
            signup_date,
            acquisition_channel
        FROM default.dim_customer
    ),
    customer_transactions AS (
        SELECT
            t.customer_id,
            t.transaction_date,
            DATEDIFF(t.transaction_date, c.signup_date) AS days_since_signup
        FROM default.fct_transactions t
        JOIN customer_cohorts c ON t.customer_id = c.customer_id
        WHERE t.is_failed = 0
    ),
    activity_flags AS (
        SELECT
            co.customer_id,
            co.cohort_month,
            co.acquisition_channel,
            MAX(CASE WHEN t.days_since_signup BETWEEN 1 AND 30  THEN 1 ELSE 0 END) AS active_30d,
            MAX(CASE WHEN t.days_since_signup BETWEEN 1 AND 60  THEN 1 ELSE 0 END) AS active_60d,
            MAX(CASE WHEN t.days_since_signup BETWEEN 1 AND 90  THEN 1 ELSE 0 END) AS active_90d
        FROM customer_cohorts co
        LEFT JOIN customer_transactions t ON co.customer_id = t.customer_id
        GROUP BY co.customer_id, co.cohort_month, co.acquisition_channel
    )
    SELECT
        cohort_month,
        COUNT(customer_id)              AS cohort_size,
        SUM(active_30d)                 AS retained_30d,
        SUM(active_60d)                 AS retained_60d,
        SUM(active_90d)                 AS retained_90d,
        ROUND(AVG(active_30d) * 100, 1) AS retention_rate_30d,
        ROUND(AVG(active_60d) * 100, 1) AS retention_rate_60d,
        ROUND(AVG(active_90d) * 100, 1) AS retention_rate_90d
    FROM activity_flags
    GROUP BY cohort_month
    ORDER BY cohort_month
""")

display(cohort_df)

# COMMAND ----------

channel_retention_df = spark.sql("""
    WITH activity AS (
        SELECT
            c.customer_id,
            c.acquisition_channel,
            MAX(CASE WHEN DATEDIFF(t.transaction_date, c.signup_date) BETWEEN 1 AND 30 THEN 1 ELSE 0 END) AS active_30d,
            MAX(CASE WHEN DATEDIFF(t.transaction_date, c.signup_date) BETWEEN 1 AND 60 THEN 1 ELSE 0 END) AS active_60d,
            MAX(CASE WHEN DATEDIFF(t.transaction_date, c.signup_date) BETWEEN 1 AND 90 THEN 1 ELSE 0 END) AS active_90d
        FROM default.dim_customer c
        LEFT JOIN default.fct_transactions t
            ON c.customer_id = t.customer_id AND t.is_failed = 0
        GROUP BY c.customer_id, c.acquisition_channel
    )
    SELECT
        acquisition_channel,
        COUNT(*)                           AS cohort_size,
        ROUND(AVG(active_30d) * 100, 1)   AS retention_30d_pct,
        ROUND(AVG(active_60d) * 100, 1)   AS retention_60d_pct,
        ROUND(AVG(active_90d) * 100, 1)   AS retention_90d_pct
    FROM activity
    GROUP BY acquisition_channel
    ORDER BY retention_90d_pct DESC
""")

display(channel_retention_df)

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE VIEW default.srv_channel_roi_summary AS
    SELECT
        channel,
        COUNT(DISTINCT customer_id)    AS total_customers_touched,
        SUM(is_converted)              AS conversions,
        ROUND(SUM(touch_cost), 2)      AS total_spend,
        ROUND(SUM(CASE WHEN is_converted = 1 THEN touch_cost ELSE 0 END)
            / NULLIF(SUM(is_converted), 0), 2) AS last_click_cac,
        ROUND(SUM(touch_cost)
            / NULLIF(SUM(is_converted), 0), 2) AS blended_cac,
        ROUND(AVG(is_converted) * 100, 2) AS conversion_rate_pct
    FROM default.fct_marketing_spend
    GROUP BY channel
""")

print("Serving view created successfully.")