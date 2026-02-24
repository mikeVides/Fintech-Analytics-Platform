# Databricks notebook source
# Create serving views for Tableau
# These views sit on top of Gold mart tables and provide
# a clean, stable interface for BI tools

serving_views = [
    {
        "view_name": "srv_channel_performance",
        "source": "default.mart_channel_performance",
        "description": "Marketing channel performance metrics for Tableau dashboards"
    },
    {
        "view_name": "srv_cohort_analysis",
        "source": "default.mart_cohort_analysis",
        "description": "Customer cohort retention analysis for Tableau dashboards"
    },
    {
        "view_name": "srv_inventory_health",
        "source": "default.mart_inventory_health",
        "description": "Inventory health metrics for Tableau dashboards"
    },
    {
        "view_name": "srv_credit_risk",
        "source": "default.mart_credit_risk",
        "description": "Credit risk metrics for Tableau dashboards"
    }
]

for view in serving_views:
    spark.sql(f"""
        CREATE OR REPLACE VIEW workspace.default.{view['view_name']}
        COMMENT '{view['description']}'
        AS SELECT * FROM workspace.{view['source']}
    """)
    print(f"Created: workspace.default.{view['view_name']}")

print("\nAll serving views created successfully!")