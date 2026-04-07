# FinTech Analytics Intelligence Platform

> **Disclaimer:** The dataset used in this project is entirely synthetic
> and artificially generated using Python (Faker and NumPy). No real
> customer, financial, or business data was used. This project was built
> for educational purposes to demonstrate data analytics and business
> intelligence skills including Python, SQL, PySpark, dbt, and dashboard
> development.

---

## Live Dashboard

**[→ View Interactive Tableau Dashboard](https://public.tableau.com/app/profile/michael.benavides6128/viz/FinTechAnalyticsPlatform/DashboardChannelPerformance)**

![Dashboard Preview](https://i.imgur.com/F89q8Bq.png)

---

## Project Overview

Customer acquisition costs are rising, loan default rates are unclear,
and inventory stockout risks are unmonitored. Using Python, SQL, PySpark,
dbt, and Tableau, I built an end-to-end analytics platform on a synthetic
FinTech dataset, transforming raw messy data into actionable business
insights across marketing, credit risk, supply chain, and demand forecasting.

---

## Key Findings & Business Questions Answered

| Question | Finding |
|---|---|
| Which channel acquires customers at the lowest cost? | Email: ~$5 blended CAC vs $127–$150 for all other channels |
| How does attribution model choice affect budget allocation? | Credit shifts significantly. Email loses share under last-click, gains under time-decay |
| What is customer retention by channel at 30, 60, 90 days? | Visible in cohort dashboard, retention drops steepest in months 2–3 across all channels |
| Which credit tier and loan purpose has the highest default risk? | Subprime 8.3% vs Prime 7.8%, Unknown purpose has highest default across all tiers |
| Which product categories are at stockout risk? | Card Reader 53.5% out-of-stock rate, Prepaid Card 36.6%. Both need immediate reorder |
| What does the 365-day loan application forecast show? | Avg 10.25 daily applications, confidence range 6.2–14.2, trained through 8/31/2025 |
| What revenue opportunity did A/B testing identify? | ~$70M combined annual uplift across 3 tests |
| How many data quality tests are passing? | 75 / 75 dbt tests passing, zero failures |

---

## Skills Demonstrated

| Category | Skills |
|---|---|
| Data Engineering | PySpark, ETL pipelines, Delta Lake, window functions, sessionization |
| Data Modeling | dbt Core, star schema, dimensional modeling, 75 automated tests |
| Analytics | SQL, cohort analysis, multi-touch attribution (5 models), A/B testing |
| Forecasting | Prophet time series model, confidence intervals, MAPE tracking |
| Business Intelligence | Tableau dashboards, KPI development, colorblind-accessible design |
| Data Quality | Profiling, deduplication, outlier flagging, quality scoring per record |
| Automation & CI/CD | GitHub Actions, pytest unit tests, ruff linting |

---

## Tools & Technologies

| Layer | Tools |
|---|---|
| Environment | Anaconda, Python|
| Data Generation | Faker, NumPy, pandas |
| Data Platform | Databricks Community Edition, Delta Lake |
| Big Data Processing | PySpark |
| Data Modeling | dbt Core 1.11.6 + dbt-databricks |
| Attribution & A/B Testing | Python (scipy, statsmodels), SQL |
| Forecasting | Prophet (Meta) |
| Dashboard | Tableau Public |

---

## Architecture
```
Raw Data (Python / Faker + NumPy)
        │
        ▼
Bronze Layer — Databricks Delta Lake
6 tables · schema enforcement · partitioned by load date
        │
        ▼
Silver Layer — PySpark
Deduplication · standardization · quality flags · sessionization
9 tables including sessions, touchpoints, and customer journey
        │
        ▼
Gold Layer — dbt (29 models)
5 dimensions · 4 fact tables · 4 marts · 5 attribution models
75 tests passing · lineage DAG auto-generated
        │
        ▼
Analytics Layer — Databricks Notebooks
Prophet demand forecast · A/B tests · channel ROI analysis
        │
        ▼
Serving Views → Tableau Public (5 Dashboards)
```

---

## Phase Summary

| Phase | What Was Built | Key Deliverable |
|---|---|---|
| 1 | Synthetic data generator + Bronze Delta ingestion | 6 tables, 33K dev rows, intentional quality issues |
| 2 | Silver transforms, dedup, clean, sessionize, quality score | 9 Silver tables, pytest unit tests |
| 3 | dbt Gold layer, star schema + KPI marts | 29 models, 75 tests passing, lineage DAG |
| 4 | 5 attribution models + A/B testing + channel ROI | ~$70M revenue opportunity identified |
| 5 | Prophet demand forecast + inventory stockout warnings | MAPE 27.6%, 0.8% monthly variance |
| 6 | 5 Tableau dashboards published to Tableau Public | Live shareable URL |

---

## Project Status

Complete: Phases 1–6 Built and Published