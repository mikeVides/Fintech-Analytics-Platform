import pandas as pd
import numpy as np
from faker import Faker
import os
import random
from datetime import datetime, timedelta

# ── Configuration — change these numbers to control volume ──
NUM_CUSTOMERS    = 10000
NUM_TRANSACTIONS = 100000
NUM_LOANS        = 20000
NUM_TOUCHES      = 50000
NUM_PRODUCTS     = 1000
NUM_EVENTS       = 150000

# ── Setup ──
fake = Faker()
Faker.seed(42)
np.random.seed(42)
random.seed(42)

OUTPUT_DIR = "data_gen/output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("Starting data generation...")

def generate_customers(n):
    customers = []
    for i in range(n):
        customers.append({
            "customer_id":         f"CUST_{i+1:05d}",
            "first_name":          fake.first_name(),
            "last_name":           fake.last_name(),
            "email":               fake.email(),
            "date_of_birth":       fake.date_of_birth(minimum_age=18, maximum_age=75),
            "gender":              random.choice(["Male", "Female", "Non-binary"]),
            "region":              np.random.choice(["Northeast", "South", "Midwest", "West"]),
            "acquisition_channel": np.random.choice(
                                       ["Email", "Social", "Direct", "Referral"],
                                       p=[0.30, 0.25, 0.25, 0.20]),
            "signup_date":         fake.date_between(start_date="-5y", end_date="today"),
            "credit_tier":         np.random.choice(
                                       ["Prime", "Near-Prime", "Subprime"],
                                       p=[0.60, 0.25, 0.15]),
            "credit_score":        np.random.randint(300, 850),
            "annual_income":       round(abs(np.random.normal(65000, 20000)), 2),
            "is_active":           np.random.choice([True, False], p=[0.85, 0.15]),
        })
    return pd.DataFrame(customers)

def generate_transactions(n, customer_ids):
    transactions = []
    for i in range(n):
        transactions.append({
            "transaction_id":       f"TXN_{i+1:07d}",
            "customer_id":          np.random.choice(customer_ids),
            "amount":               round(abs(np.random.normal(250, 180)), 2),
            "product_type":         np.random.choice(
                                        ["Personal Loan", "Credit Card", "Savings", "Checking"],
                                        p=[0.30, 0.35, 0.20, 0.15]),
            "status":               np.random.choice(
                                        ["Completed", "Pending", "Failed", "Refunded"],
                                        p=[0.85, 0.08, 0.05, 0.02]),
            "timestamp":            fake.date_time_between(start_date="-5y", end_date="now"),
            "merchant_category":    np.random.choice(
                                        ["Retail", "Food", "Travel", "Healthcare", "Entertainment"],
                                        p=[0.30, 0.25, 0.20, 0.15, 0.10]),
        })
    return pd.DataFrame(transactions)

def generate_loan_applications(n, customer_ids):
    loans = []
    for i in range(n):
        status = np.random.choice(
            ["Approved", "Declined", "Under Review"],
            p=[0.60, 0.30, 0.10])
        requested = round(abs(np.random.normal(15000, 8000)), 2)
        loan_amount = round(requested * np.random.uniform(0.7, 1.0), 2) if status == "Approved" else 0
        interest_rate = round(np.random.uniform(5, 10), 2) if status == "Approved" else None
        default_flag = np.random.choice([True, False], p=[0.08, 0.92]) if status == "Approved" else False

        loans.append({
            "application_id":   f"LOAN_{i+1:06d}",
            "customer_id":      np.random.choice(customer_ids),
            "application_date": fake.date_between(start_date="-5y", end_date="today"),
            "requested_amount": requested,
            "approval_status":  status,
            "loan_amount":      loan_amount,
            "interest_rate":    interest_rate,
            "loan_term_months": np.random.choice([12, 24, 36, 48, 60]),
            "default_flag":     default_flag,
            "purpose":          np.random.choice(
                                    ["Debt Consolidation", "Home Improvement",
                                     "Medical", "Education", "Other"],
                                    p=[0.35, 0.25, 0.15, 0.15, 0.10]),
        })
    return pd.DataFrame(loans)

def generate_marketing_touches(n, customer_ids):
    channel_campaigns = {
        "Email":       ["Welcome Series", "Re-engagement", "Promotional"],
        "Social":      ["Brand Awareness", "Retargeting", "Lookalike"],
        "Direct Mail": ["Seasonal Offer", "Pre-approved Offer"],
        "Digital":     ["Search Retargeting", "Display Prospecting"],
    }
    channel_costs = {
        "Email":       (0.01, 0.50),
        "Digital":     (0.50, 5.00),
        "Social":      (1.00, 8.00),
        "Direct Mail": (2.00, 8.00),
    }
    channel_conversion = {
        "Email":       [0.05, 0.95],
        "Social":      [0.03, 0.97],
        "Direct Mail": [0.04, 0.96],
        "Digital":     [0.02, 0.98],
    }

    touches = []
    for i in range(n):
        channel = np.random.choice(
            ["Email", "Social", "Direct Mail", "Digital"],
            p=[0.35, 0.25, 0.20, 0.20])
        campaign = random.choice(channel_campaigns[channel])
        cost_min, cost_max = channel_costs[channel]
        conv_probs = channel_conversion[channel]

        touches.append({
            "touch_id":       f"TOUCH_{i+1:07d}",
            "customer_id":    np.random.choice(customer_ids),
            "channel":        channel,
            "campaign":       campaign,
            "touchpoint_date":fake.date_between(start_date="-5y", end_date="today"),
            "cost":           round(np.random.uniform(cost_min, cost_max), 2),
            "converted":      np.random.choice([True, False], p=conv_probs),
            "utm_source":     channel.lower().replace(" ", "_"),
            "utm_campaign":   campaign.lower().replace(" ", "_"),
        })
    return pd.DataFrame(touches)

def generate_product_inventory(n):
    products = []
    for i in range(n):
        unit_cost = round(abs(np.random.normal(45, 25)), 2)
        stock_level = np.random.randint(0, 500)
        reorder_point = np.random.randint(20, 100)

        products.append({
            "product_id":       f"PROD_{i+1:04d}",
            "sku":              f"SKU-{fake.bothify('??##-###')}",
            "product_name":     fake.catch_phrase(),
            "category":         np.random.choice(
                                    ["Prepaid Card", "Card Reader",
                                     "Marketing Material", "Hardware Device", "Printed Form"],
                                    p=[0.30, 0.25, 0.20, 0.15, 0.10]),
            "unit_cost":        unit_cost,
            "selling_price":    round(unit_cost * np.random.uniform(1.2, 2.5), 2),
            "stock_level":      stock_level,
            "reorder_point":    reorder_point,
            "reorder_quantity": np.random.randint(100, 500),
            "lead_time_days":   np.random.randint(3, 30),
            "supplier":         fake.company(),
            "is_active":        np.random.choice([True, False], p=[0.90, 0.10]),
        })
    return pd.DataFrame(products)

def generate_events_raw(n, customer_ids):
    source_campaigns = {
        "email":       ["welcome_series", "re_engagement", "promotional"],
        "social":      ["brand_awareness", "retargeting", "lookalike"],
        "direct_mail": ["seasonal_offer", "pre_approved_offer"],
        "digital":     ["search_retargeting", "display_prospecting"],
        "organic":     ["none"],
    }

    events = []
    for i in range(n):
        utm_source = np.random.choice(
            ["email", "social", "direct_mail", "digital", "organic"],
            p=[0.30, 0.25, 0.20, 0.15, 0.10])
        utm_campaign = random.choice(source_campaigns[utm_source])
        customer = np.random.choice(
            [*customer_ids, None],
            p=[0.70] * len(customer_ids) / len(customer_ids) if False else
            [*[0.70 / len(customer_ids)] * len(customer_ids), 0.30])

        events.append({
            "event_id":     f"EVT_{i+1:08d}",
            "session_id":   f"SESS_{np.random.randint(1, max(2, int(n*0.3))):06d}",
            "customer_id":  customer,
            "event_type":   np.random.choice(
                                ["page_view", "cta_click", "form_start",
                                 "form_submit", "conversion"],
                                p=[0.50, 0.25, 0.10, 0.08, 0.07]),
            "page":         np.random.choice(
                                ["home", "product", "apply", "pricing", "about"],
                                p=[0.30, 0.25, 0.20, 0.15, 0.10]),
            "timestamp":    fake.date_time_between(start_date="-5y", end_date="now"),
            "device":       np.random.choice(
                                ["mobile", "desktop", "tablet"],
                                p=[0.55, 0.35, 0.10]),
            "utm_source":   utm_source,
            "utm_campaign": utm_campaign,
            "is_bot":       np.random.choice([True, False], p=[0.05, 0.95]),
        })
    return pd.DataFrame(events)

def introduce_quality_issues(df, df_name):
    df = df.copy()
    n = len(df)

    # ── 1. Null values (~5% per eligible column) ──
    nullable_columns = {
        "customers":          ["email", "annual_income", "gender"],
        "transactions":       ["merchant_category", "status"],
        "loan_applications":  ["interest_rate", "purpose"],
        "marketing_touches":  ["utm_source", "utm_campaign", "cost"],
        "product_inventory":  ["supplier", "selling_price"],
        "events_raw":         ["customer_id", "utm_campaign", "device"],
    }
    if df_name in nullable_columns:
        for col in nullable_columns[df_name]:
            if col in df.columns:
                null_indices = np.random.choice(df.index, size=int(n * 0.05), replace=False)
                df.loc[null_indices, col] = None

    # ── 2. Duplicate records (~2% of records) ──
    n_dupes = int(n * 0.02)
    dupe_indices = np.random.choice(df.index, size=n_dupes, replace=False)
    dupes = df.loc[dupe_indices].copy()
    df = pd.concat([df, dupes], ignore_index=True)

    # ── 3. Inconsistent date formats ──
    date_columns = {
        "customers":         ["signup_date"],
        "transactions":      ["timestamp"],
        "loan_applications": ["application_date"],
        "marketing_touches": ["touchpoint_date"],
        "events_raw":        ["timestamp"],
    }
    if df_name in date_columns:
        for col in date_columns[df_name]:
            if col in df.columns:
                format_indices = np.random.choice(df.index, size=int(n * 0.10), replace=False)
                for idx in format_indices:
                    val = df.loc[idx, col]
                    if val is not None and str(val) != "None":
                        fmt = random.choice(["american", "long"])
                        try:
                            if fmt == "american":
                                df.loc[idx, col] = str(val).replace("-", "/")
                            elif fmt == "long":
                                import datetime
                                if hasattr(val, "strftime"):
                                    df.loc[idx, col] = val.strftime("%B %d %Y")
                        except:
                            pass

    # ── 4. Outlier values ──
    outlier_columns = {
        "transactions":      "amount",
        "loan_applications": "requested_amount",
        "marketing_touches": "cost",
    }
    if df_name in outlier_columns:
        col = outlier_columns[df_name]
        if col in df.columns:
            outlier_indices = np.random.choice(df.index, size=int(n * 0.01), replace=False)
            df.loc[outlier_indices, col] = df[col].mean() * np.random.uniform(10, 50)

    # ── 5. Inconsistent text casing ──
    casing_columns = {
        "customers":         ["gender", "region"],
        "transactions":      ["status", "product_type"],
        "loan_applications": ["approval_status", "purpose"],
        "marketing_touches": ["channel"],
        "events_raw":        ["device", "event_type"],
    }
    if df_name in casing_columns:
        for col in casing_columns[df_name]:
            if col in df.columns:
                case_indices = np.random.choice(df.index, size=int(n * 0.08), replace=False)
                for idx in case_indices:
                    val = df.loc[idx, col]
                    if val is not None and isinstance(val, str):
                        fmt = random.choice(["upper", "lower"])
                        df.loc[idx, col] = val.upper() if fmt == "upper" else val.lower()

    return df

# Generate and save customers
df_customers = generate_customers(NUM_CUSTOMERS)
df_customers = introduce_quality_issues(df_customers, "customers")
df_customers.to_csv(f"{OUTPUT_DIR}/customers.csv", index=False)
print(f"customers.csv generated — {len(df_customers)} rows")

# Generate and save transactions
df_transactions = generate_transactions(NUM_TRANSACTIONS, df_customers["customer_id"].values)
df_transactions = introduce_quality_issues(df_transactions, "transactions")
df_transactions.to_csv(f"{OUTPUT_DIR}/transactions.csv", index=False)
print(f"transactions.csv generated — {len(df_transactions)} rows")

# Generate and save loan applications
df_loans = generate_loan_applications(NUM_LOANS, df_customers["customer_id"].values)
df_loans = introduce_quality_issues(df_loans, "loan_applications")
df_loans.to_csv(f"{OUTPUT_DIR}/loan_applications.csv", index=False)
print(f"loan_applications.csv generated — {len(df_loans)} rows")

# Generate and save marketing touches
df_touches = generate_marketing_touches(NUM_TOUCHES, df_customers["customer_id"].values)
df_touches = introduce_quality_issues(df_touches, "marketing_touches")
df_touches.to_csv(f"{OUTPUT_DIR}/marketing_touches.csv", index=False)
print(f"marketing_touches.csv generated — {len(df_touches)} rows")

# Generate and save product inventory
df_products = generate_product_inventory(NUM_PRODUCTS)
df_products = introduce_quality_issues(df_products, "product_inventory")
df_products.to_csv(f"{OUTPUT_DIR}/product_inventory.csv", index=False)
print(f"product_inventory.csv generated — {len(df_products)} rows")

# Generate and save events
df_events = generate_events_raw(NUM_EVENTS, df_customers["customer_id"].values)
df_events = introduce_quality_issues(df_events, "events_raw")
df_events.to_csv(f"{OUTPUT_DIR}/events_raw.csv", index=False)
print(f"events_raw.csv generated — {len(df_events)} rows")

print("\nAll tables generated successfully!")