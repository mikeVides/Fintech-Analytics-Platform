import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# ═══════════════════════════════════════════════════════
# TEST DATA SETUP
# ═══════════════════════════════════════════════════════

def make_customers():
    """Creates a small sample customers DataFrame with duplicates."""
    return pd.DataFrame({
        "customer_id":   ["CUST_00001", "CUST_00002", "CUST_00003", "CUST_00001"],
        "first_name":    ["Alice", "Bob", "Carol", "Alice"],
        "annual_income": [50000, 75000, 60000, 50000],
        "load_date":     ["2026-02-23", "2026-02-23", "2026-02-23", "2026-02-23"],
    })

def make_transactions():
    """Creates a sample transactions DataFrame with clear outliers."""
    np.random.seed(42)
    normal_amounts = list(np.random.normal(250, 100, 100).round(2))
    outlier_amounts = [25000.0, 30000.0]
    amounts = normal_amounts + outlier_amounts
    return pd.DataFrame({
        "transaction_id": [f"TXN_{i:03d}" for i in range(len(amounts))],
        "customer_id":    ["CUST_00001"] * len(amounts),
        "amount":         amounts,
        "status":         ["Completed"] * len(amounts),
    })

def make_events():
    """Creates a small sample events DataFrame for session boundary testing."""
    base_time = datetime(2024, 1, 1, 10, 0, 0)
    return pd.DataFrame({
        "event_id":    ["EVT_001", "EVT_002", "EVT_003", "EVT_004", "EVT_005"],
        "customer_id": ["CUST_00001"] * 5,
        "timestamp":   [
            base_time,
            base_time + timedelta(minutes=10),
            base_time + timedelta(minutes=20),
            base_time + timedelta(minutes=60),
            base_time + timedelta(minutes=70),
        ],
        "event_type": ["page_view", "cta_click", "form_start", "page_view", "cta_click"],
    })

# ═══════════════════════════════════════════════════════
# DEDUP LOGIC TESTS
# ═══════════════════════════════════════════════════════

def deduplicate(df, primary_key):
    """Replicates the dedup logic from Silver notebooks using pandas."""
    return df.drop_duplicates(subset=[primary_key], keep="first")

def test_dedup_removes_duplicates():
    """Verifies that deduplication removes duplicate primary keys."""
    df = make_customers()
    df_deduped = deduplicate(df, "customer_id")
    assert df_deduped["customer_id"].duplicated().sum() == 0

def test_dedup_correct_row_count():
    """Verifies that the correct number of rows remain after deduplication."""
    df = make_customers()
    df_deduped = deduplicate(df, "customer_id")
    assert len(df_deduped) == 3

def test_dedup_preserves_non_duplicates():
    """Verifies that non-duplicate rows are not removed during deduplication."""
    df = make_customers()
    df_deduped = deduplicate(df, "customer_id")
    assert "CUST_00002" in df_deduped["customer_id"].values
    assert "CUST_00003" in df_deduped["customer_id"].values

# ═══════════════════════════════════════════════════════
# OUTLIER THRESHOLD TESTS
# ═══════════════════════════════════════════════════════

def flag_outliers(df, column):
    """Replicates the Three Sigma Rule outlier flagging from Silver notebooks."""
    df = df.copy()
    mean  = df[column].mean()
    std   = df[column].std()
    upper = mean + (3 * std)
    df.loc[:, f"is_{column}_outlier"] = df[column] > upper
    return df, upper

def test_outlier_flags_extreme_values():
    """Verifies that extreme values are correctly flagged as outliers."""
    df = make_transactions()
    df_flagged, _ = flag_outliers(df, "amount")
    outlier_rows = df_flagged[df_flagged["is_amount_outlier"] == True]
    assert len(outlier_rows) > 0
    assert 25000.0 in outlier_rows["amount"].values
    assert 30000.0 in outlier_rows["amount"].values

def test_outlier_does_not_flag_normal_values():
    """Verifies that normal values are not incorrectly flagged as outliers."""
    df = make_transactions()
    df_flagged, _ = flag_outliers(df, "amount")
    normal_rows = df_flagged[df_flagged["is_amount_outlier"] == False]
    assert len(normal_rows) == 100

def test_outlier_upper_bound_is_three_sigma():
    """Verifies that the upper boundary is exactly mean + 3 * standard deviation."""
    df = make_transactions()
    _, upper = flag_outliers(df, "amount")
    mean = df["amount"].mean()
    std  = df["amount"].std()
    assert round(upper, 6) == round(mean + (3 * std), 6)

# ═══════════════════════════════════════════════════════
# SESSION BOUNDARY TESTS
# ═══════════════════════════════════════════════════════

def assign_sessions(df, timeout_minutes=30):
    """Replicates the session boundary logic from silver_sessionization."""
    df = df.copy().sort_values("timestamp").reset_index(drop=True)
    df.loc[:, "prev_timestamp"] = df.groupby("customer_id")["timestamp"].shift(1)
    df.loc[:, "time_gap_minutes"] = (
        df["timestamp"] - df["prev_timestamp"]
    ).dt.total_seconds() / 60
    df.loc[:, "is_new_session"] = (
        df["time_gap_minutes"] > timeout_minutes
    ) | (df["prev_timestamp"].isna())
    df.loc[:, "session_number"] = df.groupby("customer_id")["is_new_session"] \
                                    .cumsum().astype(int)
    df.loc[:, "session_id"] = (
        df["customer_id"] + df["session_number"].astype(str).str.zfill(4)
    )
    return df

def test_session_first_event_is_new_session():
    """Verifies that the first event for a customer always starts a new session."""
    df = make_events()
    df_sessions = assign_sessions(df)
    assert df_sessions.iloc[0]["is_new_session"] == True

def test_session_short_gap_same_session():
    """Verifies that events within 30 minutes are assigned to the same session."""
    df = make_events()
    df_sessions = assign_sessions(df)
    first_three = df_sessions.iloc[0:3]["session_id"].values
    assert first_three[0] == first_three[1] == first_three[2]

def test_session_long_gap_new_session():
    """Verifies that a gap over 30 minutes starts a new session."""
    df = make_events()
    df_sessions = assign_sessions(df)
    session_event_3 = df_sessions.iloc[2]["session_id"]
    session_event_4 = df_sessions.iloc[3]["session_id"]
    assert session_event_3 != session_event_4

def test_session_boundary_exactly_30_minutes():
    """Verifies that a gap of exactly 30 minutes does not start a new session."""
    df = pd.DataFrame({
        "event_id":    ["EVT_001", "EVT_002"],
        "customer_id": ["CUST_00001", "CUST_00001"],
        "timestamp":   [
            datetime(2024, 1, 1, 10, 0, 0),
            datetime(2024, 1, 1, 10, 30, 0),
        ],
        "event_type": ["page_view", "cta_click"],
    })
    df_sessions = assign_sessions(df)
    assert df_sessions.iloc[0]["session_id"] == df_sessions.iloc[1]["session_id"]