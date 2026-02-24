-- models/staging/stg_customers.sql
-- Staging view on top of Silver customers table

select
    customer_id,
    first_name,
    last_name,
    email,
    date_of_birth,
    gender,
    region,
    acquisition_channel,
    signup_date,
    credit_tier,
    credit_score,
    annual_income,
    is_active,
    is_income_outlier,
    is_duplicate,
    data_quality_score
from workspace.default.silver_customers