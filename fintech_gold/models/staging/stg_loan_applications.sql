-- models/staging/stg_loan_applications.sql
-- Staging view on top of Silver loan_applications table

select
    application_id,
    customer_id,
    application_date,
    requested_amount,
    approval_status,
    loan_amount,
    interest_rate,
    loan_term_months,
    default_flag,
    purpose,
    is_requested_amount_outlier,
    is_loan_amount_outlier,
    is_duplicate,
    data_quality_score
from workspace.default.silver_loan_applications