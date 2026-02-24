-- models/staging/stg_transactions.sql
-- Staging view on top of Silver transactions table

select
    transaction_id,
    customer_id,
    amount,
    product_type,
    status,
    timestamp,
    merchant_category,
    is_amount_outlier,
    is_duplicate,
    data_quality_score
from workspace.default.silver_transactions