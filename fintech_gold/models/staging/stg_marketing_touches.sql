-- models/staging/stg_marketing_touches.sql
-- Staging view on top of Silver marketing_touches table

select
    touch_id,
    customer_id,
    channel,
    campaign,
    touchpoint_date,
    cost,
    converted,
    utm_source,
    utm_campaign,
    is_cost_outlier,
    is_duplicate,
    data_quality_score
from workspace.default.silver_marketing_touches