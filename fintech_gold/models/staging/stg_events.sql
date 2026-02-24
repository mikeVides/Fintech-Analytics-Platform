-- models/staging/stg_events.sql
-- Staging view on top of Silver events_raw table

select
    event_id,
    session_id,
    customer_id,
    event_type,
    page,
    timestamp,
    device,
    utm_source,
    utm_campaign,
    is_bot,
    is_duplicate,
    data_quality_score
from workspace.default.silver_events_raw