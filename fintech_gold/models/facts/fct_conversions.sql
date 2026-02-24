-- models/facts/fct_conversions.sql
-- Conversions fact table for the star schema
-- One row per marketing touch that resulted in a conversion
-- Links marketing activity to customer acquisition outcomes

with touches as (
    select * from {{ ref('stg_marketing_touches') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

final as (
    select
        -- Primary key
        t.touch_id,

        -- Foreign keys
        t.customer_id,
        t.touchpoint_date,

        -- Degenerate dimensions
        t.channel,
        t.campaign,
        t.utm_source,
        t.utm_campaign,

        -- Measures
        t.cost                                       as touch_cost,

        -- Conversion flag
        case
            when t.converted = true then 1
            else 0
        end                                          as is_converted,

        -- Customer context at conversion
        c.credit_tier,
        c.region,
        c.acquisition_channel,

        -- Cost efficiency metrics
        case
            when t.converted = true then t.cost
            else null
        end                                          as cost_per_conversion,

        -- Quality metadata
        t.data_quality_score,
        t.is_cost_outlier

    from touches t
    left join customers c
        on t.customer_id = c.customer_id

    where t.is_cost_outlier     = false
    and   t.data_quality_score  = 1.0
)

select * from final