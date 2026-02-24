-- models/facts/fct_marketing_spend.sql
-- Marketing spend fact table for the star schema
-- One row per marketing touch with spend and performance metrics
-- Designed for channel performance and ROI analysis

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

        -- Surrogate keys for dimension joins
        {{ dbt_utils.generate_surrogate_key(['t.channel', 't.campaign']) }} as channel_key,
        {{ dbt_utils.generate_surrogate_key(['t.campaign', 't.utm_campaign']) }} as campaign_key,

        -- Degenerate dimensions
        t.channel,
        t.campaign,
        t.utm_source,
        t.utm_campaign,

        -- Measures
        t.cost                                       as touch_cost,

        -- Conversion metrics
        case
            when t.converted = true then 1
            else 0
        end                                          as is_converted,

        case
            when t.converted = true then t.cost
            else 0
        end                                          as converted_spend,

        case
            when t.converted = false then t.cost
            else 0
        end                                          as unconverted_spend,

        -- Customer context
        c.credit_tier,
        c.region,
        c.acquisition_channel                        as customer_acquisition_channel,

        -- Quality metadata
        t.data_quality_score,
        t.is_cost_outlier

    from touches t
    left join customers c
        on t.customer_id = c.customer_id

    where t.is_cost_outlier    = false
    and   t.data_quality_score = 1.0
)

select * from final