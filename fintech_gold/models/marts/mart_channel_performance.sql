-- models/marts/mart_channel_performance.sql
-- Channel performance mart
-- Pre-aggregated metrics per marketing channel and campaign
-- Designed for marketing ROI dashboards in Tableau

with spend as (
    select * from {{ ref('fct_marketing_spend') }}
),

conversions as (
    select * from {{ ref('fct_conversions') }}
),

final as (
    select
        -- Dimensions
        s.channel,
        s.campaign,
        s.utm_source,
        s.utm_campaign,
        s.credit_tier,
        s.region,

        -- Volume metrics
        count(s.touch_id)                            as total_touches,

        -- Spend metrics
        round(sum(s.touch_cost), 2)                  as total_spend,
        round(avg(s.touch_cost), 2)                  as avg_cost_per_touch,

        -- Conversion metrics
        sum(s.is_converted)                          as total_conversions,
        round(avg(s.is_converted) * 100, 2)          as conversion_rate_pct,

        -- Cost efficiency
        round(
            sum(s.touch_cost) / nullif(sum(s.is_converted), 0)
        , 2)                                         as cost_per_conversion,

        -- Converted vs unconverted spend
        round(sum(s.converted_spend), 2)             as converted_spend,
        round(sum(s.unconverted_spend), 2)           as unconverted_spend

    from spend s

    group by
        s.channel,
        s.campaign,
        s.utm_source,
        s.utm_campaign,
        s.credit_tier,
        s.region
)

select * from final