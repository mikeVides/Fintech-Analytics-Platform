-- models/dimensions/dim_campaign.sql
-- Campaign dimension table for the star schema
-- One row per unique campaign with performance metadata

with source as (
    select * from {{ ref('stg_marketing_touches') }}
),

final as (
    select distinct
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['campaign', 'utm_campaign']) }} as campaign_key,

        -- Campaign attributes
        campaign,
        utm_campaign,

        -- Derived attributes
        case
            when campaign in ('Welcome Series', 'Re-engagement', 'Promotional') then 'Email'
            when campaign in ('Lookalike', 'Retargeting')                        then 'Social'
            when campaign in ('Display Prospecting', 'Search Retargeting')       then 'Digital'
            when campaign in ('Seasonal Offer', 'Pre-approved Offer')            then 'Direct Mail'
            else                                                                      'Other'
        end as primary_channel,

        case
            when campaign in ('Welcome Series', 'Lookalike', 'Display Prospecting') then 'Acquisition'
            when campaign in ('Re-engagement', 'Retargeting', 'Search Retargeting') then 'Retention'
            when campaign in ('Promotional', 'Seasonal Offer', 'Pre-approved Offer') then 'Conversion'
            else                                                                          'Other'
        end as campaign_objective

    from source
)

select * from final