-- models/dimensions/dim_channel.sql
-- Channel dimension table for the star schema
-- One row per unique marketing channel and campaign combination

with source as (
    select * from {{ ref('stg_marketing_touches') }}
),

distinct_channels as (
    select distinct
        channel,
        campaign,

        case
            when channel = 'Email'       then 'Digital'
            when channel = 'Social'      then 'Digital'
            when channel = 'Digital'     then 'Digital'
            when channel = 'Direct Mail' then 'Traditional'
            else                              'Other'
        end as channel_type

    from source
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['channel', 'campaign']) }} as channel_key,
        channel,
        campaign,
        channel_type

    from distinct_channels
)

select * from final