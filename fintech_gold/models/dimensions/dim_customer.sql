-- models/dimensions/dim_customer.sql
-- Customer dimension table for the star schema
-- One row per customer with all descriptive attributes

with source as (
    select * from {{ ref('stg_customers') }}
),

final as (
    select
        -- Primary key
        customer_id,

        -- Personal attributes
        first_name,
        last_name,
        concat(first_name, ' ', last_name) as full_name,
        email,
        gender,

        -- Geographic attributes
        region,

        -- Acquisition attributes
        acquisition_channel,
        signup_date,

        -- Credit attributes
        credit_tier,
        credit_score,

        -- Financial attributes
        annual_income,

        -- Derived attributes
        case
            when credit_score >= 750 then 'Excellent'
            when credit_score >= 700 then 'Good'
            when credit_score >= 650 then 'Fair'
            else 'Poor'
        end as credit_score_band,

        case
            when annual_income >= 100000 then 'High'
            when annual_income >= 60000  then 'Medium'
            else 'Low'
        end as income_band,

        datediff(current_date(), signup_date) as days_since_signup,

        -- Status
        is_active,

        -- Quality metadata
        data_quality_score

    from source
)

select * from final