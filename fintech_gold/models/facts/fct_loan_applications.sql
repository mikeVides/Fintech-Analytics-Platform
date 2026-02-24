-- models/facts/fct_loan_applications.sql
-- Loan applications fact table for the star schema
-- One row per loan application with foreign keys to dimensions
-- and measurable loan metrics

with loans as (
    select * from {{ ref('stg_loan_applications') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

final as (
    select
        -- Primary key
        l.application_id,

        -- Foreign keys
        l.customer_id,
        l.application_date,

        -- Degenerate dimensions
        l.approval_status,
        l.purpose,
        l.loan_term_months,

        -- Measures
        l.requested_amount,
        l.loan_amount,
        l.interest_rate,

        -- Derived measures
        case
            when l.approval_status = 'Approved' then 1
            else 0
        end                                          as is_approved,

        case
            when l.approval_status = 'Declined' then 1
            else 0
        end                                          as is_declined,

        case
            when l.default_flag = true then 1
            else 0
        end                                          as is_default,

        round(
            l.loan_amount * l.interest_rate / 100, 2
        )                                            as annual_interest_amount,

        round(
            l.requested_amount - l.loan_amount, 2
        )                                            as amount_gap,

        datediff(
            l.application_date,
            c.signup_date
        )                                            as days_since_signup_at_application,

        -- Credit context at time of application
        c.credit_tier,
        c.credit_score,

        -- Quality metadata
        l.data_quality_score,
        l.is_requested_amount_outlier,
        l.is_loan_amount_outlier

    from loans l
    left join customers c
        on l.customer_id = c.customer_id

    where l.is_requested_amount_outlier = false
    and   l.is_loan_amount_outlier      = false
    and   l.data_quality_score          = 1.0
)

select * from final