-- models/marts/mart_credit_risk.sql
-- Credit risk mart
-- Pre-aggregated risk metrics by credit tier, purpose, and region
-- Designed for risk management dashboards in Tableau

with loans as (
    select * from {{ ref('fct_loan_applications') }}
),

final as (
    select
        -- Dimensions
        credit_tier,
        approval_status,
        purpose,
        loan_term_months,

        -- Volume metrics
        count(application_id)                        as total_applications,

        sum(is_approved)                             as total_approved,
        sum(is_declined)                             as total_declined,
        sum(is_default)                              as total_defaults,

        -- Rate metrics
        round(
            avg(is_approved) * 100
        , 2)                                         as approval_rate_pct,

        round(
            avg(is_declined) * 100
        , 2)                                         as decline_rate_pct,

        coalesce(round(
            sum(is_default) /
            nullif(sum(is_approved), 0) * 100
        , 2), 0.0)                                   as default_rate_pct,

        -- Loan amount metrics
        round(avg(requested_amount), 2)              as avg_requested_amount,
        round(avg(loan_amount), 2)                   as avg_approved_amount,
        round(sum(loan_amount), 2)                   as total_loan_portfolio,
        round(avg(interest_rate), 2)                 as avg_interest_rate,
        round(avg(annual_interest_amount), 2)        as avg_annual_interest,

        -- Risk metrics
        round(avg(amount_gap), 2)                    as avg_amount_gap,
        round(avg(credit_score), 2)                  as avg_credit_score,

        -- Days to application
        round(
            avg(days_since_signup_at_application)
        , 0)                                         as avg_days_to_application

    from loans

    group by
        credit_tier,
        approval_status,
        purpose,
        loan_term_months
)

select * from final