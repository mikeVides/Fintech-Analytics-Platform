-- models/marts/mart_cohort_analysis.sql
-- Cohort analysis mart
-- Tracks customer behavior over time grouped by signup month
-- Answers: of customers who signed up in month X, how many
-- were still active N months later?

with customers as (
    select * from {{ ref('stg_customers') }}
),

transactions as (
    select * from {{ ref('fct_transactions') }}
),

-- Assign each customer to a cohort based on signup month
customer_cohorts as (
    select
        customer_id,
        signup_date,
        date_format(signup_date, 'yyyy-MM')          as cohort_month,
        credit_tier,
        region,
        acquisition_channel
    from customers
),

-- Get each customer's transaction activity by month
customer_activity as (
    select
        t.customer_id,
        date_format(t.transaction_date, 'yyyy-MM')   as activity_month,
        count(t.transaction_id)                      as monthly_transactions,
        sum(t.transaction_amount)                    as monthly_spend
    from transactions t
    where t.status = 'Completed'
    group by
        t.customer_id,
        date_format(t.transaction_date, 'yyyy-MM')
),

-- Join cohorts to activity
cohort_activity as (
    select
        c.cohort_month,
        c.customer_id,
        c.credit_tier,
        c.region,
        c.acquisition_channel,
        a.activity_month,
        a.monthly_transactions,
        a.monthly_spend,

        -- Calculate months since signup
        months_between(
            to_date(concat(a.activity_month, '-01')),
            to_date(concat(c.cohort_month,   '-01'))
        )                                            as months_since_signup

    from customer_cohorts c
    left join customer_activity a
        on c.customer_id = a.customer_id
),

final as (
    select
        cohort_month,
        credit_tier,
        region,
        acquisition_channel,
        cast(months_since_signup as int)             as months_since_signup,

        -- Cohort size
        count(distinct customer_id)                  as active_customers,

        -- Activity metrics
        sum(monthly_transactions)                    as total_transactions,
        round(sum(monthly_spend), 2)                 as total_spend,
        round(avg(monthly_spend), 2)                 as avg_spend_per_customer

    from cohort_activity
    where months_since_signup >= 0

    group by
        cohort_month,
        credit_tier,
        region,
        acquisition_channel,
        cast(months_since_signup as int)
)

select * from final