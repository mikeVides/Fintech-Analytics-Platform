-- models/facts/fct_transactions.sql
-- Transaction fact table for the star schema
-- One row per transaction with foreign keys to dimensions
-- and measurable transaction metrics

with transactions as (
    select * from {{ ref('stg_transactions') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

final as (
    select
        -- Primary key
        t.transaction_id,

        -- Foreign keys
        t.customer_id,
        cast(t.timestamp as date)                    as transaction_date,

        -- Degenerate dimensions (descriptive but not worth a separate table)
        t.product_type,
        t.status,
        t.merchant_category,

        -- Measures
        t.amount                                     as transaction_amount,

        -- Derived measures
        case
            when t.status = 'Completed' then t.amount
            else 0
        end                                          as completed_amount,

        case
            when t.status = 'Failed' then 1
            else 0
        end                                          as is_failed,

        -- Days since customer signup
        datediff(
            cast(t.timestamp as date),
            c.signup_date
        )                                            as days_since_signup_at_transaction,

        -- Quality metadata
        t.data_quality_score,
        t.is_amount_outlier

    from transactions t
    left join customers c
        on t.customer_id = c.customer_id

    where t.is_amount_outlier = false
    and   t.data_quality_score = 1.0
)

select * from final