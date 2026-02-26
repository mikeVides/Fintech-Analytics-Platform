WITH touches AS (
    SELECT
        touch_id,
        customer_id,
        channel,
        campaign,
        touchpoint_date,
        touch_cost,
        is_converted
    FROM {{ ref('fct_marketing_spend') }}
),

customer_touch_sequences AS (
    SELECT
        touch_id,
        customer_id,
        channel,
        campaign,
        touchpoint_date,
        touch_cost,
        is_converted,
        CASE
            WHEN SUM(is_converted) OVER (PARTITION BY customer_id) > 0
            THEN ROW_NUMBER() OVER (
                    PARTITION BY customer_id
                    ORDER BY touchpoint_date ASC
                 )
            ELSE NULL
        END AS touch_rank_asc
    FROM touches
),

final AS (
    SELECT
        touch_id,
        customer_id,
        channel,
        campaign,
        touchpoint_date,
        touch_cost,
        is_converted,
        CASE
            WHEN touch_rank_asc = 1
             AND SUM(is_converted) OVER (PARTITION BY customer_id) > 0
            THEN 1.0
            ELSE 0.0
        END AS attribution_credit,
        CASE
            WHEN touch_rank_asc = 1
             AND SUM(is_converted) OVER (PARTITION BY customer_id) > 0
            THEN 1.0
            ELSE 0.0
        END AS attributed_conversions,
        'first_click' AS attribution_model
    FROM customer_touch_sequences
)

SELECT * FROM final