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

customer_stats AS (
    SELECT
        customer_id,
        COUNT(touch_id)   AS total_touches,
        SUM(is_converted) AS total_conversions
    FROM touches
    GROUP BY customer_id
),

touches_with_stats AS (
    SELECT
        t.touch_id,
        t.customer_id,
        t.channel,
        t.campaign,
        t.touchpoint_date,
        t.touch_cost,
        t.is_converted,
        s.total_touches,
        s.total_conversions
    FROM touches t
    INNER JOIN customer_stats s
        ON t.customer_id = s.customer_id
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
            WHEN total_conversions > 0
            THEN ROUND(1.0 / total_touches, 6)
            ELSE 0.0
        END AS attribution_credit,
        CASE
            WHEN total_conversions > 0
            THEN ROUND(1.0 / total_touches, 6)
            ELSE 0.0
        END AS attributed_conversions,
        'linear' AS attribution_model
    FROM touches_with_stats
)

SELECT * FROM final