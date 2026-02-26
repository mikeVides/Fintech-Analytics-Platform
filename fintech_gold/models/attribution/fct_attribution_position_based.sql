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

customer_journey AS (
    SELECT
        touch_id,
        customer_id,
        channel,
        campaign,
        touchpoint_date,
        touch_cost,
        is_converted,
        COUNT(touch_id) OVER (PARTITION BY customer_id) AS total_touches,
        SUM(is_converted) OVER (PARTITION BY customer_id) AS total_conversions,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY touchpoint_date ASC, touch_id ASC
        ) AS touch_position
    FROM touches
),

middle_touch_counts AS (
    SELECT
        customer_id,
        GREATEST(total_touches - 2, 0) AS middle_touch_count
    FROM customer_journey
    GROUP BY customer_id, total_touches
),

final AS (
    SELECT
        j.touch_id,
        j.customer_id,
        j.channel,
        j.campaign,
        j.touchpoint_date,
        j.touch_cost,
        j.is_converted,
        j.total_touches,
        j.touch_position,
        CASE
            WHEN j.touch_position = 1                  THEN 'first'
            WHEN j.touch_position = j.total_touches    THEN 'last'
            ELSE 'middle'
        END AS touch_position_label,
        CASE
            WHEN j.total_conversions = 0               THEN 0.0
            WHEN j.total_touches = 1                   THEN 1.0
            WHEN j.total_touches = 2                   THEN 0.5
            WHEN j.touch_position = 1                  THEN 0.40
            WHEN j.touch_position = j.total_touches    THEN 0.40
            ELSE ROUND(0.20 / m.middle_touch_count, 6)
        END AS attribution_credit,
        CASE
            WHEN j.total_conversions = 0               THEN 0.0
            WHEN j.total_touches = 1                   THEN 1.0
            WHEN j.total_touches = 2                   THEN 0.5
            WHEN j.touch_position = 1                  THEN 0.40
            WHEN j.touch_position = j.total_touches    THEN 0.40
            ELSE ROUND(0.20 / m.middle_touch_count, 6)
        END AS attributed_conversions,
        'position_based' AS attribution_model
    FROM customer_journey j
    INNER JOIN middle_touch_counts m
        ON j.customer_id = m.customer_id
)

SELECT * FROM final