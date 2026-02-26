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

conversion_dates AS (
    SELECT
        customer_id,
        MIN(CASE WHEN is_converted = 1 THEN touchpoint_date END) AS conversion_date,
        SUM(is_converted) AS total_conversions
    FROM touches
    GROUP BY customer_id
),

touches_with_conversion AS (
    SELECT
        t.touch_id,
        t.customer_id,
        t.channel,
        t.campaign,
        t.touchpoint_date,
        t.touch_cost,
        t.is_converted,
        c.conversion_date,
        c.total_conversions,
        DATEDIFF(c.conversion_date, t.touchpoint_date) AS days_before_conversion
    FROM touches t
    INNER JOIN conversion_dates c
        ON t.customer_id = c.customer_id
),

touches_with_weights AS (
    SELECT
        touch_id,
        customer_id,
        channel,
        campaign,
        touchpoint_date,
        touch_cost,
        is_converted,
        total_conversions,
        days_before_conversion,
        CASE
            WHEN total_conversions > 0 AND days_before_conversion >= 0
            THEN POWER(2.0, -1.0 * days_before_conversion / 7.0)
            ELSE 0.0
        END AS decay_weight
    FROM touches_with_conversion
),

weight_totals AS (
    SELECT
        customer_id,
        SUM(decay_weight) AS total_decay_weight
    FROM touches_with_weights
    GROUP BY customer_id
),

final AS (
    SELECT
        w.touch_id,
        w.customer_id,
        w.channel,
        w.campaign,
        w.touchpoint_date,
        w.touch_cost,
        w.is_converted,
        w.days_before_conversion,
        CASE
            WHEN wt.total_decay_weight > 0
            THEN ROUND(w.decay_weight / wt.total_decay_weight, 6)
            ELSE 0.0
        END AS attribution_credit,
        CASE
            WHEN wt.total_decay_weight > 0
            THEN ROUND(w.decay_weight / wt.total_decay_weight, 6)
            ELSE 0.0
        END AS attributed_conversions,
        'time_decay' AS attribution_model
    FROM touches_with_weights w
    INNER JOIN weight_totals wt
        ON w.customer_id = wt.customer_id
)

SELECT * FROM final