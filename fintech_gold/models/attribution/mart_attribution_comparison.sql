WITH last_click AS (
    SELECT channel, campaign, touch_cost, attribution_credit, attributed_conversions,
           'last_click' AS attribution_model
    FROM {{ ref('fct_attribution_last_click') }}
),

first_click AS (
    SELECT channel, campaign, touch_cost, attribution_credit, attributed_conversions,
           'first_click' AS attribution_model
    FROM {{ ref('fct_attribution_first_click') }}
),

linear AS (
    SELECT channel, campaign, touch_cost, attribution_credit, attributed_conversions,
           'linear' AS attribution_model
    FROM {{ ref('fct_attribution_linear') }}
),

time_decay AS (
    SELECT channel, campaign, touch_cost, attribution_credit, attributed_conversions,
           'time_decay' AS attribution_model
    FROM {{ ref('fct_attribution_time_decay') }}
),

position_based AS (
    SELECT channel, campaign, touch_cost, attribution_credit, attributed_conversions,
           'position_based' AS attribution_model
    FROM {{ ref('fct_attribution_position_based') }}
),

all_models AS (
    SELECT * FROM last_click
    UNION ALL SELECT * FROM first_click
    UNION ALL SELECT * FROM linear
    UNION ALL SELECT * FROM time_decay
    UNION ALL SELECT * FROM position_based
),

channel_model_summary AS (
    SELECT
        attribution_model,
        channel,
        COUNT(*)                              AS total_touches,
        ROUND(SUM(touch_cost), 2)             AS total_spend,
        ROUND(SUM(attributed_conversions), 4) AS attributed_conversions
    FROM all_models
    GROUP BY attribution_model, channel
),

model_totals AS (
    SELECT
        attribution_model,
        SUM(attributed_conversions) AS model_total_conversions,
        SUM(total_spend)            AS model_total_spend
    FROM channel_model_summary
    GROUP BY attribution_model
),

final AS (
    SELECT
        s.attribution_model,
        s.channel,
        s.total_touches,
        s.total_spend,
        s.attributed_conversions,
        ROUND(
            s.attributed_conversions / NULLIF(t.model_total_conversions, 0) * 100, 2
        ) AS conversion_share_pct,
        ROUND(
            s.total_spend / NULLIF(s.attributed_conversions, 0), 2
        ) AS effective_cac,
        ROUND(
            s.total_spend / NULLIF(t.model_total_spend, 0) * 100, 2
        ) AS spend_share_pct,
        ROUND(
            (s.attributed_conversions / NULLIF(t.model_total_conversions, 0) * 100)
            - (s.total_spend / NULLIF(t.model_total_spend, 0) * 100), 2
        ) AS reallocation_signal
    FROM channel_model_summary s
    INNER JOIN model_totals t
        ON s.attribution_model = t.attribution_model
)

SELECT * FROM final
ORDER BY attribution_model, conversion_share_pct DESC