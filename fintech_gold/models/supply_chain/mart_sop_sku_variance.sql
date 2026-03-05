WITH inventory_health AS (
    SELECT
        product_id,
        product_name,
        category,
        supplier,
        stock_level,
        reorder_point,
        reorder_quantity,
        lead_time_days,
        unit_cost,
        inventory_value,
        days_of_supply,
        reorder_trigger,
        obsolescence_risk,
        stock_status
    FROM {{ ref('fct_inventory_health') }}
),

stockout_warnings AS (
    SELECT
        product_id,
        warning_level,
        days_until_stockout,
        recommended_order_qty,
        recommended_action
    FROM {{ ref('fct_stockout_early_warning') }}
),

forecast_demand AS (
    SELECT
        ROUND(AVG(predicted_applications), 2) AS avg_daily_forecast,
        ROUND(SUM(predicted_applications), 2) AS total_forecast_30d
    FROM default.srv_loan_forecast
    WHERE forecast_date >= CURRENT_DATE()
      AND forecast_date <  CURRENT_DATE() + INTERVAL 30 DAYS
),

sku_variance AS (
    SELECT
        i.product_id,
        i.product_name,
        i.category,
        i.supplier,
        i.stock_level,
        i.reorder_point,
        i.reorder_quantity,
        i.lead_time_days,
        i.unit_cost,
        i.inventory_value,
        i.days_of_supply,
        i.reorder_trigger,
        i.obsolescence_risk,
        i.stock_status,
        s.warning_level,
        s.days_until_stockout,
        s.recommended_order_qty,
        s.recommended_action,
        f.avg_daily_forecast,
        f.total_forecast_30d,
        ROUND(
            i.stock_level - (f.avg_daily_forecast * i.lead_time_days), 2
        ) AS supply_demand_variance,
        CASE
            WHEN i.stock_level - (f.avg_daily_forecast * i.lead_time_days) < 0
            THEN 'DEFICIT'
            WHEN i.stock_level - (f.avg_daily_forecast * i.lead_time_days) < i.reorder_point
            THEN 'TIGHT'
            ELSE 'SURPLUS'
        END AS variance_status,
        ROUND(
            i.stock_level / NULLIF(f.avg_daily_forecast, 0), 1
        ) AS forecast_days_of_supply
    FROM inventory_health i
    LEFT JOIN stockout_warnings s
        ON i.product_id = s.product_id
    CROSS JOIN forecast_demand f
)

SELECT * FROM sku_variance
ORDER BY
    CASE warning_level
        WHEN 'STOCKOUT' THEN 1
        WHEN 'CRITICAL' THEN 2
        WHEN 'AT_RISK'  THEN 3
        WHEN 'WARNING'  THEN 4
        ELSE                 5
    END,
    supply_demand_variance ASC