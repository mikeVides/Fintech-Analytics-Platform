WITH inventory_health AS (
    SELECT
        product_id,
        product_name,
        category,
        supplier,
        snapshot_date,
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

stockout_flags AS (
    SELECT
        product_id,
        product_name,
        category,
        supplier,
        snapshot_date,
        stock_level,
        reorder_point,
        reorder_quantity,
        lead_time_days,
        unit_cost,
        inventory_value,
        days_of_supply,
        reorder_trigger,
        obsolescence_risk,
        stock_status,
        CASE
            WHEN stock_status = 'out_of_stock'            THEN 'STOCKOUT'
            WHEN stock_status = 'critical'                THEN 'CRITICAL'
            WHEN days_of_supply <= lead_time_days         THEN 'AT_RISK'
            WHEN days_of_supply <= lead_time_days * 1.5   THEN 'WARNING'
            ELSE                                               'OK'
        END AS warning_level,
        CASE
            WHEN stock_status = 'out_of_stock' THEN 0
            WHEN days_of_supply <= lead_time_days
            THEN CAST(days_of_supply AS INT)
            ELSE NULL
        END AS days_until_stockout,
        CASE
            WHEN stock_level < reorder_point
            THEN reorder_quantity
            WHEN days_of_supply <= lead_time_days * 1.5
            THEN CAST(CEIL(reorder_quantity * 1.25) AS INT)
            ELSE 0
        END AS recommended_order_qty,
        CASE
            WHEN stock_status = 'out_of_stock'          THEN 'Emergency order required immediately'
            WHEN stock_status = 'critical'              THEN 'Place order now — below reorder point'
            WHEN days_of_supply <= lead_time_days       THEN 'Order urgently — stockout within lead time'
            WHEN days_of_supply <= lead_time_days * 1.5 THEN 'Monitor closely — approaching reorder point'
            ELSE                                             'No action required'
        END AS recommended_action
    FROM inventory_health
)

SELECT * FROM stockout_flags
ORDER BY
    CASE warning_level
        WHEN 'STOCKOUT' THEN 1
        WHEN 'CRITICAL' THEN 2
        WHEN 'AT_RISK'  THEN 3
        WHEN 'WARNING'  THEN 4
        ELSE                 5
    END,
    days_of_supply ASC