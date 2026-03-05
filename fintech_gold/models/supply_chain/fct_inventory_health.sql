WITH inventory AS (
    SELECT
        product_id,
        product_name,
        category,
        unit_cost,
        selling_price,
        stock_level,
        reorder_point,
        reorder_quantity,
        lead_time_days,
        supplier,
        CURRENT_DATE() AS snapshot_date
    FROM {{ ref('stg_product_inventory') }}
    WHERE is_duplicate = false
),

final AS (
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
        selling_price,
        ROUND(stock_level * unit_cost, 2)                    AS inventory_value,
        ROUND(stock_level / NULLIF(reorder_quantity, 0), 1)  AS days_of_supply,
        CASE
            WHEN stock_level <= reorder_point THEN 1
            ELSE 0
        END                                                   AS reorder_trigger,
        CASE
            WHEN ROUND(stock_level / NULLIF(reorder_quantity, 0), 1) > 180 THEN 'high'
            WHEN ROUND(stock_level / NULLIF(reorder_quantity, 0), 1) > 90  THEN 'medium'
            ELSE 'low'
        END                                                   AS obsolescence_risk,
        CASE
            WHEN stock_level <= 0                                           THEN 'out_of_stock'
            WHEN stock_level <= reorder_point                               THEN 'critical'
            WHEN ROUND(stock_level / NULLIF(reorder_quantity, 0), 1) <= 30 THEN 'low'
            WHEN ROUND(stock_level / NULLIF(reorder_quantity, 0), 1) <= 60 THEN 'moderate'
            ELSE 'healthy'
        END                                                   AS stock_status
    FROM inventory
)

SELECT * FROM final
ORDER BY days_of_supply ASC