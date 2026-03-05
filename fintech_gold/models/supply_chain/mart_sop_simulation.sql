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

product_demand AS (
    SELECT
        product_type                       AS category,
        COUNT(transaction_id)              AS total_transactions,
        ROUND(SUM(transaction_amount), 2)  AS total_revenue,
        ROUND(AVG(transaction_amount), 2)  AS avg_transaction_value,
        COUNT(DISTINCT customer_id)        AS unique_customers
    FROM {{ ref('fct_transactions') }}
    WHERE is_failed = 0
    GROUP BY product_type
),

product_full AS (
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
        COALESCE(d.total_transactions, 0)   AS total_transactions,
        COALESCE(d.total_revenue, 0)        AS total_revenue,
        COALESCE(d.avg_transaction_value, 0) AS avg_transaction_value,
        COALESCE(d.unique_customers, 0)     AS unique_customers
    FROM inventory_health i
    LEFT JOIN stockout_warnings s ON i.product_id = s.product_id
    LEFT JOIN product_demand d    ON i.category = d.category
),

category_sop AS (
    SELECT
        category,
        COUNT(product_id)                                    AS total_skus,
        SUM(stock_level)                                     AS total_units_on_hand,
        ROUND(SUM(inventory_value), 2)                       AS total_inventory_value,
        ROUND(AVG(days_of_supply), 1)                        AS avg_days_of_supply,
        SUM(CASE WHEN stock_status = 'out_of_stock' THEN 1 ELSE 0 END) AS skus_out_of_stock,
        SUM(CASE WHEN stock_status = 'critical'     THEN 1 ELSE 0 END) AS skus_critical,
        SUM(CASE WHEN stock_status = 'low'          THEN 1 ELSE 0 END) AS skus_low,
        SUM(CASE WHEN stock_status = 'moderate'     THEN 1 ELSE 0 END) AS skus_moderate,
        SUM(CASE WHEN stock_status = 'healthy'      THEN 1 ELSE 0 END) AS skus_healthy,
        SUM(CASE WHEN reorder_trigger = 1           THEN 1 ELSE 0 END) AS skus_needing_reorder,
        SUM(CASE WHEN obsolescence_risk = 'high'    THEN 1 ELSE 0 END) AS skus_high_obsolescence,
        ROUND(SUM(total_revenue), 2)                         AS total_category_revenue,
        ROUND(AVG(avg_transaction_value), 2)                 AS avg_transaction_value,
        SUM(total_transactions)                              AS total_transactions,
        ROUND(
            SUM(CASE WHEN stock_status IN ('out_of_stock','critical') THEN 1 ELSE 0 END)
            / NULLIF(COUNT(product_id), 0) * 100
        , 1)                                                 AS pct_skus_at_risk
    FROM product_full
    GROUP BY category
),

final AS (
    SELECT
        category,
        total_skus,
        total_units_on_hand,
        total_inventory_value,
        avg_days_of_supply,
        skus_out_of_stock,
        skus_critical,
        skus_low,
        skus_moderate,
        skus_healthy,
        skus_needing_reorder,
        skus_high_obsolescence,
        total_category_revenue,
        avg_transaction_value,
        total_transactions,
        pct_skus_at_risk,
        CASE
            WHEN pct_skus_at_risk >= 50 THEN 'HIGH RISK'
            WHEN pct_skus_at_risk >= 25 THEN 'MEDIUM RISK'
            ELSE 'LOW RISK'
        END AS category_risk_level,
        CASE
            WHEN pct_skus_at_risk >= 50 THEN 'Immediate procurement review required'
            WHEN pct_skus_at_risk >= 25 THEN 'Schedule procurement review this week'
            ELSE 'No immediate action required'
        END AS sop_recommendation
    FROM category_sop
)

SELECT * FROM final
ORDER BY pct_skus_at_risk DESC