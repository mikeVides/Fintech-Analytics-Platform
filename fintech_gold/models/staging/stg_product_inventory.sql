-- models/staging/stg_product_inventory.sql
-- Staging view on top of Silver product_inventory table

select
    product_id,
    sku,
    product_name,
    category,
    unit_cost,
    selling_price,
    stock_level,
    reorder_point,
    reorder_quantity,
    lead_time_days,
    supplier,
    is_active,
    is_unit_cost_outlier,
    is_selling_price_outlier,
    is_duplicate,
    data_quality_score
from workspace.default.silver_product_inventory