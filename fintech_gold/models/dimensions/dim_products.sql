-- models/dimensions/dim_product.sql
-- Product dimension table for the star schema
-- One row per product with all descriptive attributes

with source as (
    select * from {{ ref('stg_product_inventory') }}
),

final as (
    select
        -- Primary key
        product_id,

        -- Product attributes
        sku,
        product_name,
        category,

        -- Pricing attributes
        unit_cost,
        selling_price,
        round(selling_price - unit_cost, 2)                          as gross_margin,
        round((selling_price - unit_cost) / selling_price * 100, 2)  as margin_pct,

        -- Inventory attributes
        stock_level,
        reorder_point,
        reorder_quantity,
        lead_time_days,
        supplier,

        -- Derived attributes
        case
            when stock_level = 0              then 'Out of Stock'
            when stock_level <= reorder_point then 'Low Stock'
            else                                   'In Stock'
        end as stock_status,

        case
            when margin_pct >= 50 then 'High Margin'
            when margin_pct >= 30 then 'Medium Margin'
            else                       'Low Margin'
        end as margin_band,

        -- Status
        is_active,

        -- Quality metadata
        data_quality_score

    from source
    where is_active = true
)

select * from final