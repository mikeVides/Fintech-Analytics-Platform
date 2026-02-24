-- models/marts/mart_inventory_health.sql
-- Inventory health mart
-- Pre-aggregated inventory metrics by category and supplier
-- Designed for operations dashboards in Tableau

with products as (
    select * from {{ ref('dim_products') }}
),

final as (
    select
        -- Dimensions
        category,
        supplier,

        -- Volume metrics
        count(product_id)                            as total_products,
        sum(stock_level)                             as total_stock_units,

        -- Stock health metrics
        sum(case when stock_status = 'Out of Stock'
            then 1 else 0 end)                       as out_of_stock_count,
        sum(case when stock_status = 'Low Stock'
            then 1 else 0 end)                       as low_stock_count,
        sum(case when stock_status = 'In Stock'
            then 1 else 0 end)                       as in_stock_count,

        round(
            sum(case when stock_status = 'Out of Stock'
                then 1 else 0 end)
            / nullif(count(product_id), 0) * 100
        , 2)                                         as out_of_stock_rate_pct,

        -- Financial metrics
        round(sum(unit_cost * stock_level), 2)       as total_inventory_cost,
        round(sum(selling_price * stock_level), 2)   as total_inventory_value,
        round(avg(margin_pct), 2)                    as avg_margin_pct,
        round(avg(gross_margin), 2)                  as avg_gross_margin,

        -- Reorder metrics
        sum(case when stock_level <= reorder_point
            then reorder_quantity else 0 end)        as total_units_to_reorder,
        round(
            sum(case when stock_level <= reorder_point
                then reorder_quantity * unit_cost
                else 0 end)
        , 2)                                         as estimated_reorder_cost,

        -- Lead time
        round(avg(lead_time_days), 1)                as avg_lead_time_days,
        max(lead_time_days)                          as max_lead_time_days

    from products

    group by
        category,
        supplier
)

select * from final