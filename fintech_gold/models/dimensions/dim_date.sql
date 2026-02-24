-- models/dimensions/dim_date.sql
-- Date dimension table for the star schema
-- One row per date from 2020-01-01 to 2027-12-31
-- This dimension does not come from any source table
-- it is generated entirely from SQL date logic

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2027-12-31' as date)"
    ) }}
),

final as (
    select
        -- Primary key
        cast(date_day as date)                          as date_id,

        -- Date attributes
        year(date_day)                                  as year,
        quarter(date_day)                               as quarter,
        month(date_day)                                 as month,
        day(date_day)                                   as day,
        dayofweek(date_day)                             as day_of_week,
        dayofyear(date_day)                             as day_of_year,
        weekofyear(date_day)                            as week_of_year,

        -- Named attributes
        date_format(date_day, 'MMMM')                   as month_name,
        date_format(date_day, 'EEEE')                   as day_name,
        date_format(date_day, 'MMM yyyy')               as month_year,
        date_format(date_day, 'yyyy-MM')                as year_month,

        -- Quarter label
        concat('Q', quarter(date_day), ' ',
               year(date_day))                          as quarter_label,

        -- Boolean flags
        case when dayofweek(date_day) in (1, 7)
             then true else false
        end                                             as is_weekend,

        case when dayofweek(date_day) in (1, 7)
             then false else true
        end                                             as is_weekday,

        case when month(date_day) in (12, 1, 2)
             then 'Winter'
             when month(date_day) in (3, 4, 5)
             then 'Spring'
             when month(date_day) in (6, 7, 8)
             then 'Summer'
             else 'Fall'
        end                                             as season

    from date_spine
)

select * from final