
  
    

  create  table "booking_analytics"."public_marts"."dim_hotels__dbt_tmp"
  
  
    as
  
  (
    with bookings as (
    select * from "booking_analytics"."public_staging"."stg_bookings"
),

hotel_stats as (
    select
        hotel_name,
        country,
        count(*) as total_bookings,
        sum(total_price_usd) as total_revenue,
        round(avg(total_price_usd), 2) as avg_booking_value,
        round(avg(nights), 1) as avg_nights,
        count(distinct user_id) as unique_guests,
        min(event_timestamp) as first_booking_at,
        max(event_timestamp) as last_booking_at
    from bookings
    group by hotel_name, country
)

select
    md5(cast(coalesce(cast(hotel_name as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as hotel_id,
    *
from hotel_stats
  );
  