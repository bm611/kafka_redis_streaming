with bookings as (
    select * from {{ ref('stg_bookings') }}
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
    {{ dbt_utils.generate_surrogate_key(['hotel_name']) }} as hotel_id,
    *
from hotel_stats
