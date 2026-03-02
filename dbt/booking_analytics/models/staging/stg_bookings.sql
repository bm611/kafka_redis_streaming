with source as (
    select * from {{ source('raw', 'bookings') }}
),

cleaned as (
    select
        booking_id,
        event_timestamp,
        user_id,
        trim(hotel_name) as hotel_name,
        room_type,
        check_in_date,
        check_out_date,
        (check_out_date - check_in_date) as nights,
        total_price_usd,
        lower(trim(payment_method)) as payment_method,
        trim(country) as country,
        loaded_at
    from source
    where booking_id is not null
      and total_price_usd >= 0
)

select * from cleaned
