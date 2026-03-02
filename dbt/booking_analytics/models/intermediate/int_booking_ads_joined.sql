with clicks as (
    select * from {{ ref('stg_ad_clicks') }}
),

impressions as (
    select * from {{ ref('stg_ad_impressions') }}
),

bookings as (
    select * from {{ ref('stg_bookings') }}
),

clicks_enriched as (
    select
        c.click_id,
        c.impression_id,
        c.event_timestamp as click_timestamp,
        c.user_id,
        c.booking_id,
        c.led_to_booking,
        i.partner_id,
        i.partner_name,
        i.placement,
        i.hotel_name,
        i.country,
        i.bid_amount_usd,
        i.event_timestamp as impression_timestamp
    from clicks c
    inner join impressions i on c.impression_id = i.impression_id
),

with_booking_data as (
    select
        ce.*,
        b.total_price_usd as booking_revenue,
        b.room_type,
        b.payment_method
    from clicks_enriched ce
    left join bookings b on ce.booking_id = b.booking_id
)

select * from with_booking_data
