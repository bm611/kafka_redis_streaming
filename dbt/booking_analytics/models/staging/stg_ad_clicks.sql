with source as (
    select * from {{ source('raw', 'ad_clicks') }}
),

cleaned as (
    select
        click_id,
        impression_id,
        event_timestamp,
        user_id,
        booking_id,
        case when booking_id is not null then true else false end as led_to_booking,
        loaded_at
    from source
    where click_id is not null
)

select * from cleaned
