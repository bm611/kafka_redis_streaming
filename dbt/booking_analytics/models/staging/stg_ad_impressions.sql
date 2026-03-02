with source as (
    select * from {{ source('raw', 'ad_impressions') }}
),

cleaned as (
    select
        impression_id,
        event_timestamp,
        partner_id,
        trim(partner_name) as partner_name,
        lower(trim(placement)) as placement,
        trim(hotel_name) as hotel_name,
        trim(country) as country,
        bid_amount_usd,
        loaded_at
    from source
    where impression_id is not null
      and bid_amount_usd >= 0
)

select * from cleaned
