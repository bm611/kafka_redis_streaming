with impressions as (
    select * from {{ ref('stg_ad_impressions') }}
),

partner_stats as (
    select
        partner_id,
        partner_name,
        count(*) as total_impressions,
        sum(bid_amount_usd) as total_spend,
        round(avg(bid_amount_usd), 4) as avg_bid,
        count(distinct placement) as placements_used,
        count(distinct hotel_name) as hotels_advertised,
        min(event_timestamp) as first_impression_at,
        max(event_timestamp) as last_impression_at
    from impressions
    group by partner_id, partner_name
)

select * from partner_stats
