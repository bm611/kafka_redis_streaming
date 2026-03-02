
      
  
    

  create  table "booking_analytics"."public_marts"."fct_ad_performance"
  
  
    as
  
  (
    

with  __dbt__cte__int_booking_ads_joined as (
with clicks as (
    select * from "booking_analytics"."public_staging"."stg_ad_clicks"
),

impressions as (
    select * from "booking_analytics"."public_staging"."stg_ad_impressions"
),

bookings as (
    select * from "booking_analytics"."public_staging"."stg_bookings"
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
), impressions as (
    select * from "booking_analytics"."public_staging"."stg_ad_impressions"
),

clicks as (
    select * from "booking_analytics"."public_staging"."stg_ad_clicks"
),

joined as (
    select * from __dbt__cte__int_booking_ads_joined
),

-- Aggregate at partner + hotel + placement + day grain
daily_performance as (
    select
        i.partner_id,
        i.partner_name,
        i.hotel_name,
        i.country,
        i.placement,
        date_trunc('day', i.event_timestamp)::date as report_date,

        -- Impressions
        count(distinct i.impression_id) as impressions,

        -- Clicks
        count(distinct c.click_id) as clicks,

        -- CTR
        case
            when count(distinct i.impression_id) > 0
            then round(count(distinct c.click_id)::numeric / count(distinct i.impression_id), 4)
            else 0
        end as ctr,

        -- Spend
        sum(i.bid_amount_usd) as spend,

        -- Bookings from clicks
        count(distinct case when c.led_to_booking then c.booking_id end) as bookings,

        -- Revenue from attributed bookings
        coalesce(sum(case when j.led_to_booking then j.booking_revenue end), 0) as revenue,

        -- ROAS (Return on Ad Spend)
        case
            when sum(i.bid_amount_usd) > 0
            then round(
                coalesce(sum(case when j.led_to_booking then j.booking_revenue end), 0)::numeric
                / sum(i.bid_amount_usd), 2
            )
            else 0
        end as roas

    from impressions i
    left join clicks c on c.impression_id = i.impression_id
    left join joined j on j.click_id = c.click_id

    

    group by
        i.partner_id,
        i.partner_name,
        i.hotel_name,
        i.country,
        i.placement,
        date_trunc('day', i.event_timestamp)::date
)

select
    md5(cast(coalesce(cast(partner_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(hotel_name as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(placement as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(report_date as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as performance_key,
    *
from daily_performance
  );
  
  