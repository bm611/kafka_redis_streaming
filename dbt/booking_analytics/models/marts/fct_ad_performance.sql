{{
    config(
        materialized='incremental',
        unique_key='performance_key',
        incremental_strategy='merge'
    )
}}

with impressions as (
    select * from {{ ref('stg_ad_impressions') }}
),

clicks as (
    select * from {{ ref('stg_ad_clicks') }}
),

joined as (
    select * from {{ ref('int_booking_ads_joined') }}
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

    {% if is_incremental() %}
    where i.event_timestamp > (select max(report_date) from {{ this }})
    {% endif %}

    group by
        i.partner_id,
        i.partner_name,
        i.hotel_name,
        i.country,
        i.placement,
        date_trunc('day', i.event_timestamp)::date
)

select
    {{ dbt_utils.generate_surrogate_key([
        'partner_id', 'hotel_name', 'placement', 'report_date'
    ]) }} as performance_key,
    *
from daily_performance
