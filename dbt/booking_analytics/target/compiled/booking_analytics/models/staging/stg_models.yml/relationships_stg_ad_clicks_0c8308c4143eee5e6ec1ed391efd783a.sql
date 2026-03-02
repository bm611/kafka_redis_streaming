
    
    

with child as (
    select impression_id as from_field
    from "booking_analytics"."public_staging"."stg_ad_clicks"
    where impression_id is not null
),

parent as (
    select impression_id as to_field
    from "booking_analytics"."public_staging"."stg_ad_impressions"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


