
    
    

select
    click_id as unique_field,
    count(*) as n_records

from "booking_analytics"."raw"."ad_clicks"
where click_id is not null
group by click_id
having count(*) > 1


