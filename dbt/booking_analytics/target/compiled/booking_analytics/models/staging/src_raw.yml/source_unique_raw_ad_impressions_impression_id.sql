
    
    

select
    impression_id as unique_field,
    count(*) as n_records

from "booking_analytics"."raw"."ad_impressions"
where impression_id is not null
group by impression_id
having count(*) > 1


