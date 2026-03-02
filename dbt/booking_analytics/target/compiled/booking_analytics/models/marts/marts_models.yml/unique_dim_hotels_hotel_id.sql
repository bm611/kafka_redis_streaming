
    
    

select
    hotel_id as unique_field,
    count(*) as n_records

from "booking_analytics"."public_marts"."dim_hotels"
where hotel_id is not null
group by hotel_id
having count(*) > 1


