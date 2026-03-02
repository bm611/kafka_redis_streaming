
    
    

select
    booking_id as unique_field,
    count(*) as n_records

from "booking_analytics"."public_staging"."stg_bookings"
where booking_id is not null
group by booking_id
having count(*) > 1


