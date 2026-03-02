
    
    

with all_values as (

    select
        room_type as value_field,
        count(*) as n_records

    from "booking_analytics"."public_staging"."stg_bookings"
    group by room_type

)

select *
from all_values
where value_field not in (
    'Standard','Deluxe','Suite'
)


