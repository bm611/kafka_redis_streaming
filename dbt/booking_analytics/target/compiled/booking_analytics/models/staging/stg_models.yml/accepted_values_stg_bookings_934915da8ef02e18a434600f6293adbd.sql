
    
    

with all_values as (

    select
        payment_method as value_field,
        count(*) as n_records

    from "booking_analytics"."public_staging"."stg_bookings"
    group by payment_method

)

select *
from all_values
where value_field not in (
    'credit_card','paypal','debit_card'
)


