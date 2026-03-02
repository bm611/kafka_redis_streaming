
    
    

select
    performance_key as unique_field,
    count(*) as n_records

from "booking_analytics"."public_marts"."fct_ad_performance"
where performance_key is not null
group by performance_key
having count(*) > 1


