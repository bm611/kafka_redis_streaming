
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_price_usd
from "booking_analytics"."raw"."bookings"
where total_price_usd is null



  
  
      
    ) dbt_internal_test