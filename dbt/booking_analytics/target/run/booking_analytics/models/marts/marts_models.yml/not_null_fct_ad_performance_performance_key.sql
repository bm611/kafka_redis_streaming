
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select performance_key
from "booking_analytics"."public_marts"."fct_ad_performance"
where performance_key is null



  
  
      
    ) dbt_internal_test