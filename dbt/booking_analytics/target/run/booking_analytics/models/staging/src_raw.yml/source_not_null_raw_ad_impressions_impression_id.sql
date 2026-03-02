
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select impression_id
from "booking_analytics"."raw"."ad_impressions"
where impression_id is null



  
  
      
    ) dbt_internal_test