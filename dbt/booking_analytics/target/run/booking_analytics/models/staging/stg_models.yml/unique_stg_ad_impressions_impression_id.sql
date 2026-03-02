
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    impression_id as unique_field,
    count(*) as n_records

from "booking_analytics"."public_staging"."stg_ad_impressions"
where impression_id is not null
group by impression_id
having count(*) > 1



  
  
      
    ) dbt_internal_test