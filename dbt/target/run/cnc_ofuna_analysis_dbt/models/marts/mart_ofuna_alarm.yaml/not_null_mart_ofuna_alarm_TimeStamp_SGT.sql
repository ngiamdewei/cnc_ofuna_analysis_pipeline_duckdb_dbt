
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select TimeStamp_SGT
from "cnc_ofuna_dev"."dbt_dev"."mart_ofuna_alarm"
where TimeStamp_SGT is null



  
  
      
    ) dbt_internal_test