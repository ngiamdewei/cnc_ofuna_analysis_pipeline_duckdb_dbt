
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select monitor_timelog
from "cnc_ofuna_dev"."dbt_dev"."mart_ofuna_monitor_snapshot"
where monitor_timelog is null



  
  
      
    ) dbt_internal_test