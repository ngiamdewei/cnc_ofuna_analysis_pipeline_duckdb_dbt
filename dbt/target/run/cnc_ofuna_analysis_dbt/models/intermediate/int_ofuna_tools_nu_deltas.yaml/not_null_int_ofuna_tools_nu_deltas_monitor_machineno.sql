
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select monitor_machineno
from "cnc_ofuna_dev"."raw"."int_ofuna_tools_nu_deltas"
where monitor_machineno is null



  
  
      
    ) dbt_internal_test