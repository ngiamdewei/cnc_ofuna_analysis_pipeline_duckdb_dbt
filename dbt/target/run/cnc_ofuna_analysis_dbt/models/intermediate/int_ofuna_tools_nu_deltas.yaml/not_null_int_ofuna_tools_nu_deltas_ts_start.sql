
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ts_start
from "cnc_ofuna_dev"."raw"."int_ofuna_tools_nu_deltas"
where ts_start is null



  
  
      
    ) dbt_internal_test