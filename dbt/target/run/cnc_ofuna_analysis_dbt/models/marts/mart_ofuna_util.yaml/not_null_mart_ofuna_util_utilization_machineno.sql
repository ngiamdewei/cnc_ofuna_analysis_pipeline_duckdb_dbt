
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select utilization_machineno
from "cnc_ofuna_dev"."raw"."mart_ofuna_util"
where utilization_machineno is null



  
  
      
    ) dbt_internal_test