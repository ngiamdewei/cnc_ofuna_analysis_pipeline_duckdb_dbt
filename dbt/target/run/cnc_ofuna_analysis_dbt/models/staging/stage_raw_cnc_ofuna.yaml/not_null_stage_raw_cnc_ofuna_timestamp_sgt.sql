
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select timestamp_sgt
from "cnc_ofuna_dev"."raw"."stage_raw_cnc_ofuna"
where timestamp_sgt is null



  
  
      
    ) dbt_internal_test