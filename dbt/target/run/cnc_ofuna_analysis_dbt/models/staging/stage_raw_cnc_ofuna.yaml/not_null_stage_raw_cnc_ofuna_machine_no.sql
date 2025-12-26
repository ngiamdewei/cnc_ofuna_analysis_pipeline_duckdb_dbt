
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select machine_no
from "cnc_ofuna_dev"."raw"."stage_raw_cnc_ofuna"
where machine_no is null



  
  
      
    ) dbt_internal_test