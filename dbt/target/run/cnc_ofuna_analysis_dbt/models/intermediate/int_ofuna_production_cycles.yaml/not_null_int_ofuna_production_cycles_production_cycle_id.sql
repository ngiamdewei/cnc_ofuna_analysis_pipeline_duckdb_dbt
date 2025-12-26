
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select production_cycle_id
from "cnc_ofuna_dev"."dbt_dev"."int_ofuna_production_cycles"
where production_cycle_id is null



  
  
      
    ) dbt_internal_test