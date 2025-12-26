
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select production_day
from "cnc_ofuna_dev"."raw"."mart_ofuna_production"
where production_day is null



  
  
      
    ) dbt_internal_test