
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        monitor_machineno, TimeStamp_SGT
    from "cnc_ofuna_dev"."dbt_dev"."int_cnc_ofuna"
    group by monitor_machineno, TimeStamp_SGT
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test