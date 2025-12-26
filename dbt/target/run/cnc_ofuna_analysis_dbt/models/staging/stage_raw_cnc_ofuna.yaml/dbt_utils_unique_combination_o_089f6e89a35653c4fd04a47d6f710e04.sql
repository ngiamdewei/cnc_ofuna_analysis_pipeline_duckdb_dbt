
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        machine_no, timestamp_utc, pointname
    from "cnc_ofuna_dev"."dbt_dev"."stage_raw_cnc_ofuna"
    group by machine_no, timestamp_utc, pointname
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test