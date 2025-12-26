
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        monitor_machineno, TimeStamp_SGT, alarm_code
    from "cnc_ofuna_dev"."dbt_dev"."mart_ofuna_alarm"
    group by monitor_machineno, TimeStamp_SGT, alarm_code
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test