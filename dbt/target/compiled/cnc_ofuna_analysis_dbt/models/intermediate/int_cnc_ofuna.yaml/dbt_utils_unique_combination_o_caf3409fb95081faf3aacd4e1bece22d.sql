





with validation_errors as (

    select
        monitor_machineno, TimeStamp_SGT
    from "cnc_ofuna_dev"."dbt_dev"."int_cnc_ofuna"
    group by monitor_machineno, TimeStamp_SGT
    having count(*) > 1

)

select *
from validation_errors


