





with validation_errors as (

    select
        monitor_machineno, monitor_timelog
    from "cnc_ofuna_dev"."dbt_dev"."mart_ofuna_monitor_snapshot"
    group by monitor_machineno, monitor_timelog
    having count(*) > 1

)

select *
from validation_errors


