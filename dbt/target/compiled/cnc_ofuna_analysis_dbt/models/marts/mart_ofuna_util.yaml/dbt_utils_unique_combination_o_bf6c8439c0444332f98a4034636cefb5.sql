





with validation_errors as (

    select
        utilization_machineno, utilization_day, utilization_time
    from "warehouse"."public"."mart_ofuna_util"
    group by utilization_machineno, utilization_day, utilization_time
    having count(*) > 1

)

select *
from validation_errors


