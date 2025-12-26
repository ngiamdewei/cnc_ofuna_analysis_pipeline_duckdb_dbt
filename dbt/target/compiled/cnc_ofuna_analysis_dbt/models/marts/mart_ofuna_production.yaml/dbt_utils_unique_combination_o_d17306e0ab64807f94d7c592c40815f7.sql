





with validation_errors as (

    select
        production_machineno, production_starttime
    from "warehouse"."public"."mart_ofuna_production"
    group by production_machineno, production_starttime
    having count(*) > 1

)

select *
from validation_errors


