with progress_ordered as (
    select
        monitor_machineno,
        "TimeStamp_SGT" as timestamp_sgt,
        "TimeStamp_UTC" as timestamp_utc,
        monitor_program,
        monitor_progress,
        lag(monitor_progress) over (
            partition by monitor_machineno
            order by timestamp_utc
        ) as prev_progress
    from {{ ref('stage_raw_cnc_ofuna') }}
    where monitor_progress is not null
),

cycle_markers as (
    select
        monitor_machineno,
        timestamp_sgt,
        timestamp_utc,
        monitor_program,
        monitor_progress,
        case
            when monitor_progress > 0 and (prev_progress is null or prev_progress = 0) then 1
            else 0
        end as is_cycle_start
    from progress_ordered
),

cycle_ids as (
    select
        monitor_machineno,
        timestamp_sgt,
        timestamp_utc,
        monitor_program,
        monitor_progress,
        sum(is_cycle_start) over (
            partition by monitor_machineno
            order by timestamp_utc
            rows unbounded preceding
        ) as cycle_seq
    from cycle_markers
),

cycle_ranges as (
    select
        monitor_machineno,
        cycle_seq,
        min(timestamp_sgt) as cycle_start_sgt,
        max(monitor_program) as monitor_program
    from cycle_ids
    where cycle_seq > 0
    group by monitor_machineno, cycle_seq
),

cycle_with_end as (
    select
        c.monitor_machineno,
        c.cycle_seq,
        c.monitor_program,
        c.cycle_start_sgt,
        lead(c.cycle_start_sgt) over (
            partition by c.monitor_machineno
            order by c.cycle_start_sgt
        ) as cycle_end_sgt
    from cycle_ranges c
),

final as (
    select
        (cycle_start_sgt::date) as production_day,
        monitor_machineno,
        monitor_program,
        cycle_start_sgt,
        cycle_end_sgt,
        extract(epoch from (cycle_end_sgt - cycle_start_sgt))::int as cycle_duration_s,
        round((extract(epoch from (cycle_end_sgt - cycle_start_sgt)) / 60.0)::numeric, 2) as cycle_duration_mins,
        round((extract(epoch from (cycle_end_sgt - cycle_start_sgt)) / 3600.0)::numeric, 2) as cycle_duration_hrs,
        (cycle_end_sgt is not null) as production_hitscountfinish,
        (cycle_end_sgt is not null) as production_endtimefinish,
        cycle_seq as production_cycle_id
    from cycle_with_end
)

select *
from final
where (cycle_end_sgt is null or cycle_duration_s >= 0)
