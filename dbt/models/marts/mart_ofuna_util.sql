with base as (
    select
        monitor_machineno,
        timestamp_sgt::timestamp  as ts_sgt,
        timestamp_utc             as ts_utc,
        machine_state_ofuna,
        monitor_nu
    from {{ ref('stage_raw_cnc_ofuna') }}
),


-- 1) MachineState events (expect val like 'MOWORK','MOWAIT','MOIDLE','MOSTOP','MOALAM')
state_events as (
    select
        monitor_machineno,
        ts_sgt,
        machine_state_ofuna
    from base
    where machine_state_ofuna is not null
),

-- 2) Convert to segments using LEAD()
segments as (
    select
        monitor_machineno,
        ts_sgt as segment_start_sgt,
        lead(ts_sgt) over (partition by monitor_machineno order by ts_sgt) as segment_end_sgt,
        machine_state_ofuna
    from state_events
),

-- 3) Clamp segments to same-day (and clamp open-ended segment to now or midnight)
segments_day as (
    select
        monitor_machineno,
        segment_start_sgt,
        case
            when segment_end_sgt is null then
                case
                    when segment_start_sgt::date = (now() at time zone 'Asia/Singapore')::date
                        then (now() at time zone 'Asia/Singapore')::timestamp
                    else date_trunc('day', segment_start_sgt) + interval '1 day'
                end
            when segment_start_sgt::date = segment_end_sgt::date then segment_end_sgt
            else date_trunc('day', segment_start_sgt) + interval '1 day'
        end as segment_end_sgt_eff,
        case
            when machine_state_ofuna = 'MOWORK' then 'WORK'
            when machine_state_ofuna in ('MOWAIT','MOIDLE') then 'WAIT'
            when machine_state_ofuna in ('MOSTOP','MOALAM') then 'STOP'
            else 'OTHER'
        end as segment_state
    from segments
    where segment_start_sgt is not null
),

-- 4) Build (day, machine) grid
days_machines as (
    select distinct
        segment_start_sgt::date as utilization_day,
        monitor_machineno       as utilization_machineno
    from segments_day
),

-- 5) Create 48 half-hour buckets per day
nums as (
    select generate_series(0,47) as n
),

buckets as (
    select
        dm.utilization_day,
        (dm.utilization_day::timestamp + (n.n * interval '30 minutes'))::time as utilization_time,
        dm.utilization_machineno,
        (dm.utilization_day::timestamp + (n.n * interval '30 minutes'))       as bucket_start,
        (dm.utilization_day::timestamp + ((n.n+1) * interval '30 minutes'))   as bucket_end
    from days_machines dm
    cross join nums n
),

-- 6) Compute overlaps (seconds) between segments and buckets
seg_overlaps as (
    select
        b.utilization_day,
        b.utilization_time,
        b.utilization_machineno,
        s.segment_state,
        case
            when s.segment_start_sgt < b.bucket_end
             and s.segment_end_sgt_eff > b.bucket_start
            then extract(epoch from (
                    least(s.segment_end_sgt_eff, b.bucket_end)
                  - greatest(s.segment_start_sgt, b.bucket_start)
                 ))::int
            else 0
        end as overlap_s
    from buckets b
    left join segments_day s
      on s.monitor_machineno = b.utilization_machineno
     and s.segment_start_sgt::date = b.utilization_day
),

agg_state as (
    select
        utilization_day,
        utilization_time,
        utilization_machineno,
        sum(case when segment_state in ('WORK','WAIT','STOP') then overlap_s else 0 end)::int as poweron_s,
        sum(case when segment_state = 'WORK' then overlap_s else 0 end)::int               as working_s,
        sum(case when segment_state = 'WAIT' then overlap_s else 0 end)::int               as waiting_s,
        sum(case when segment_state = 'STOP' then overlap_s else 0 end)::int               as stopping_s
    from seg_overlaps
    group by 1,2,3
),

-- 7) Optional: NU deltas -> hitscount per half-hour
nu_events as (
    select
        monitor_machineno,
        ts_sgt,
        monitor_nu::bigint as nu
    from base
    where monitor_nu is not null
),

nu_deltas as (
    select
        monitor_machineno,
        ts_sgt,
        nu,
        lag(nu) over (partition by monitor_machineno order by ts_sgt) as prev_nu
    from nu_events
),

nu_bucket as (
    select
        ts_sgt::date as utilization_day,
        (date_trunc('hour', ts_sgt) + (extract(minute from ts_sgt)::int / 30) * interval '30 minutes')::time as utilization_time,
        monitor_machineno as utilization_machineno,
        sum(
            case
                when prev_nu is null then 0
                when nu >= prev_nu then (nu - prev_nu)
                else 0
            end
        )::int as hits_nu
    from nu_deltas
    where nu is not null
    group by 1,2,3
)

select
    -- =========================================================
    -- BASE 9 cols FIRST (Power BI “SELECT *” stable)
    -- =========================================================
    a.utilization_day::date                  as utilization_day,
    a.utilization_time::time                 as utilization_time,
    a.utilization_machineno::varchar(20)     as utilization_machineno,
    a.poweron_s::int                         as utilization_poweron,
    a.working_s::int                         as utilization_working,
    a.waiting_s::int                         as utilization_waiting,
    a.stopping_s::int                        as utilization_stopping,
    (6 * a.working_s)::int                   as utilization_spindle,
    coalesce(n.hits_nu, 0)::int              as utilization_hitscount,

    -- =========================================================
    -- EXTRAS (safe at end)
    -- =========================================================
    (a.utilization_day + a.utilization_time)::timestamp                     as utilization_datetime,
    (1800 - a.poweron_s)::int                as utilization_poweroff,
    to_char(interval '1 second' * (1800 - a.poweron_s), 'HH24:MI:SS')        as poweroff_time,
    to_char(interval '1 second' * a.poweron_s,           'HH24:MI:SS')       as poweron_time,
    to_char(interval '1 second' * a.stopping_s,          'HH24:MI:SS')       as stopping_time,
    to_char(interval '1 second' * a.working_s,           'HH24:MI:SS')       as working_time,
    to_char(interval '1 second' * a.waiting_s,           'HH24:MI:SS')       as waiting_time
from agg_state a
left join nu_bucket n
  on n.utilization_day = a.utilization_day
 and n.utilization_time = a.utilization_time
 and n.utilization_machineno = a.utilization_machineno
