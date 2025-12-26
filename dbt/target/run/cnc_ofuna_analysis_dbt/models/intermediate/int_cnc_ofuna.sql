
  
    

  create  table "cnc_ofuna_dev"."raw"."int_cnc_ofuna__dbt_tmp"
  
  
    as
  
  (
    with b as (
  select
    lpad(machine_no::text, 3, '0')                   as monitor_machineno,
    timestamp_sgt                                    as "TimeStamp_SGT",
    timestamp_utc                                    as "TimeStamp_UTC",
    status                                           as "Quality",
    value                                            as "Value",
    pointname                                        as "PointName",
    regexp_replace(pointname, '^.*\.', '')           as "OpcTag"
  from "cnc_ofuna_dev"."raw"."stage_raw_cnc_ofuna"
),

base_calc as (
  select
    b.*,

    -- Diameter (TRY_CONVERT(float, Value)/1000)
    case
      when "OpcTag" = 'Diameter' and "Value" ~ '^-?\d+(\.\d+)?$'
      then ("Value"::double precision) / 1000.0
    end as monitor_diameter,

    -- AR runtime from Status -> monitor_elapsed (seconds)
    case
      when "OpcTag" = 'Status'
       and position('AR' in "Value") > 0
       and substring("Value" from position('AR' in "Value") + 2 for 8) ~ '^\d{2}:\d{2}:\d{2}$'
      then extract(epoch from (
        (substring("Value" from position('AR' in "Value") + 2 for 8)::time)
        - time '00:00:00'
      ))::int
    end as monitor_elapsed,

    -- AH hit counter -> monitor_phdt
    case
      when "OpcTag" = 'Status'
       and position('AH' in "Value") > 0
      then nullif(ltrim(substring("Value" from position('AH' in "Value") + 2 for 7)), '')::int
    end as monitor_phdt,

    -- AP progress -> monitor_progress
    case
      when "OpcTag" = 'Status'
       and position('AP' in "Value") > 0
      then nullif(ltrim(substring("Value" from position('AP' in "Value") + 2 for 3)), '')::int
    end as monitor_progress

  from b
),

zs_extract as (
  select
    bc.*,

    -- SpindleFieldRaw: substring ZS... until next comma
    case
      when "OpcTag" = 'Status' and position('ZS' in "Value") > 0
      then (regexp_match("Value", '(ZS[^,]*)'))[1]
    end as "SpindleFieldRaw",

    case
      when "OpcTag" = 'Status' and position('ZS' in "Value") > 0
      then right((regexp_match("Value", '(ZS[^,]*)'))[1], 6)
    end as monitor_spindle,

    -- machine_state_ofuna: token MO...
    case
      when "OpcTag" = 'Status' and position(',MO' in "Value") > 0
      then (regexp_match("Value", ',(MO[^,]*)'))[1]
    end as machine_state_ofuna

  from base_calc bc
),

alarm_parse as (
  select
    z.*,

    -- StatusErrorCode: EC token
    case
      when "OpcTag" = 'Status' and position(',EC' in "Value") > 0
      then (regexp_match("Value", ',(EC[^,]*)'))[1]
    end as "StatusErrorCode",

    -- monitor_program: substring after ",FN"
    case
      when "OpcTag" = 'Status' and position(',FN' in "Value") > 0
      then substring("Value" from position(',FN' in "Value") + 3 for 255)
    end as monitor_program,

    -- machinestate
    case
      when "OpcTag" = 'MachineState' and "Value" ~ '^\s*\d+\s*$'
      then "Value"::int
    end as machinestate,

    -- tool no from Tools tag (T...)
    case
      when "OpcTag" = 'Tools' and position('T' in "Value") > 0
      then nullif((regexp_match("Value", 'T([0-9]+)'))[1], '')::int
    end as monitor_toolno,

    -- NU from Tools tag (NU...)
    case
      when "OpcTag" = 'Tools' and position('NU' in "Value") > 0
      then nullif((regexp_match("Value", 'NU([0-9]+)'))[1], '')::int
    end as monitor_nu

  from zs_extract z
),

alarm_positions as (
  select
    a.*,
    a."Value"::text as vval,
    position(' * ' in a."Value") as star_block,
    position('*' in a."Value") as star_any,
    case
      when position(';' in reverse(a."Value")) > 0
      then length(a."Value") - position(';' in reverse(a."Value")) + 1
      else 0
    end as semi_last_any
  from alarm_parse a
),

alarm_final as (
  select
    p.*,
    case
      when p.star_block > 0 then p.star_block + 1
      when p.star_any > 0 then p.star_any
      else 0
    end as star_idx
  from alarm_positions p
),

alarm_final2 as (
  select
    af.*,
    case
      when af."OpcTag" = 'Error'
       and af.semi_last_any > af.star_idx
      then af.semi_last_any
      else 0
    end as semi_last_pos
  from alarm_final af
)

select
  monitor_machineno,
  "TimeStamp_SGT",
  "TimeStamp_UTC",
  "Quality",
  "Value",
  "PointName",
  "OpcTag",
  monitor_diameter,
  monitor_elapsed,
  monitor_phdt,
  monitor_progress,
  "SpindleFieldRaw",
  monitor_spindle,
  machine_state_ofuna,

  -- alarm_content/alarm_alarmcode exactly like MSSQL logic
  case
    when "OpcTag" = 'Error' and semi_last_pos > 0 and star_idx > 0
    then btrim(substring("Value" from (star_idx + 1) for (semi_last_pos - (star_idx + 1))))
  end as alarm_content,

  case
    when "OpcTag" = 'Error' and semi_last_pos > 0
    then nullif(substring("Value" from (semi_last_pos + 1) for 20), '')::int
  end as alarm_alarmcode,

  "StatusErrorCode",
  monitor_program,
  machinestate,
  monitor_toolno,
  monitor_nu
from alarm_final2
  );
  