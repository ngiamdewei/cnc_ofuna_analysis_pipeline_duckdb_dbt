

select
  -- keep the clean staging columns (downstream expects these)
  monitor_machineno,
  "TimeStamp_SGT",
  "TimeStamp_UTC",
  "Quality" as status,
  "Value" as value,
  "PointName" as pointname,
  "OpcTag",
  "OpcTag" as opctag,
  monitor_diameter,
  monitor_elapsed,
  monitor_phdt,
  monitor_progress,
  "SpindleFieldRaw",
  monitor_spindle,
  machine_state_ofuna,
  alarm_content,
  alarm_alarmcode,
  "StatusErrorCode" as StatusErrorCode,
  monitor_program,
  machinestate,
  monitor_toolno,
  monitor_nu,

  -- add “raw-contract” aliases (your stage tests expect these)
  monitor_machineno::int    as machine_no,
  "TimeStamp_SGT"           as timestamp_sgt,
  "TimeStamp_UTC"           as timestamp_utc,
--   "PointName"               as pointname,
  now()::timestamp          as ingested_at


from "cnc_ofuna_dev"."raw"."vw_OFUNA_84_TO_91_STAGING_CLEAN_duckdb"

