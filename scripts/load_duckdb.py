import duckdb
from pathlib import Path

DB  = Path(__file__).resolve().parents[1] / "local" / "warehouse.duckdb"
CSV = Path(__file__).resolve().parents[1] / "data" / "vw_OFUNA_84_TO_91_STAGING_CLEAN_duckdb.csv"

DB.parent.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(str(DB))
con.execute("create schema if not exists public;")

# Adjust the column names here to match your CSV headers
con.execute("""
create or replace table public.raw_cnc_ofuna as
select
  cast(monitor_machineno as SMALLINT)  as machine_no,
  cast(TimeStamp_UTC as TIMESTAMP)     as timestamp_utc,
  cast(TimeStamp_SGT as TIMESTAMP)     as timestamp_sgt,
  cast(Quality as INTEGER)             as status,
  cast(Value as VARCHAR)               as value,
  cast(PointName as VARCHAR)           as pointname,
  now()                                as ingested_at
from read_csv_auto(?, header=true);
""", [str(CSV)])

print("raw_cnc_ofuna rows:", con.execute("select count(*) from public.raw_cnc_ofuna").fetchone()[0])
con.close()
