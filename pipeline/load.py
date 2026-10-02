"""Load landed files into DuckDB as an untyped raw table.

Every column stays VARCHAR here. Typing is dbt's job (stg_bhavcopy), so a bad
value in one file surfaces as a failed test rather than a failed load.

A file is loaded when its sha256 is not yet in raw.loaded_files. Loading a day
replaces that day's rows inside one transaction, so a corrected file from the
exchange restates the day instead of duplicating it.
"""
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from . import RAW_DIR, WAREHOUSE
from .ingest import raw_path, read_manifest
from .sources import EXPECTED_COLUMNS

DDL = f"""
create schema if not exists raw;
create table if not exists raw.bhavcopy (
    {", ".join(f'"{c}" varchar' for c in EXPECTED_COLUMNS)},
    _source varchar, _trade_date date, _sha256 varchar, _loaded_at timestamptz
);
create table if not exists raw.loaded_files (
    source varchar, trade_date date, sha256 varchar, rows bigint, loaded_at timestamptz,
    primary key (source, trade_date)
);
"""


def load(warehouse: Path = WAREHOUSE, raw_dir: Path = RAW_DIR, manifest: dict | None = None,
         log=print) -> int:
    manifest = read_manifest() if manifest is None else manifest
    con = duckdb.connect(str(warehouse))
    con.execute(DDL)
    done = {(s, d.isoformat()): h for s, d, h in
            con.execute("select source, trade_date, sha256 from raw.loaded_files").fetchall()}
    # One timestamp for the whole batch: dbt's incremental models pick up
    # everything newer than the last batch they saw.
    batch = datetime.now(timezone.utc)
    cols = ", ".join(f'"{c}"' for c in EXPECTED_COLUMNS)
    loaded = 0
    for (source, day), entry in sorted(manifest.items()):
        if entry["status"] != "loaded" or done.get((source, day)) == entry["sha256"]:
            continue
        path = raw_path(source, datetime.fromisoformat(day).date(), raw_dir)
        con.execute("begin")
        con.execute("delete from raw.bhavcopy where _source = ? and _trade_date = ?", [source, day])
        con.execute(
            f"""insert into raw.bhavcopy
                select {cols}, ?, ?::date, ?, ?
                from read_csv(?, header = true, all_varchar = true)""",
            [source, day, entry["sha256"], batch, str(path)],
        )
        rows = con.execute(
            "select count(*) from raw.bhavcopy where _source = ? and _trade_date = ?", [source, day]
        ).fetchone()[0]
        if rows != int(entry["rows"]):
            con.execute("rollback")
            raise RuntimeError(f"{source} {day}: manifest says {entry['rows']} rows, loaded {rows}")
        con.execute("insert or replace into raw.loaded_files values (?, ?, ?, ?, ?)",
                    [source, day, entry["sha256"], rows, batch])
        con.execute("commit")
        loaded += 1
    con.close()
    log(f"loaded {loaded} file(s) into {warehouse.name}")
    return loaded
