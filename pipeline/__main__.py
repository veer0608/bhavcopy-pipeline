"""python -m pipeline <command>

  ingest   land bhavcopy files for a date, a range, or the last N days
  load     load landed files into the DuckDB warehouse
  build    run dbt build (models + tests)
  export   write the dashboard's JSON from the marts
  run      ingest (lookback) + load + build + export: the daily job
"""
import argparse
import os
import subprocess
import sys
from datetime import date, timedelta

from . import DBT_DIR, WAREHOUSE
from .ingest import ingest_range
from .load import load
from .sources import SOURCES


def dbt_build(full_refresh: bool = False) -> int:
    cmd = [sys.executable, "-m", "dbt.cli.main", "build",
           "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    if full_refresh:
        cmd.append("--full-refresh")
    env = {**os.environ, "BHAV_WAREHOUSE": str(WAREHOUSE)}
    return subprocess.run(cmd, cwd=DBT_DIR, env=env).returncode


def main() -> int:
    p = argparse.ArgumentParser(prog="pipeline", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    def add_ingest_args(sp):
        sp.add_argument("--source", nargs="+", choices=sorted(SOURCES), default=sorted(SOURCES))
        sp.add_argument("--lookback", type=int, default=7,
                        help="days back from today to ingest when no dates are given")

    ing = sub.add_parser("ingest")
    add_ingest_args(ing)
    ing.add_argument("--date", type=date.fromisoformat)
    ing.add_argument("--from", dest="start", type=date.fromisoformat)
    ing.add_argument("--to", dest="end", type=date.fromisoformat)
    ing.add_argument("--force", action="store_true", help="re-fetch days already landed")

    sub.add_parser("load")
    b = sub.add_parser("build")
    b.add_argument("--full-refresh", action="store_true")
    sub.add_parser("export")
    add_ingest_args(sub.add_parser("run"))

    a = p.parse_args()
    today = date.today()

    if a.cmd == "ingest":
        if a.date:
            start = end = a.date
        else:
            end = a.end or today
            start = a.start or end - timedelta(days=a.lookback)
        ingest_range(a.source, start, end, force=a.force, lookback_days=a.lookback)
        return 0
    if a.cmd == "load":
        load()
        return 0
    if a.cmd == "build":
        return dbt_build(a.full_refresh)
    if a.cmd == "export":
        from .export import export
        export()
        return 0
    if a.cmd == "run":
        from .export import export
        ingest_range(a.source, today - timedelta(days=a.lookback), today, lookback_days=a.lookback)
        load()
        rc = dbt_build()
        # Export even when tests fail: the dashboard is where a failure shows up.
        export()
        return rc
    return 2


if __name__ == "__main__":
    sys.exit(main())
