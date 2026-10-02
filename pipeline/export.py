"""Write docs/data/summary.json, the only thing the dashboard reads."""
import json
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import duckdb

from . import DBT_DIR, SITE_DATA, WAREHOUSE
from .ingest import read_manifest


def _rows(con, sql: str) -> list[dict]:
    cur = con.execute(sql)
    names = [d[0] for d in cur.description]
    return [dict(zip(names, r)) for r in cur.fetchall()]


def _json(o):
    if isinstance(o, (date, datetime)):
        return o.isoformat()
    if isinstance(o, Decimal):
        return float(o)
    raise TypeError(type(o))


def read_checks(run_results: Path) -> dict:
    """Summarise the data tests from dbt's last run."""
    if not run_results.exists():
        return {"total": 0, "pass": 0, "warn": 0, "fail": 0, "skipped": 0, "results": []}
    data = json.loads(run_results.read_text())
    results = []
    for r in data["results"]:
        kind, _, name = r["unique_id"].split(".")[:3]
        if kind != "test":
            continue
        status = "fail" if r["status"] in ("fail", "error") else r["status"]
        results.append({"name": name, "status": status, "failures": r.get("failures") or 0})
    results.sort(key=lambda r: (["fail", "skipped", "warn", "pass"].index(r["status"]), r["name"]))
    count = lambda s: sum(r["status"] == s for r in results)
    return {"total": len(results), "pass": count("pass"), "warn": count("warn"),
            "fail": count("fail"), "skipped": count("skipped"), "results": results}


def export(warehouse: Path = WAREHOUSE, out_dir: Path = SITE_DATA, log=print) -> dict:
    con = duckdb.connect(str(warehouse), read_only=True)
    manifest = read_manifest()
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "checks": read_checks(DBT_DIR / "target" / "run_results.json"),
        "holidays": sorted({d for (_, d), e in manifest.items() if e["status"] == "no_data"}),
        "coverage": _rows(con, """
            select source as exchange, min(trade_date) as first_date, max(trade_date) as last_date,
                   count(*) as days, sum(rows) as rows
            from raw.loaded_files group by source order by source"""),
        "files": _rows(con, """
            select source as exchange, trade_date, rows
            from raw.loaded_files order by trade_date, source"""),
        "breadth": _rows(con, """
            select exchange, trade_date, securities_traded, advancers, decliners, unchanged,
                   turnover_crore, median_pct_change
            from mart_market_breadth order by trade_date, exchange"""),
        "reconciliation": _rows(con, """
            select trade_date, count(*) as pairs,
                   round(median(close_diff_bps), 2) as median_bps,
                   round(quantile_cont(close_diff_bps, 0.99), 2) as p99_bps,
                   count(*) filter (where close_diff_bps > 200 and nse_close >= 10) as over_200bps
            from mart_exchange_reconciliation group by trade_date order by trade_date"""),
        "movers": _rows(con, """
            select exchange, trade_date, direction, rank, symbol, security_name,
                   close_price, pct_change, turnover_crore
            from mart_top_movers order by exchange, direction, rank"""),
    }
    con.close()
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "summary.json").write_text(json.dumps(summary, default=_json, indent=1) + "\n")
    log(f"wrote {out_dir / 'summary.json'}")
    return summary
