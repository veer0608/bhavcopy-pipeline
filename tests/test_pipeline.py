"""The pipeline's promises, tested without the network.

A fake exchange serves synthetic bhavcopy files, so these run anywhere and
in a few seconds. The dbt tests run inside the build under test, so a build
returning 0 means every data check passed too.
"""
import os
import subprocess
import sys
from datetime import date
from pathlib import Path

import duckdb
import pytest

from pipeline import DBT_DIR
from pipeline.ingest import BadFile, ingest_day, raw_path
from pipeline.load import load
from pipeline.sources import EXPECTED_COLUMNS

DAYS = [date(2026, 9, 28), date(2026, 9, 29), date(2026, 9, 30)]


def make_csv(source: str, day: date, n: int = 40, bump: float = 0.0, drop: int = 0) -> bytes:
    series = "EQ" if source == "NSE" else "A"
    lines = [",".join(EXPECTED_COLUMNS)]
    for i in range(n - drop):
        close = round(100 + i + day.day * 0.5 + bump, 2)
        row = dict.fromkeys(EXPECTED_COLUMNS, "")
        row.update(TradDt=day.isoformat(), BizDt=day.isoformat(), Sgmt="CM", Src=source,
                   FinInstrmTp="STK", FinInstrmId=str(1000 + i), ISIN=f"INE{i:06d}01",
                   TckrSymb=f"SYM{i}", SctySrs=series, FinInstrmNm=f"Company {i}",
                   OpnPric=f"{close - 1:.2f}", HghPric=f"{close + 2:.2f}", LwPric=f"{close - 2:.2f}",
                   ClsPric=f"{close:.2f}", LastPric=f"{close:.2f}", PrvsClsgPric=f"{close - 0.5:.2f}",
                   TtlTradgVol="200000", TtlTrfVal="25000000.00", TtlNbOfTxsExctd="900",
                   SsnId="F1", NewBrdLotQty="1")
        lines.append(",".join(row[c] for c in EXPECTED_COLUMNS))
    return ("\n".join(lines) + "\n").encode()


class FakeExchange:
    def __init__(self):
        self.files, self.calls = {}, 0

    def put(self, source, day, **kw):
        self.files[(source, day)] = make_csv(source, day, **kw)

    def __call__(self, source, day):
        self.calls += 1
        return self.files.get((source.name, day))


@pytest.fixture
def env(tmp_path):
    ex = FakeExchange()
    for d in DAYS:
        for s in ("NSE", "BSE"):
            ex.put(s, d)
    return ex, tmp_path / "raw", tmp_path / "wh.duckdb"


def ingest_all(ex, raw, manifest, days=DAYS, **kw):
    return [ingest_day(s, d, manifest, raw_dir=raw, fetcher=ex, **kw)
            for d in days for s in ("NSE", "BSE")]


def build(warehouse: Path, *extra) -> None:
    env = {**os.environ, "BHAV_WAREHOUSE": str(warehouse)}
    cmd = [sys.executable, "-m", "dbt.cli.main", "build", "--project-dir", str(DBT_DIR),
           "--profiles-dir", str(DBT_DIR), "--target-path", str(warehouse.parent / "target"), *extra]
    r = subprocess.run(cmd, cwd=DBT_DIR, env=env, capture_output=True, text=True)
    assert r.returncode == 0, r.stdout[-3000:]


def fact(warehouse: Path):
    """Everything in the fact table except the load timestamp."""
    con = duckdb.connect(str(warehouse), read_only=True)
    try:
        return con.execute(
            "select * exclude (loaded_at) from fct_daily_prices order by price_key").fetchall()
    finally:
        con.close()


def test_reingest_is_a_noop(env):
    ex, raw, _ = env
    manifest = {}
    assert {r.status for r in ingest_all(ex, raw, manifest)} == {"loaded"}
    before = {p: p.read_bytes() for p in raw.rglob("*.gz")}
    snapshot, calls = dict(manifest), ex.calls

    assert {r.status for r in ingest_all(ex, raw, manifest)} == {"skipped"}
    assert ex.calls == calls, "a landed day must not be fetched again"
    assert manifest == snapshot
    assert {p: p.read_bytes() for p in raw.rglob("*.gz")} == before


def test_forced_refetch_of_same_content_writes_identical_bytes(env):
    ex, raw, _ = env
    manifest = {}
    ingest_all(ex, raw, manifest)
    path = raw_path("NSE", DAYS[0], raw)
    before, entry = path.read_bytes(), dict(manifest[("NSE", DAYS[0].isoformat())])
    assert ingest_day("NSE", DAYS[0], manifest, raw_dir=raw, fetcher=ex, force=True).status == "skipped"
    assert path.read_bytes() == before
    assert manifest[("NSE", DAYS[0].isoformat())] == entry


def test_holiday_is_recorded_and_only_retried_inside_the_lookback(env):
    ex, raw, _ = env
    manifest, holiday = {}, date(2026, 10, 2)
    assert ingest_day("NSE", holiday, manifest, raw_dir=raw, fetcher=ex).status == "no_data"
    calls = ex.calls
    assert ingest_day("NSE", holiday, manifest, raw_dir=raw, fetcher=ex, retry_no_data=False).status == "skipped"
    assert ex.calls == calls
    # Inside the lookback the day is asked for again, in case the file was late.
    ex.put("NSE", holiday)
    assert ingest_day("NSE", holiday, manifest, raw_dir=raw, fetcher=ex).status == "loaded"


def test_file_for_the_wrong_day_is_rejected(env):
    ex, raw, _ = env
    ex.files[("NSE", DAYS[1])] = make_csv("NSE", DAYS[0])
    manifest = {}
    with pytest.raises(BadFile):
        ingest_day("NSE", DAYS[1], manifest, raw_dir=raw, fetcher=ex)
    assert manifest == {} and not raw_path("NSE", DAYS[1], raw).exists()


def test_incremental_build_equals_full_refresh(env):
    ex, raw, wh = env
    manifest = {}
    # Day by day, the way the daily job sees it.
    for d in DAYS:
        ingest_all(ex, raw, manifest, days=[d])
        load(wh, raw, manifest, log=lambda *_: None)
        build(wh)
    incremental = fact(wh)
    assert len(incremental) == 40 * 2 * len(DAYS)

    build(wh)  # nothing new: must change nothing
    assert fact(wh) == incremental

    build(wh, "--full-refresh")
    assert fact(wh) == incremental


def test_corrected_file_restates_its_day(env):
    ex, raw, wh = env
    manifest = {}
    ingest_all(ex, raw, manifest)
    load(wh, raw, manifest, log=lambda *_: None)
    build(wh)

    # The exchange republishes one day: prices changed and two instruments removed.
    ex.put("NSE", DAYS[1], bump=3.0, drop=2)
    assert ingest_day("NSE", DAYS[1], manifest, raw_dir=raw, fetcher=ex, force=True).status == "loaded"
    assert load(wh, raw, manifest, log=lambda *_: None) == 1
    build(wh)
    restated = fact(wh)
    assert len(restated) == 40 * 2 * len(DAYS) - 2

    build(wh, "--full-refresh")
    assert fact(wh) == restated
