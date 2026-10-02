# bhavcopy-pipeline

A daily data pipeline for Indian stock market end-of-day prices. It pulls the
bhavcopy file from NSE and BSE each evening, lands it, loads it into DuckDB,
models it with dbt, tests it, and publishes a small dashboard.

No LLM anywhere. This is plain data engineering: incremental loads, idempotent
backfills, restatements, and data quality checks that are allowed to fail.

## Where it stands

Measured on 2026-10-02, on the data in this repo:

| | |
|---|---|
| Trading days ingested | 66 per exchange, 2026-07-01 to 2026-10-01 |
| Rows loaded | 563,111 (NSE 234,475, BSE 328,636) |
| dbt data tests | 18: 16 pass, 2 warn, 0 fail |
| Unit tests | 6 pass, no network needed |
| Exchange holidays detected | 2 (2026-09-14, 2026-10-02) |

## How it works

```
exchange file  ->  data/raw/<SRC>/<year>/<date>.csv.gz   landing zone, in git
               ->  raw.bhavcopy                          DuckDB, every column VARCHAR
               ->  stg_bhavcopy                          dbt view: renamed and typed
               ->  fct_daily_prices                      dbt incremental, one row per instrument per day
               ->  mart_market_breadth                   advancers and decliners per day
                   mart_exchange_reconciliation          NSE close against BSE close
                   mart_top_movers                       latest day's biggest movers
               ->  docs/data/summary.json                what the dashboard reads
```

The landing zone is the only durable state. `warehouse.duckdb` is a cache and is
not committed. Delete it and the next run rebuilds it from `data/raw`.

## The promises, and what checks each one

**Re-running changes nothing.** One file per exchange per day, always at the same
path, written with a fixed gzip timestamp so the same CSV gives the same bytes. A
day already landed is not fetched again. `test_reingest_is_a_noop`

**Incremental equals full refresh.** `fct_daily_prices` picks up load batches newer
than the last one it saw and replaces each exchange-day whole. Building day by day
gives exactly the table a `--full-refresh` gives. `test_incremental_build_equals_full_refresh`

**A corrected file restates its day.** If an exchange republishes a day, the new
sha256 triggers a reload and the day is deleted and reinserted, including rows the
correction removed. `test_corrected_file_restates_its_day`

**Nothing is dropped or duplicated.** Row counts per exchange-day in the fact table
must equal the count recorded when the file landed. `assert_fact_reconciles_to_raw`

**A bad file is stopped at the door.** Wrong header, or any row carrying a different
trade date, and the file is rejected before it is written. `test_file_for_the_wrong_day_is_rejected`

**A holiday is not an error.** A weekday with no file is recorded as `no_data` and
retried only inside a 7-day lookback, in case the file was simply late.

## What the checks found in real data

These are the reason the tests exist, so they are listed rather than hidden.

- **BSE answers 200 for a missing day.** It serves its HTML app shell instead of a
  404. The fetcher treats an HTML body as "no file".
- **Two rows where the exchange's own close is outside its high-low range.** Both are
  thinly traded non-equity series (an NSE T+0 settlement series and a BSE
  infrastructure fund), where the published close is not taken from the day's
  trades. The range check is a hard failure for equities and a warning for
  everything else.
- **149 cases where NSE and BSE close more than 2% apart** on a liquid stock listed
  on both, out of 48,102 comparisons. They cluster on a few days (40 on
  2026-08-31, 30 on 2026-09-18). The median gap is about 7 basis points. This stays
  a warning. I have not established the cause.

## Run it

Needs Python 3.11. Commands are for Git Bash; in PowerShell use
`.venv\Scripts\python` the same way.

```bash
python -m venv .venv
.venv/Scripts/python -m pip install -r requirements.txt

.venv/Scripts/python -m pipeline run                                   # the daily job
.venv/Scripts/python -m pipeline ingest --from 2026-07-01 --to 2026-09-30   # backfill
.venv/Scripts/python -m pipeline build --full-refresh                  # rebuild every model
.venv/Scripts/python -m pytest -q
```

To see the dashboard locally:

```bash
.venv/Scripts/python -m http.server 4174 --directory docs
```

## Scheduling

`.github/workflows/daily.yml` runs on weekdays at 22:00 IST. It runs the unit
tests, restores the warehouse cache, runs the pipeline, and commits new landing
files and the dashboard JSON. If a data test fails, the data is still committed so
the failure shows on the dashboard, and the run goes red.

Both exchanges serve these files to GitHub's runners. A manual run with a
`probe_date` downloads that day on the runner and reports the row counts without
writing anything: on 2026-10-02 it returned 3,712 NSE rows and 5,163 BSE rows for
2026-10-01, the same counts as the local download.

## Known limits

- The landing zone grows about 470 KB per trading day, roughly 120 MB a year.
- Exchange holidays are inferred from a missing file, not read from a calendar.
- Prices are not adjusted for splits or bonuses.
