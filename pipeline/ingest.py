"""Land one exchange-day as an immutable gzip file and record it in the manifest.

The landing zone is the durable state of the pipeline. Everything downstream
(the DuckDB warehouse, the dbt models, the dashboard) can be rebuilt from it.

Idempotence rules:
  * one file per (source, trade date), always at the same path
  * gzip is written with mtime=0, so the same CSV always yields the same bytes
  * a day already landed is skipped unless --force
  * a day the exchange had no file for is recorded as no_data, and retried only
    while it is inside the lookback window (the file may simply be late)
"""
import csv
import gzip
import hashlib
import io
import time
import urllib.error
import urllib.request
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from . import MANIFEST, RAW_DIR
from .sources import EXPECTED_COLUMNS, SOURCES, Source

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
}
MANIFEST_FIELDS = ["source", "trade_date", "status", "rows", "sha256", "fetched_at"]
RETRIES = 3


class BadFile(Exception):
    """The exchange answered, but not with a bhavcopy for the day we asked for."""


@dataclass
class Result:
    source: str
    trade_date: date
    status: str  # loaded | no_data | skipped
    rows: int = 0


def raw_path(source: str, day: date, raw_dir: Path = RAW_DIR) -> Path:
    return raw_dir / source / f"{day.year}" / f"{day.isoformat()}.csv.gz"


def fetch(source: Source, day: date, timeout: int = 40) -> bytes | None:
    """Return the day's CSV bytes, or None if the exchange has no file (404)."""
    req = urllib.request.Request(
        source.url_for(day), headers={**HEADERS, "Referer": source.referer}
    )
    for attempt in range(1, RETRIES + 1):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = resp.read()
            break
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            if e.code < 500 or attempt == RETRIES:
                raise
        except (urllib.error.URLError, TimeoutError):
            if attempt == RETRIES:
                raise
        time.sleep(2 ** attempt)
    # BSE answers 200 with its HTML app shell for a day that has no file.
    if body.lstrip()[:1] == b"<":
        return None
    if source.zipped:
        with zipfile.ZipFile(io.BytesIO(body)) as z:
            body = z.read(z.namelist()[0])
    return body


def validate(body: bytes, source: str, day: date) -> int:
    """Check header and trade date on every row. Return the data row count."""
    reader = csv.reader(io.StringIO(body.decode("utf-8-sig")))
    header = next(reader, None)
    if header is None or [h.strip() for h in header[: len(EXPECTED_COLUMNS)]] != EXPECTED_COLUMNS:
        raise BadFile(f"{source} {day}: unexpected header {header!r:.200}")
    rows = 0
    for row in reader:
        if not row:
            continue
        if row[0] != day.isoformat():
            raise BadFile(f"{source} {day}: row carries trade date {row[0]!r}")
        rows += 1
    if rows == 0:
        raise BadFile(f"{source} {day}: header only, no rows")
    return rows


def read_manifest(path: Path = MANIFEST) -> dict[tuple[str, str], dict]:
    if not path.exists():
        return {}
    with path.open(newline="") as f:
        return {(r["source"], r["trade_date"]): r for r in csv.DictReader(f)}


def write_manifest(entries: dict[tuple[str, str], dict], path: Path = MANIFEST) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=MANIFEST_FIELDS, lineterminator="\n")
        w.writeheader()
        for key in sorted(entries):
            w.writerow(entries[key])


def ingest_day(
    source: str,
    day: date,
    manifest: dict,
    *,
    force: bool = False,
    retry_no_data: bool = True,
    raw_dir: Path = RAW_DIR,
    fetcher=fetch,
) -> Result:
    key = (source, day.isoformat())
    prior = manifest.get(key)
    path = raw_path(source, day, raw_dir)
    if prior and not force:
        if prior["status"] == "loaded" and path.exists():
            return Result(source, day, "skipped", int(prior["rows"]))
        if prior["status"] == "no_data" and not retry_no_data:
            return Result(source, day, "skipped")

    body = fetcher(SOURCES[source], day)
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    if body is None:
        manifest[key] = dict(source=source, trade_date=day.isoformat(), status="no_data",
                             rows=0, sha256="", fetched_at=now)
        return Result(source, day, "no_data")

    rows = validate(body, source, day)
    sha = hashlib.sha256(body).hexdigest()
    if prior and prior["sha256"] == sha and path.exists():
        return Result(source, day, "skipped", rows)  # forced re-fetch, same content
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as f, gzip.GzipFile(fileobj=f, mode="wb", mtime=0, filename="") as gz:
        gz.write(body)
    manifest[key] = dict(source=source, trade_date=day.isoformat(), status="loaded",
                         rows=rows, sha256=sha, fetched_at=now)
    return Result(source, day, "loaded", rows)


def weekdays(start: date, end: date):
    day = start
    while day <= end:
        if day.weekday() < 5:
            yield day
        day += timedelta(days=1)


def ingest_range(
    sources: list[str],
    start: date,
    end: date,
    *,
    force: bool = False,
    lookback_days: int = 7,
    pause: float = 1.0,
    log=print,
) -> list[Result]:
    """Ingest every weekday in [start, end]. The manifest is saved after each day,
    so an interrupted backfill resumes where it stopped."""
    manifest = read_manifest()
    retry_after = date.today() - timedelta(days=lookback_days)
    results = []
    for day in weekdays(start, end):
        for source in sources:
            r = ingest_day(source, day, manifest, force=force, retry_no_data=day >= retry_after)
            results.append(r)
            if r.status != "skipped":
                write_manifest(manifest)
                log(f"{r.source} {r.trade_date} {r.status}" + (f" {r.rows} rows" if r.rows else ""),
                    flush=True)
                time.sleep(pause)
    return results
