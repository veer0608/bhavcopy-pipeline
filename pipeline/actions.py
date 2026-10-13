"""Land NSE corporate actions in data/corporate_actions.csv.

Splits and bonuses change the share count overnight, and the bhavcopy reports
the lower price as a fall: GOODLUCK's 2:1 bonus on 2026-08-21 shows as -65.90%,
even in the exchange's own previous-close column. dbt uses this file to
adjust daily changes (fct_daily_returns).

NSE splits listings across two feeds: stocks under "equities", ETFs (which
trade in the same EQ series) under "mf". The mf feed returns every row twice,
and a 10:1 split applied twice would become 100:1, so rows are deduplicated.

Each run fetches a window around today and merges it into the file. Ex-dates
are announced weeks ahead, so overlapping windows mean a failed day loses
nothing; a failure is logged and the run carries on with the file it has.
"""
import csv
import gzip
import http.cookiejar
import json
import re
import urllib.request
from datetime import date, datetime
from pathlib import Path

from . import ROOT

ACTIONS = ROOT / "data" / "corporate_actions.csv"
FEEDS = ("equities", "mf")
FIELDS = ["feed", "symbol", "series", "isin", "ex_date", "kind", "num_a", "num_b", "subject"]

PAGE = "https://www.nseindia.com/companies-listing/corporate-filings-actions"
API = "https://www.nseindia.com/api/corporates-corporateActions?index={}&from_date={}&to_date={}"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Accept": "*/*",
    "Referer": PAGE,
}

# "Face Value Split (Sub-Division) - From Rs 10/- Per Share To Re 1/- Per Share"
SPLIT = re.compile(r"^Face Value Split.*From R[se]\.? ?([\d.]+).*To R[se]\.? ?([\d.]+)", re.I)
# "Bonus 2:1" is 2 new shares for every 1 held. Anchored, so "Scheme Of
# Arrangement - Bonus Ncrps 4:1" (preference shares) is not read as one.
BONUS = re.compile(r"^Bonus (\d+):(\d+)", re.I)


def classify(subject: str) -> tuple[str, str, str]:
    """(kind, num_a, num_b). Split: old and new face value. Bonus: x and y of x:y."""
    if m := SPLIT.match(subject):
        return "split", m[1], m[2]
    if m := BONUS.match(subject):
        return "bonus", m[1], m[2]
    s = subject.lower()
    for kind in ("demerger", "rights", "dividend"):
        if kind in s:
            return kind, "", ""
    return "other", "", ""


def fetch_nse(start: date, end: date) -> list[tuple[str, dict]]:
    """(feed, action) pairs from both feeds. The API needs the cookies its
    listing page sets, so the page is fetched first in the same session."""
    opener = urllib.request.build_opener(
        urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))

    def get(url: str) -> bytes:
        r = opener.open(urllib.request.Request(url, headers=HEADERS), timeout=30)
        body = r.read()
        return gzip.decompress(body) if r.headers.get("Content-Encoding") == "gzip" else body

    get(PAGE)
    fmt = "%d-%m-%Y"
    return [(feed, r)
            for feed in FEEDS
            for r in json.loads(get(API.format(feed, start.strftime(fmt), end.strftime(fmt))))]


def to_row(feed: str, r: dict) -> dict:
    kind, a, b = classify(r["subject"])
    return {"feed": feed, "symbol": r["symbol"], "series": r["series"], "isin": r["isin"],
            "ex_date": datetime.strptime(r["exDate"], "%d-%b-%Y").date().isoformat(),
            "kind": kind, "num_a": a, "num_b": b, "subject": r["subject"]}


def key(row: dict) -> tuple:
    return row["symbol"], row["ex_date"], row["subject"]


def read_actions(path: Path = ACTIONS) -> list[dict]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def refresh_actions(start: date, end: date, path: Path = ACTIONS, fetcher=fetch_nse,
                    log=print) -> int:
    """Merge actions with ex-dates in [start, end] into the file. Returns the
    number of new rows. Existing rows are never dropped, so a feed that later
    omits an action cannot un-adjust history."""
    try:
        fetched = fetcher(start, end)
    except Exception as e:  # network, NSE throttling, a changed API
        log(f"corporate actions not refreshed ({type(e).__name__}: {e}); keeping {path.name}")
        return 0
    rows = {key(r): r for r in read_actions(path)}
    before = len(rows)
    for feed, r in fetched:
        row = to_row(feed, r)
        rows.setdefault(key(row), row)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, lineterminator="\n")
        w.writeheader()
        w.writerows(sorted(rows.values(), key=lambda r: (r["ex_date"], r["symbol"], r["subject"])))
    added = len(rows) - before
    log(f"corporate actions {start}..{end}: {len(fetched)} fetched, {added} new, {len(rows)} total")
    return added
