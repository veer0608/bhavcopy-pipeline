import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "data" / "raw"
MANIFEST = ROOT / "data" / "manifest.csv"
# The warehouse is a cache of the landing zone. BHAV_WAREHOUSE points both the
# loader and dbt at another file, which is how the tests build a scratch copy.
WAREHOUSE = Path(os.environ.get("BHAV_WAREHOUSE", ROOT / "warehouse.duckdb")).resolve()
DBT_DIR = ROOT / "dbt"
SITE_DATA = ROOT / "docs" / "data"
