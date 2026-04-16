"""Convert a CSV file to SQLite via the cleaning pipeline.

Public API:
  convert_to_sqlite(csv_path, sqlite_dir) -> (Path, str)
    Cleans the CSV and writes <sqlite_dir>/<table_name>.sqlite.
    Idempotent: skips conversion if file already exists.

  resolve_table_name(csv_name) -> str
    Normalize a CSV filename stem to a clean SQLite table name.
"""

from __future__ import annotations

import logging
import re
import sqlite3
import unicodedata
from pathlib import Path

from src.cleaning import clean_csv

log = logging.getLogger(__name__)


def _normalize_table_name(stem: str) -> str:
  """Lowercase, bỏ dấu tiếng Việt (giữ chữ cái), thay . và - thành _."""
  # đ/Đ không decompose qua NFD nên handle riêng
  stem = stem.replace("đ", "d").replace("Đ", "D")
  stem = unicodedata.normalize("NFD", stem).encode("ascii", "ignore").decode("ascii")
  stem = stem.lower().replace(".", "_").replace("-", "_")
  stem = re.sub(r"_+", "_", stem).strip("_")
  return stem or "table"


def resolve_table_name(csv_name: str) -> str:
  """Normalize csv filename stem to a clean SQLite table name."""
  return _normalize_table_name(Path(csv_name).stem)


def convert_to_sqlite(
  csv_path: Path,
  sqlite_dir: Path,
  *,
  overwrite: bool = False,
) -> tuple[Path, str]:
  """Clean csv_path and write to <sqlite_dir>/<table_name>.sqlite.

  Returns (sqlite_path, table_name).
  Skips if the file already exists unless overwrite=True.
  """
  table_name = resolve_table_name(csv_path.name)
  sqlite_path = sqlite_dir / f"{table_name}.sqlite"

  if sqlite_path.exists() and not overwrite:
    log.debug("SQLite already exists, skipping: %s", sqlite_path)
    return sqlite_path, table_name

  sqlite_dir.mkdir(parents=True, exist_ok=True)

  df = clean_csv(csv_path)
  if df.empty:
    raise ValueError(f"No data after cleaning: {csv_path.name}")

  with sqlite3.connect(sqlite_path) as conn:
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.commit()

  log.info(
    "Converted: %-50s → %-40s (%d rows)",
    csv_path.name, table_name, len(df),
  )
  return sqlite_path, table_name
