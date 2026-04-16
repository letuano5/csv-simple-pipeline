"""M-Schema generation from a SQLite file.

Format (from m-schema.txt):
  【DB_ID】 <table_name>
  【Schema】
  # Table: <table_name>
  [
  (col:TYPE, [Primary Key,] Examples: [v1, v2, v3]),
  ...
  ]
  Sample rows from `<table_name>`:
  val1, val2, ...

Public API:
  build_m_schema(sqlite_path, table_name, sample_rows=3, sample_values=3) -> str
  get_table_name(sqlite_path) -> str | None
"""

from __future__ import annotations

import sqlite3
from pathlib import Path


def _sqlite_type_to_schema_type(sqlite_type: str) -> str:
  """Map SQLite affinity type to M-Schema type label."""
  t = sqlite_type.upper().strip()
  if "INT" in t:
    return "INTEGER"
  if any(x in t for x in ("REAL", "FLOAT", "DOUBLE", "NUMERIC", "DECIMAL")):
    return "REAL"
  if any(x in t for x in ("DATE", "TIME")):
    return "DATE"
  if "BOOL" in t:
    return "BOOLEAN"
  return "TEXT"


def get_table_name(sqlite_path: Path) -> str | None:
  """Return the first user table name in the SQLite file."""
  with sqlite3.connect(sqlite_path) as conn:
    row = conn.execute(
      "SELECT name FROM sqlite_master WHERE type='table' ORDER BY rowid LIMIT 1"
    ).fetchone()
  return row[0] if row else None


def build_m_schema(
  sqlite_path: Path,
  table_name: str,
  *,
  sample_rows: int = 3,
  sample_values: int = 3,
) -> str:
  """Build an M-Schema string for a single-table SQLite database.

  Args:
    sqlite_path:   Path to the .sqlite file.
    table_name:    Table name inside the SQLite file.
    sample_rows:   Number of sample rows to include.
    sample_values: Max distinct non-null example values per column.

  Returns:
    Multi-line M-Schema string.
  """
  with sqlite3.connect(sqlite_path) as conn:
    conn.row_factory = sqlite3.Row

    # Get column info
    pragma = conn.execute(f'PRAGMA table_info("{table_name}")').fetchall()
    if not pragma:
      return f"【DB_ID】 {table_name}\n(no columns found)"

    columns = [(row["name"], row["type"], bool(row["pk"])) for row in pragma]
    col_names = [c[0] for c in columns]

    # Sample distinct values per column
    sample_vals: dict[str, list] = {}
    for col_name, _, _ in columns:
      try:
        rows = conn.execute(
          f'SELECT DISTINCT "{col_name}" FROM "{table_name}" '
          f'WHERE "{col_name}" IS NOT NULL '
          f'LIMIT {sample_values}'
        ).fetchall()
        sample_vals[col_name] = [r[0] for r in rows]
      except Exception:
        sample_vals[col_name] = []

    # Sample rows
    try:
      sample_row_data = conn.execute(
        f'SELECT * FROM "{table_name}" LIMIT {sample_rows}'
      ).fetchall()
    except Exception:
      sample_row_data = []

  # Build schema string
  lines: list[str] = []
  lines.append(f"【DB_ID】 {table_name}")
  lines.append("【Schema】")
  lines.append(f"# Table: {table_name}")
  lines.append("[")

  for col_name, col_type, is_pk in columns:
    schema_type = _sqlite_type_to_schema_type(col_type)
    examples = sample_vals.get(col_name, [])
    example_strs = [repr(v) if isinstance(v, str) else str(v) for v in examples[:sample_values]]
    parts = [f"{col_name}:{schema_type}"]
    if is_pk:
      parts.append("Primary Key")
    if example_strs:
      parts.append(f"Examples: [{', '.join(example_strs)}]")
    lines.append(f"({', '.join(parts)}),")

  lines.append("]")

  if sample_row_data:
    lines.append(f"Sample rows from `{table_name}`:")
    for row in sample_row_data:
      row_strs = [str(v) if v is not None else "NULL" for v in row]
      lines.append(", ".join(row_strs))

  return "\n".join(lines)
