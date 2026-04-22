"""CSV data cleaning pipeline.

Adapted from excel-eda/src/normalizer.py + utils/*.
Handles: encoding detection, delimiter sniffing, header detection,
column name sanitization, ditto marks, Excel errors, type conversion,
leading-zero protection, VN/US number formats.
"""

from __future__ import annotations

import csv
import re
import unicodedata
from enum import Enum
from pathlib import Path
from typing import Any

import chardet
import pandas as pd


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_EXCEL_ERRORS = frozenset({
  "#n/a", "#value!", "#ref!", "#div/0!", "#num!", "#name?", "#null!",
  "#getting_data", "#spill!", "#calc!", "#field!", "#unknown!",
})

_NA_STRINGS = frozenset({
  "", "n/a", "na", "null", "none", "#n/a", "nan", "-",
})

# Protect leading-zero identifiers (phone numbers, codes, etc.)
_LEADING_ZERO_RE = re.compile(r"^0\d+$")

MAX_SCAN_ROWS = 30
MAX_HEADER_ROWS = 6


# ---------------------------------------------------------------------------
# Number parsing (VN / US / plain)
# ---------------------------------------------------------------------------

VN_NUMBER_RE = re.compile(r"^-?\d{1,3}(\.\d{3})+(,\d+)?$")
US_NUMBER_RE = re.compile(r"^-?\d{1,3}(,\d{3})+(\.\d+)?$")
PLAIN_INT_RE = re.compile(r"^-?\d+$")
PLAIN_FLOAT_RE = re.compile(r"^-?\d+\.\d+$")
VN_SIMPLE_DECIMAL_RE = re.compile(r"^-?\d+,\d+$")  # 10,5 → 10.5 ; 10,111 → 10.111
PERCENT_RE = re.compile(r"^-?[\d.,]+\s*%$")
CURRENCY_RE = re.compile(
  r"^[\$€£¥₫]?\s*-?[\d.,]+\s*(?:đồng|đ|VND|VNĐ|USD|\$|€|£)?$",
  re.IGNORECASE,
)
CURRENCY_SYMBOL_RE = re.compile(r"[\$€£¥₫]|đồng|VND|VNĐ|USD", re.IGNORECASE)


def _parse_vn_number(s: str) -> float | int | None:
  s = s.strip()
  if VN_NUMBER_RE.match(s):
    try:
      f = float(s.replace(".", "").replace(",", "."))
      return int(f) if f == int(f) else f
    except (ValueError, OverflowError):
      return None
  return None


def _parse_us_number(s: str) -> float | int | None:
  s = s.strip()
  if US_NUMBER_RE.match(s):
    try:
      f = float(s.replace(",", ""))
      return int(f) if f == int(f) else f
    except (ValueError, OverflowError):
      return None
  return None


def _parse_plain_number(s: str) -> float | int | None:
  s = s.strip()
  if PLAIN_INT_RE.match(s):
    try:
      return int(s)
    except (ValueError, OverflowError):
      return None
  if PLAIN_FLOAT_RE.match(s):
    try:
      return float(s)
    except (ValueError, OverflowError):
      return None
  return None


def _parse_vn_simple_decimal(s: str) -> float | int | None:
  """VN decimal không có dấu chấm nghìn: 10,5 → 10.5 ; 10,111 → 10.111."""
  s = s.strip()
  if VN_SIMPLE_DECIMAL_RE.match(s):
    try:
      f = float(s.replace(",", "."))
      return int(f) if f == int(f) else f
    except (ValueError, OverflowError):
      return None
  return None


def _parse_number_auto(s: str) -> float | int | None:
  """Fallback khi không biết format cột: plain → VN simple (comma=decimal) → US → VN full."""
  if not isinstance(s, str):
    return None
  s = s.strip()
  if not s:
    return None
  val = _parse_plain_number(s)
  if val is not None:
    return val
  # Prefer VN comma-as-decimal over US comma-as-thousands when format cannot be determined from column context
  val = _parse_vn_simple_decimal(s)
  if val is not None:
    return val
  val = _parse_us_number(s)
  if val is not None:
    return val
  return _parse_vn_number(s)


def _parse_percentage(s: str) -> float | None:
  s = s.strip()
  if not PERCENT_RE.match(s):
    return None
  num_str = s.replace("%", "").strip()
  val = _parse_vn_number(num_str) or _parse_us_number(num_str) or _parse_plain_number(num_str)
  return round(float(val) / 100.0, 6) if val is not None else None


def _parse_currency(s: str) -> tuple[float | None, str | None]:
  s = s.strip()
  if not CURRENCY_RE.match(s):
    return None, None
  m = CURRENCY_SYMBOL_RE.search(s)
  if m is None:
    # No actual currency marker — don't classify plain numbers as currency
    return None, None
  symbol = m.group(0)
  num_str = CURRENCY_SYMBOL_RE.sub("", s).strip()
  val = _parse_vn_number(num_str) or _parse_us_number(num_str) or _parse_plain_number(num_str)
  return (float(val), symbol) if val is not None else (None, None)


def _detect_col_number_format(values: list) -> str:
  counts = {"plain": 0, "us": 0, "vn": 0}
  total = 0
  for v in values:
    if not isinstance(v, str) or not v.strip():
      continue
    s = v.strip()
    total += 1
    if PLAIN_INT_RE.match(s) or PLAIN_FLOAT_RE.match(s):
      counts["plain"] += 1
    elif US_NUMBER_RE.match(s):
      counts["us"] += 1
    elif VN_NUMBER_RE.match(s):
      counts["vn"] += 1
    elif VN_SIMPLE_DECIMAL_RE.match(s):
      # chỉ đếm khi không overlap với US (vd: 10,5 là VN rõ ràng; 10,111 đã bị US bắt trước)
      counts["vn"] += 1
  if total == 0:
    return "plain"
  for fmt, count in sorted(counts.items(), key=lambda x: -x[1]):
    if count > total * 0.5:
      return fmt
  return "mixed"


# ---------------------------------------------------------------------------
# Date / bool parsing
# ---------------------------------------------------------------------------

from datetime import datetime

# Strip timezone suffix trước khi parse: +07:00, +0700, Z, UTC, GMT
TZ_SUFFIX_RE = re.compile(r"\s*(Z|UTC|GMT|[+-]\d{2}:?\d{2})$", re.IGNORECASE)

DATE_PATTERNS = [
  # Date only
  (re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$"),                               "dmy"),
  (re.compile(r"^(\d{4})-(\d{1,2})-(\d{1,2})$"),                               "ymd"),
  (re.compile(r"^(\d{1,2})-(\d{1,2})-(\d{4})$"),                               "dmy"),
  (re.compile(r"^(\d{1,2})\.(\d{1,2})\.(\d{4})$"),                             "dmy"),
  # Datetime — ISO (T hoặc space), fractional seconds tùy chọn
  (re.compile(r"^(\d{4})-(\d{1,2})-(\d{1,2})[T ](\d{2}):(\d{2}):(\d{2})(?:\.\d+)?$"), "ymd_hms"),
  (re.compile(r"^(\d{4})-(\d{1,2})-(\d{1,2})[T ](\d{2}):(\d{2})$"),           "ymd_hm"),
  # Datetime — DD/MM/YYYY HH:MM[:SS]
  (re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})\s+(\d{2}):(\d{2}):(\d{2})(?:\.\d+)?$"), "dmy_hms"),
  (re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})\s+(\d{2}):(\d{2})$"),            "dmy_hm"),
]

BOOL_TRUE = {"x", "✓", "✔", "có", "yes", "true", "1", "đúng"}
BOOL_FALSE = {"✗", "✘", "không", "no", "false", "0", "sai"}


def _try_parse_date(value: Any) -> datetime | None:
  if isinstance(value, datetime):
    return value
  if not isinstance(value, str):
    return None
  s = TZ_SUFFIX_RE.sub("", value.strip()).strip()
  for pattern, fmt in DATE_PATTERNS:
    m = pattern.match(s)
    if m:
      g = m.groups()
      try:
        if fmt == "dmy":
          return datetime(int(g[2]), int(g[1]), int(g[0]))
        elif fmt == "ymd":
          return datetime(int(g[0]), int(g[1]), int(g[2]))
        elif fmt == "ymd_hms":
          return datetime(int(g[0]), int(g[1]), int(g[2]), int(g[3]), int(g[4]), int(g[5]))
        elif fmt == "ymd_hm":
          return datetime(int(g[0]), int(g[1]), int(g[2]), int(g[3]), int(g[4]))
        elif fmt == "dmy_hms":
          return datetime(int(g[2]), int(g[1]), int(g[0]), int(g[3]), int(g[4]), int(g[5]))
        elif fmt == "dmy_hm":
          return datetime(int(g[2]), int(g[1]), int(g[0]), int(g[3]), int(g[4]))
      except ValueError:
        continue
  return None


def _try_parse_bool(value: Any) -> bool | None:
  if isinstance(value, bool):
    return value
  if not isinstance(value, str):
    return None
  s = value.strip().lower()
  if s in BOOL_TRUE:
    return True
  if s in BOOL_FALSE:
    return False
  return None


# ---------------------------------------------------------------------------
# Column type detection
# ---------------------------------------------------------------------------

def _detect_column_type(
  values: list[Any],
  col_name: str = "",
  threshold: float = 0.7,
) -> tuple[str, list[Any]]:
  """Return (type_str, converted_values). Types: date/bool/percentage/currency/int/float/text."""
  non_null = [(i, v) for i, v in enumerate(values) if v is not None]
  if not non_null:
    return "text", values

  total = len(non_null)
  results: dict[str, list] = {t: [] for t in ["date", "bool", "percentage", "currency", "int", "float", "text"]}

  str_values = [str(v) for _, v in non_null if isinstance(v, str)]
  num_format = _detect_col_number_format(str_values)

  for idx, val in non_null:
    if isinstance(val, bool):
      results["bool"].append((idx, val))
      continue
    if isinstance(val, (int, float)):
      if isinstance(val, float) and val == int(val) and abs(val) < 2**53:
        results["int"].append((idx, int(val)))
      results["float"].append((idx, round(float(val), 6)))
      continue
    if isinstance(val, datetime):
      results["date"].append((idx, val))
      continue
    if not isinstance(val, str):
      results["text"].append((idx, str(val)))
      continue

    s = val.strip()
    if not s:
      continue

    d = _try_parse_date(s)
    if d is not None:
      results["date"].append((idx, d))
      continue

    b = _try_parse_bool(s)
    if b is not None:
      results["bool"].append((idx, b))
      continue

    pct = _parse_percentage(s)
    if pct is not None:
      results["percentage"].append((idx, pct))
      continue

    cur_val, _ = _parse_currency(s)
    if cur_val is not None:
      results["currency"].append((idx, cur_val))
      continue

    num = None
    if num_format == "vn":
      num = _parse_vn_number(s)
      if num is None:
        num = _parse_vn_simple_decimal(s)
    elif num_format == "us":
      num = _parse_us_number(s)
    if num is None:
      num = _parse_number_auto(s)

    if num is not None:
      if isinstance(num, int):
        results["int"].append((idx, num))
      results["float"].append((idx, round(float(num), 6)))
      continue

    results["text"].append((idx, s))

  counts = {
    "date": len(results["date"]),
    "bool": len(results["bool"]),
    "percentage": len(results["percentage"]),
    "currency": len(results["currency"]),
    "float": len(results["float"]),
    "text": len(results["text"]),
  }

  best_type = "text"
  best_count = 0
  for t in ["date", "bool", "percentage", "currency", "float", "text"]:
    if counts[t] > best_count:
      best_count = counts[t]
      best_type = t

  # Below-threshold columns stay as text; a mixed column that is only partially parseable should not be coerced
  if best_count / total < threshold:
    best_type = "text"

  # Integer values are accumulated inside the float bucket; downgrade only when every parsed value is a whole number
  if best_type == "float":
    all_int = all(
      isinstance(v, float) and v == int(v) and abs(v) < 2**53
      for _, v in results["float"]
    )
    if all_int and results["float"]:
      best_type = "int"

  converted = list(values)
  if best_type == "date":
    has_time = any(
      val.hour != 0 or val.minute != 0 or val.second != 0
      for _, val in results["date"]
    )
    date_fmt = "%Y-%m-%d %H:%M:%S" if has_time else "%Y-%m-%d"
    for idx, val in results["date"]:
      converted[idx] = val.strftime(date_fmt)
  elif best_type == "bool":
    for idx, val in results["bool"]:
      converted[idx] = 1 if val else 0
  elif best_type in ("percentage", "currency"):
    for idx, val in results[best_type]:
      converted[idx] = val
  elif best_type == "int":
    for idx, val in results["float"]:
      converted[idx] = int(val) if float(val) == int(val) else val
  elif best_type == "float":
    for idx, val in results["float"]:
      converted[idx] = round(float(val), 6)
  else:
    for i in range(len(converted)):
      if converted[i] is not None:
        converted[i] = str(converted[i]).strip()

  return best_type, converted


# ---------------------------------------------------------------------------
# Header detection (RowType classification)
# ---------------------------------------------------------------------------

class _RowType(Enum):
  EMPTY = "empty"
  TITLE = "title"
  HEADER = "header_candidate"
  DATA = "data"
  AMBIGUOUS = "ambiguous"


def _classify_row(row: list[Any], total_cols: int) -> _RowType:
  filled = [v for v in row if v is not None]
  n_filled = len(filled)
  if n_filled == 0:
    return _RowType.EMPTY

  unique_vals = {str(v).strip() for v in filled if str(v).strip()}
  n_unique = len(unique_vals)
  has_numeric = any(isinstance(v, (int, float)) and not isinstance(v, bool) for v in filled)
  is_all_text = all(isinstance(v, str) for v in filled)

  if n_unique <= 2 and is_all_text:
    if n_filled <= 2 or n_unique == 1:
      return _RowType.TITLE

  if has_numeric:
    return _RowType.DATA

  if is_all_text and n_unique >= 3:
    return _RowType.HEADER

  if is_all_text and n_unique >= 2 and n_filled >= 2:
    return _RowType.HEADER

  return _RowType.AMBIGUOUS


def _detect_header(grid: list[list[Any]]) -> tuple[list[int], int, str]:
  if not grid:
    return [], 0, "fallback"

  total_cols = max(len(r) for r in grid) if grid else 0
  scan_limit = min(len(grid), MAX_SCAN_ROWS)

  row_types = []
  for i in range(scan_limit):
    row = list(grid[i]) if i < len(grid) else []
    while len(row) < total_cols:
      row.append(None)
    row_types.append(_classify_row(row, total_cols))

  header_indices: list[int] = []
  i = 0

  while i < scan_limit and row_types[i] in (_RowType.EMPTY, _RowType.TITLE):
    i += 1

  while i < scan_limit and row_types[i] in (_RowType.HEADER, _RowType.AMBIGUOUS):
    if len(header_indices) >= MAX_HEADER_ROWS:
      break
    if row_types[i] == _RowType.HEADER:
      header_indices.append(i)
    elif row_types[i] == _RowType.AMBIGUOUS and header_indices:
      header_indices.append(i)
    i += 1

  if len(header_indices) >= MAX_HEADER_ROWS:
    header_indices = [header_indices[0]]
    i = header_indices[0] + 1

  while i < scan_limit and row_types[i] == _RowType.EMPTY:
    i += 1

  data_start = i if i < scan_limit else None

  if header_indices and data_start is not None and data_start > header_indices[-1]:
    return header_indices, data_start, "auto"

  first_data = next((idx for idx, rt in enumerate(row_types) if rt == _RowType.DATA), None)
  if first_data is not None and first_data > 0:
    candidate = first_data - 1
    while candidate > 0 and row_types[candidate] == _RowType.EMPTY:
      candidate -= 1
    if row_types[candidate] != _RowType.EMPTY:
      return [candidate], first_data, "fallback"

  if len(grid) > 1:
    return [0], 1, "fallback"
  return [], 0, "fallback"


def _merge_header_rows(grid: list[list[Any]], header_indices: list[int]) -> list[str | None]:
  if not header_indices:
    return []
  total_cols = max(len(r) for r in grid) if grid else 0
  merged = []
  for col in range(total_cols):
    parts = []
    for row_idx in header_indices:
      if row_idx < len(grid) and col < len(grid[row_idx]):
        val = grid[row_idx][col]
        if val is not None:
          s = str(val).strip()
          if s and s not in parts:
            parts.append(s)
    merged.append(" - ".join(parts) if parts else None)
  return merged


# ---------------------------------------------------------------------------
# Grid normalization helpers
# ---------------------------------------------------------------------------

def _drop_empty_rows_cols(grid: list[list[Any]]) -> list[list[Any]]:
  if not grid:
    return grid
  non_empty = [r for r in grid if any(v is not None and str(v).strip() != "" for v in r)]
  if not non_empty:
    return []
  n_cols = max(len(r) for r in non_empty)
  empty_cols = {
    c for c in range(n_cols)
    if all(c >= len(r) or r[c] is None or str(r[c]).strip() == "" for r in non_empty)
  }
  if not empty_cols:
    return non_empty
  result = []
  for row in non_empty:
    new_row = [v for i, v in enumerate(row) if i not in empty_cols]
    while len(new_row) < n_cols - len(empty_cols):
      new_row.append(None)
    result.append(new_row)
  return result


def _resolve_ditto_marks(grid: list[list[Any]]) -> list[list[Any]]:
  if not grid or len(grid) < 2:
    return grid
  n_cols = max(len(r) for r in grid)
  for r in range(1, len(grid)):
    for c in range(min(n_cols, len(grid[r]))):
      val = grid[r][c]
      if isinstance(val, str) and val.strip() in ('"', "'", "nt", "nt.", "như trên"):
        for above in range(r - 1, -1, -1):
          if above < len(grid) and c < len(grid[above]):
            above_val = grid[above][c]
            if above_val is not None and (
              not isinstance(above_val, str)
              or above_val.strip() not in ('"', "'", "nt", "nt.", "như trên")
            ):
              grid[r][c] = above_val
              break
  return grid


def _replace_excel_errors(grid: list[list[Any]]) -> list[list[Any]]:
  for r in range(len(grid)):
    for c in range(len(grid[r])):
      v = grid[r][c]
      if isinstance(v, str) and v.strip().lower() in _EXCEL_ERRORS:
        grid[r][c] = None
  return grid


def _normalize_na(grid: list[list[Any]]) -> list[list[Any]]:
  """Replace common NA strings with None."""
  for r in range(len(grid)):
    for c in range(len(grid[r])):
      v = grid[r][c]
      if isinstance(v, str) and v.strip().lower() in _NA_STRINGS:
        grid[r][c] = None
  return grid


def _unicode_normalize(grid: list[list[Any]]) -> list[list[Any]]:
  for r in range(len(grid)):
    for c in range(len(grid[r])):
      if isinstance(grid[r][c], str):
        grid[r][c] = unicodedata.normalize("NFC", grid[r][c])
  return grid


def _whitespace_cleanup(grid: list[list[Any]]) -> list[list[Any]]:
  for r in range(len(grid)):
    for c in range(len(grid[r])):
      if isinstance(grid[r][c], str):
        s = grid[r][c].strip()
        # s = re.sub(r"\s+", " ", grid[r][c]).strip()
        grid[r][c] = s if s else None
  return grid


def _clean_column_name(name: str | None, idx: int) -> str:
  if name is None or str(name).strip() == "":
    return f"col_{idx}"
  s = unicodedata.normalize("NFC", str(name).strip().lower())
  s = re.sub(r"\s+", "_", s)
  s = re.sub(r"[^\w]", "_", s)
  s = re.sub(r"_+", "_", s).strip("_")
  return s if s else f"col_{idx}"


def _ensure_unique_names(names: list[str]) -> list[str]:
  seen: dict[str, int] = {}
  result = []
  for name in names:
    if name in seen:
      seen[name] += 1
      result.append(f"{name}_{seen[name]}")
    else:
      seen[name] = 1
      result.append(name)
  return result


# ---------------------------------------------------------------------------
# CSV reading
# ---------------------------------------------------------------------------

def _detect_encoding(csv_path: Path) -> str:
  with csv_path.open("rb") as f:
    raw = f.read(65536)
  result = chardet.detect(raw)
  enc = result.get("encoding") or "utf-8"
  # Normalize BOM variants
  return enc.lower().replace("-sig", "").replace("utf-8", "utf-8")


def _detect_delimiter(csv_path: Path, encoding: str) -> str:
  try:
    with csv_path.open("r", encoding=encoding, errors="replace", newline="") as fh:
      sample = fh.read(8192)
    dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
    return dialect.delimiter
  except csv.Error:
    return ","


def _read_raw_grid(csv_path: Path) -> list[list[Any]]:
  """Read CSV into a raw grid trying multiple encodings."""
  encodings_to_try = ["utf-8-sig", "utf-8", "cp1252", "latin-1"]
  # Prepend chardet guess
  detected = _detect_encoding(csv_path)
  encodings_to_try = [detected] + [e for e in encodings_to_try if e != detected]

  last_err: Exception | None = None
  for enc in encodings_to_try:
    try:
      delim = _detect_delimiter(csv_path, enc)
      with csv_path.open("r", encoding=enc, errors="replace", newline="") as fh:
        reader = csv.reader(fh, delimiter=delim)
        rows = [list(row) for row in reader]

      if not rows:
        return []

      # Pad all rows to same width
      n_cols = max(len(r) for r in rows)
      for row in rows:
        while len(row) < n_cols:
          row.append(None)

      # Convert empty strings → None
      for r in rows:
        for c in range(len(r)):
          if r[c] == "":
            r[c] = None

      return rows
    except (UnicodeDecodeError, OSError) as e:
      last_err = e
      continue

  raise RuntimeError(f"Cannot read {csv_path.name}: {last_err}")


# ---------------------------------------------------------------------------
# Leading-zero protection
# ---------------------------------------------------------------------------

def _has_leading_zero_values(col_values: list[Any]) -> bool:
  """Return True if any non-null value looks like a leading-zero string."""
  for v in col_values:
    if v is not None and isinstance(v, str) and _LEADING_ZERO_RE.match(v.strip()):
      return True
  return False


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def clean_csv(csv_path: Path) -> pd.DataFrame:
  """Full cleaning pipeline for a single CSV file.

  Steps:
  1. Read raw grid (encoding + delimiter detection)
  2. Drop empty rows/cols
  3. Header detection
  4. Column name cleaning
  5. Ditto mark resolution
  6. Unicode normalization + whitespace cleanup
  7. Excel error replacement + NA normalization
  8. Type detection per column (with leading-zero protection)
  9. Drop duplicate rows

  Returns cleaned DataFrame.
  """
  grid = _read_raw_grid(csv_path)
  if not grid:
    return pd.DataFrame()

  # 2. Drop empty rows/cols
  grid = _drop_empty_rows_cols(grid)
  if len(grid) < 2:
    return pd.DataFrame()

  # 3. Header detection
  header_indices, data_start, _ = _detect_header(grid)

  if header_indices:
    col_names = _merge_header_rows(grid, header_indices)
  else:
    col_names = [None] * max(len(r) for r in grid)

  data_rows = [list(r) for r in grid[data_start:]]
  if not data_rows:
    return pd.DataFrame()

  # 4. Column name cleaning
  n_cols = max(len(r) for r in data_rows)
  while len(col_names) < n_cols:
    col_names.append(None)
  col_names = col_names[:n_cols]

  cleaned_names = [_clean_column_name(name, i) for i, name in enumerate(col_names)]
  cleaned_names = _ensure_unique_names(cleaned_names)

  # Pad rows
  for row in data_rows:
    while len(row) < n_cols:
      row.append(None)

  # 5. Ditto marks
  data_rows = _resolve_ditto_marks(data_rows)

  # 6. Unicode + whitespace
  data_rows = _unicode_normalize(data_rows)
  data_rows = _whitespace_cleanup(data_rows)

  # 7. Excel errors + NA
  data_rows = _replace_excel_errors(data_rows)
  data_rows = _normalize_na(data_rows)

  # 8. Type detection per column (with leading-zero protection)
  col_types: dict[str, str] = {}
  for col_idx, col_name in enumerate(cleaned_names):
    col_values = [row[col_idx] if col_idx < len(row) else None for row in data_rows]

    # Protect leading-zero identifiers
    if _has_leading_zero_values(col_values):
      col_types[col_name] = "text"
      for row_idx in range(len(data_rows)):
        if col_idx < len(data_rows[row_idx]) and data_rows[row_idx][col_idx] is not None:
          data_rows[row_idx][col_idx] = str(data_rows[row_idx][col_idx])
      continue

    type_str, converted = _detect_column_type(col_values, col_name)
    col_types[col_name] = type_str
    for row_idx, val in enumerate(converted):
      if col_idx < len(data_rows[row_idx]):
        data_rows[row_idx][col_idx] = val

  # 9. Build DataFrame + dedup
  df = pd.DataFrame(data_rows, columns=cleaned_names)
  df = df.drop_duplicates().reset_index(drop=True)

  # 10. Explicit dtype cast (pandas 3.x doesn't auto-infer from object arrays)
  for col_name, type_str in col_types.items():
    if col_name not in df.columns:
      continue
    try:
      if type_str == "int":
        df[col_name] = pd.to_numeric(df[col_name], errors="coerce").astype("Int64")
      elif type_str in ("float", "percentage", "currency"):
        df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
      elif type_str == "bool":
        df[col_name] = df[col_name].astype("Int64")
    except Exception:
      pass  # Leave as-is if conversion fails

  return df
