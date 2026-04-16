"""Execution accuracy evaluation (Spider2 style).

Adapted from Spider2's compare_pandas_table with pipeline.md adjustments:
  - Float tolerance: 1e-6 (stricter than Spider2's 1e-2)
  - Column subset check: all gold columns must appear in predicted result
  - Sort both sides if SQL has no ORDER BY
  - NULL == NULL → True

Public API:
  compare_results(pred_rows, gold_rows, *, ignore_order=True) -> bool
  execution_accuracy(predictions, gold_list) -> dict
"""

from __future__ import annotations

import math
import re
from typing import Any


_ORDER_BY_RE = re.compile(r"\bORDER\s+BY\b", re.IGNORECASE)
_TOLERANCE = 1e-2


def _values_equal(a: Any, b: Any) -> bool:
  """Compare two scalar values with NULL and float tolerance."""
  # NULL == NULL
  if a is None and b is None:
    return True
  if a is None or b is None:
    return False
  # Numeric tolerance
  if isinstance(a, (int, float)) and isinstance(b, (int, float)):
    return math.isclose(float(a), float(b), abs_tol=_TOLERANCE, rel_tol=1e-9)
  return a == b


def _sort_key(x: Any):
  return (x is None, str(x) if x is not None else "", isinstance(x, (int, float)))


def _vectors_match(v1: list, v2: list, *, ignore_order: bool) -> bool:
  if len(v1) != len(v2):
    return False
  if ignore_order:
    v1 = sorted(v1, key=_sort_key)
    v2 = sorted(v2, key=_sort_key)
  return all(_values_equal(a, b) for a, b in zip(v1, v2))


def compare_results(
  pred_rows: list[list[Any]],
  gold_rows: list[list[Any]],
  *,
  ignore_order: bool = True,
  sql: str | None = None,
) -> bool:
  """Compare predicted and gold result sets.

  Uses column-subset logic: every column in gold must match some column in pred.
  If sql contains ORDER BY, ordering is respected (ignore_order=False).

  Args:
    pred_rows:    Predicted query results (list of rows).
    gold_rows:    Gold query results (list of rows).
    ignore_order: Whether to ignore row ordering (overridden by SQL analysis).
    sql:          The predicted SQL query (used to detect ORDER BY).

  Returns:
    True if results match, False otherwise.
  """
  # Detect ORDER BY in SQL → preserve order
  if sql and _ORDER_BY_RE.search(sql):
    ignore_order = False

  if not gold_rows and not pred_rows:
    return True
  if not gold_rows or not pred_rows:
    return False

  # Transpose to column vectors
  n_gold_cols = len(gold_rows[0])
  n_pred_cols = len(pred_rows[0]) if pred_rows else 0

  gold_cols = [[row[c] for row in gold_rows] for c in range(n_gold_cols)]
  pred_cols = [[row[c] for row in pred_rows] for c in range(n_pred_cols)]

  # Every gold column must match some pred column
  for gold_col in gold_cols:
    if not any(_vectors_match(gold_col, pred_col, ignore_order=ignore_order) for pred_col in pred_cols):
      return False

  return True


def execution_accuracy(
  predictions: list[dict],
  gold_list: list[dict],
) -> dict:
  """Compute execution accuracy over a list of predictions.

  Each prediction dict must have:
    instance_id, exec_answer (list[list] or {"error": ...}), sql_answer (str)

  Each gold dict must have:
    instance_id, exec_answer (list[list])

  Returns:
    {
      "score": float,
      "correct": int,
      "total": int,
      "details": [{"instance_id": ..., "correct": bool, "error": ...}]
    }
  """
  gold_map = {g["instance_id"]: g for g in gold_list}
  details = []
  correct = 0

  for pred in predictions:
    iid = pred["instance_id"]
    gold = gold_map.get(iid)
    if gold is None:
      details.append({"instance_id": iid, "correct": False, "error": "no gold found"})
      continue

    pred_exec = pred.get("exec_answer")
    gold_exec = gold.get("exec_answer")

    if isinstance(pred_exec, dict) and "error" in pred_exec:
      details.append({"instance_id": iid, "correct": False, "error": pred_exec["error"]})
      continue

    if not isinstance(pred_exec, list) or not isinstance(gold_exec, list):
      details.append({"instance_id": iid, "correct": False, "error": "invalid exec_answer format"})
      continue

    sql = pred.get("sql_answer", "")
    ok = compare_results(pred_exec, gold_exec, sql=sql)
    if ok:
      correct += 1
    details.append({"instance_id": iid, "correct": ok, "error": None})

  total = len(predictions)
  score = correct / total if total > 0 else 0.0
  return {"score": score, "correct": correct, "total": total, "details": details}
