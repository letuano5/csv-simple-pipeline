#!/usr/bin/env python3
"""CSV → SQLite → Text-to-SQL pipeline CLI.

Usage:
  uv run main.py --model claude
  uv run main.py --model gemini
  uv run main.py --model openai
  uv run main.py --model all
  uv run main.py --model claude --fresh --minibatch 20 --limit 50
  uv run main.py --eval --model claude  (evaluation only)
  uv run main.py --convert-only        (only CSV → SQLite)

Environment variables (in .env):
  ANTHROPIC_API_KEY  — for Claude
  GEMINI_API_KEY     — for Gemini
  OPENAI_API_KEY     — for OpenAI
  CLAUDE_MODEL       — override model (default: claude-haiku-4-5-20251001)
  GEMINI_MODEL       — override model (default: gemini-2.5-flash)
  OPENAI_MODEL       — override model (default: gpt-4o-mini)
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
  level=logging.INFO,
  format="%(asctime)s [%(levelname)s] %(message)s",
  datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

ROOT = Path(__file__).parent
INPUT_DIR = ROOT / "input"
CSV_DIR = INPUT_DIR / "csv"
QUESTIONS_PATH = INPUT_DIR / "questions.json"
INPUT_SQLITE_DIR = INPUT_DIR / "sqlite"
SQLITE_DIR = ROOT / "output" / "sqlite"
OUTPUT_DIR = ROOT / "output"
PROMPT_PATH = ROOT / "prompt.txt"

SUPPORTED_MODELS = ["claude", "gemini", "openai", 'qwen2.5-coder-7b', 'qwen3.5-coder', 'claude-sonnet-4-6']


def cmd_run(args: argparse.Namespace) -> None:
  from src.pipeline import run_pipeline

  models = SUPPORTED_MODELS if args.model == "all" else [args.model]
  for model in models:
    log.info("=== Running pipeline for model: %s ===", model)
    try:
      output_path = run_pipeline(
        model=model,
        questions_path=QUESTIONS_PATH,
        csv_dir=CSV_DIR,
        sqlite_dir=SQLITE_DIR,
        output_dir=OUTPUT_DIR,
        minibatch_size=args.minibatch,
        fresh=args.fresh,
        prompt_path=PROMPT_PATH,
        limit=args.limit,
        concurrent=args.concurrent,
      )
      log.info("Done: %s", output_path)
    except Exception as e:
      log.error("Pipeline failed for %s: %s", model, e)
      if args.debug:
        raise


def cmd_convert_only(args: argparse.Namespace) -> None:
  from src.converter import convert_to_sqlite

  questions = json.loads(QUESTIONS_PATH.read_text(encoding="utf-8"))
  csv_files = sorted({
    q.get("csv_file", q["db_id"] + ".csv") for q in questions
  })
  log.info("Converting %d unique CSV files...", len(csv_files))

  for csv_file in csv_files:
    csv_path = CSV_DIR / csv_file
    if not csv_path.exists():
      log.warning("CSV not found: %s", csv_path)
      continue
    try:
      sqlite_path, table_name = convert_to_sqlite(csv_path, SQLITE_DIR)
      log.info("OK: %s → %s (table: %s)", csv_file, sqlite_path.name, table_name)
    except Exception as e:
      log.error("FAIL: %s — %s", csv_file, e)


def _normalize_reparsed_predictions(reparsed: list[dict], questions: list[dict]) -> list[dict]:
  """Convert .reparsed.json format → standard evaluator format.

  reparsed entries have:  index, db_id, result (JSON string), sql
  standard format needs:  instance_id, exec_answer (list[list]), sql_answer
  """
  index_to_iid: dict[int, str] = {}
  for i, q in enumerate(questions):
    q_idx = q.get("index")
    if q_idx is not None:
      iid = q.get("instance_id") or f"{q.get('db_id', '')}_{i}"
      index_to_iid[q_idx] = iid

  result = []
  for entry in reparsed:
    idx = entry.get("index")
    iid = index_to_iid.get(idx) if idx is not None else None
    if iid is None:
      iid = f"{entry.get('db_id', 'unknown')}_{idx}"

    raw = entry.get("result", "[]")
    try:
      exec_answer = json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError):
      exec_answer = {"error": "unparseable result"}

    # Wrap flat list (single-column results) into list of single-element lists
    if isinstance(exec_answer, list) and exec_answer and not isinstance(exec_answer[0], (list, dict)):
      exec_answer = [[item] for item in exec_answer]

    # Pad ragged rows to uniform width
    if isinstance(exec_answer, list) and exec_answer and isinstance(exec_answer[0], list):
      max_cols = max(len(row) for row in exec_answer)
      exec_answer = [row + [None] * (max_cols - len(row)) for row in exec_answer]

    result.append({
      "instance_id": iid,
      "sql_answer": entry.get("sql", ""),
      "exec_answer": exec_answer,
    })
  return result


def cmd_eval(args: argparse.Namespace) -> None:
  from src.evaluator import execution_accuracy
  from src.executor import execute_sql

  models = SUPPORTED_MODELS if args.model == "all" else [args.model]

  questions = json.loads(QUESTIONS_PATH.read_text(encoding="utf-8"))
  if args.limit:
    questions = questions[:args.limit]

  # Execute gold SQL from questions.json to build gold_list
  gold_list: list[dict] = []
  gold_errors: list[str] = []
  gold_missing_sql = 0
  for q in questions:
    iid = q.get("instance_id") or f"{q.get('db_id', '')}_{questions.index(q)}"
    gold_sql = q.get("sql") or q.get("sql_gold") or q.get("query") or q.get("gold_sql", "")
    if not gold_sql:
      gold_missing_sql += 1
      continue
    db_id = q.get("db_id", "")
    sqlite_path = INPUT_SQLITE_DIR / f"{db_id}.sqlite"
    exec_answer = execute_sql(sqlite_path, gold_sql)
    if isinstance(exec_answer, dict) and "error" in exec_answer:
      gold_errors.append(f"  {iid}: {exec_answer['error']}")
    gold_list.append({"instance_id": iid, "exec_answer": exec_answer})

  if not gold_list:
    log.error("No gold SQL found in questions.json (expected field: sql, sql_gold, query, or gold_sql)")
    sys.exit(1)

  if gold_missing_sql:
    log.warning("Gold: %d questions skipped (no SQL field)", gold_missing_sql)
  if gold_errors:
    log.warning("Gold execution errors: %d/%d", len(gold_errors), len(gold_list))
    for line in gold_errors[:10]:
      log.warning(line)
    if len(gold_errors) > 10:
      log.warning("  ... and %d more", len(gold_errors) - 10)
  log.info("Gold executed: %d questions (%d errors)", len(gold_list), len(gold_errors))

  for model in models:
    pred_path = OUTPUT_DIR / f"{model}.json"
    reparsed_path = OUTPUT_DIR / f"{model}.reparsed.json"

    if pred_path.exists():
      predictions = json.loads(pred_path.read_text(encoding="utf-8"))
      log.info("[%s] Loaded %d predictions", model, len(predictions))
    elif reparsed_path.exists():
      log.info("[%s] Using reparsed format: %s", model, reparsed_path)
      raw = json.loads(reparsed_path.read_text(encoding="utf-8"))
      predictions = _normalize_reparsed_predictions(raw, questions)
      log.info("[%s] Loaded %d reparsed predictions", model, len(predictions))
    else:
      log.warning("No predictions found for %s: %s", model, pred_path)
      continue

    # Check instance_id overlap
    gold_ids = {g["instance_id"] for g in gold_list}
    pred_ids = {p["instance_id"] for p in predictions}
    matched = gold_ids & pred_ids
    log.info("[%s] instance_id overlap: %d/%d gold matched", model, len(matched), len(gold_ids))
    if len(matched) == 0:
      log.error("[%s] No matching instance_ids! Gold sample: %s", model, sorted(gold_ids)[:3])
      log.error("[%s] Pred sample: %s", model, sorted(pred_ids)[:3])
      continue

    result = execution_accuracy(predictions, gold_list)

    # Breakdown: errors vs wrong answers
    pred_errors = [d for d in result["details"] if d["error"] and not d["correct"]]
    wrong_no_error = [d for d in result["details"] if not d["correct"] and not d["error"]]
    log.info(
      "[%s] correct=%d  pred_errors=%d  wrong_results=%d  no_gold=%d",
      model,
      result["correct"],
      len(pred_errors),
      len(wrong_no_error),
      len([d for d in result["details"] if d["error"] == "no gold found"]),
    )
    if pred_errors:
      log.warning("[%s] First 5 prediction errors:", model)
      for d in pred_errors[:5]:
        log.warning("  %s: %s", d["instance_id"], d["error"])
    if wrong_no_error:
      pred_map = {p["instance_id"]: p for p in predictions}
      gold_map = {g["instance_id"]: g for g in gold_list}
      log.info("[%s] First 5 wrong (no error):", model)
      for d in wrong_no_error[:5]:
        iid = d["instance_id"]
        pred_entry = pred_map.get(iid, {})
        gold_entry = gold_map.get(iid, {})
        log.info("  [%s]", iid)
        log.info("    SQL   : %s", pred_entry.get("sql_answer", "<missing>"))
        log.info("    SQLG  : %s", pred_entry.get("sql", "<missing>"))
        log.info("    Pred  : %s", pred_entry.get("exec_answer"))
        log.info("    Gold  : %s", gold_entry.get("exec_answer"))

    print(
      f"[{model}] Execution Accuracy: {result['score']:.4f} "
      f"({result['correct']}/{result['total']})"
    )


def build_parser() -> argparse.ArgumentParser:
  parser = argparse.ArgumentParser(
    description="CSV → SQLite → Text-to-SQL pipeline",
    formatter_class=argparse.RawDescriptionHelpFormatter,
  )
  parser.add_argument(
    "--model",
    choices=SUPPORTED_MODELS + ["all"],
    default="claude",
    help="LLM backend to use (default: claude)",
  )
  parser.add_argument(
    "--minibatch",
    type=int,
    default=50,
    help="Number of questions per LLM batch (default: 50)",
  )
  parser.add_argument(
    "--fresh",
    action="store_true",
    help="Ignore checkpoint and start from scratch",
  )
  parser.add_argument(
    "--limit",
    type=int,
    default=None,
    metavar="N",
    help="Only process the first N questions",
  )
  parser.add_argument(
    "--concurrent",
    action="store_true",
    help="Use concurrent single-request API calls instead of batch API",
  )
  parser.add_argument(
    "--eval",
    action="store_true",
    dest="eval_only",
    help="Only evaluate existing predictions (requires input/gold.json)",
  )
  parser.add_argument(
    "--convert-only",
    action="store_true",
    dest="convert_only",
    help="Only convert CSVs to SQLite, skip LLM inference",
  )
  parser.add_argument(
    "--debug",
    action="store_true",
    help="Re-raise exceptions (for debugging)",
  )
  return parser


def main() -> None:
  parser = build_parser()
  args = parser.parse_args()

  if not QUESTIONS_PATH.exists():
    log.error("questions.json not found: %s", QUESTIONS_PATH)
    log.error("Create input/questions.json with your questions first.")
    sys.exit(1)

  if args.convert_only:
    cmd_convert_only(args)
  elif args.eval_only:
    cmd_eval(args)
  else:
    cmd_run(args)


if __name__ == "__main__":
  main()
