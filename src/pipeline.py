"""Main pipeline orchestrator.

Flow:
  1. Load input/questions.json
  2. Convert each CSV to SQLite (idempotent)
  3. Resume from checkpoint (skip already-processed instance_ids)
  4. Split remaining questions into minibatches
  5. For each minibatch:
     a. Build M-Schema prompt per question
     b. Call LLM backend batch
     c. Execute each returned SQL on SQLite
     d. Append results + checkpoint
  6. Write final output/<model>.json

Public API:
  run_pipeline(model, questions_path, csv_dir, sqlite_dir, output_dir,
               minibatch_size, fresh) -> Path
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING

from tqdm import tqdm

from src.converter import convert_to_sqlite
from src.executor import execute_sql
from src.schema import build_m_schema, get_table_name

if TYPE_CHECKING:
  from src.inference.base import LLMBackend

log = logging.getLogger(__name__)

# Prompt template (loaded once)
_PROMPT_TEMPLATE: str | None = None


def _load_prompt_template(prompt_path: Path) -> str:
  global _PROMPT_TEMPLATE
  if _PROMPT_TEMPLATE is None:
    _PROMPT_TEMPLATE = prompt_path.read_text(encoding="utf-8")
  return _PROMPT_TEMPLATE


def _build_prompt(template: str, schema: str, question: str, evidence: str) -> str:
  """Fill the prompt template with schema, evidence, and question."""
  prompt = template
  prompt = prompt.replace("{DATABASE SCHEMA}", schema)
  # Evidence: if empty, omit the label; if present, prepend label
  if evidence and evidence.strip():
    evidence_block = f"Evidence: {evidence.strip()}\n"
  else:
    evidence_block = ""
  prompt = prompt.replace("{EVIDENCE}", evidence_block)
  prompt = prompt.replace("{QUESTION}", question)
  return prompt


def _load_checkpoint(checkpoint_path: Path) -> tuple[set[str], list[dict], dict | None]:
  """Load existing results from checkpoint. Returns (done_ids, results, pending_batch | None)."""
  if not checkpoint_path.exists():
    return set(), [], None
  try:
    data = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    done = set(data.get("done", []))
    results = data.get("results", [])
    # pending_batch holds the in-flight batch_id so the run can be resumed after a crash or SIGINT
  pending_batch = data.get("pending_batch")  # {"batch_id": ..., "instance_ids": [...]}
    return done, results, pending_batch
  except Exception:
    return set(), [], None


def _save_checkpoint(
  checkpoint_path: Path,
  done_ids: set[str],
  results: list[dict],
  *,
  pending_batch: dict | None = None,
) -> None:
  checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
  data: dict = {"done": sorted(done_ids), "results": results}
  if pending_batch:
    data["pending_batch"] = pending_batch
  checkpoint_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _save_output(output_path: Path, results: list[dict]) -> None:
  output_path.parent.mkdir(parents=True, exist_ok=True)
  output_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")


def _normalize_questions(questions: list[dict]) -> list[dict]:
  """Normalize questions to the internal format expected by the pipeline.

  Handles input that uses ``db_id`` / ``external_knowledge`` (new format) as
  well as input that already has ``csv_file`` / ``evidence`` / ``instance_id``
  (legacy format).  Fields are added in-place on copies so the originals are
  not mutated.
  """
  normalized = []
  for i, q in enumerate(questions):
    q = dict(q)  # shallow copy — don't mutate caller's data
    # csv_file: derive from db_id when missing
    if "csv_file" not in q:
      q["csv_file"] = q["db_id"] + ".csv"
    # evidence: alias from external_knowledge when missing
    if "evidence" not in q:
      q["evidence"] = q.get("external_knowledge", "")
    # instance_id: generate from db_id + position when missing
    if "instance_id" not in q:
      q["instance_id"] = f"{q['db_id']}_{i}"
    normalized.append(q)
  return normalized


def _process_llm_results(
  llm_results: list[dict],
  minibatch: list[dict],
  accumulated_results: list[dict],
  done_ids: set[str],
) -> None:
  """Map LLM results back to original request info, execute SQL, append to accumulated_results."""
  req_map = {r["instance_id"]: r for r in minibatch}

  for llm_result in llm_results:
    iid = llm_result["instance_id"]
    req = req_map.get(iid)
    if req is None:
      continue

    q = req["question"]
    sqlite_path = req["sqlite_path"]
    sql = llm_result.get("sql", "")
    error = llm_result.get("error")

    exec_answer = execute_sql(sqlite_path, sql) if (sql and not error) else {"error": error or "no SQL generated"}

    result = {
      **{k: v for k, v in q.items()},
      "sqlite_converted_path": str(sqlite_path),
      "sql_answer": sql,
      "exec_answer": exec_answer,
    }
    if error:
      result["llm_error"] = error

    accumulated_results.append(result)
    done_ids.add(iid)


def run_pipeline(
  model: str,
  questions_path: Path,
  csv_dir: Path,
  sqlite_dir: Path,
  output_dir: Path,
  *,
  minibatch_size: int = 50,
  fresh: bool = False,
  prompt_path: Path | None = None,
  limit: int | None = None,
  concurrent: bool = False,
) -> Path:
  """Run the full text-to-SQL pipeline for one model.

  Args:
    model:           One of "claude", "gemini", "openai".
    questions_path:  Path to input/questions.json.
    csv_dir:         Directory containing CSV files.
    sqlite_dir:      Directory to write .sqlite files.
    output_dir:      Directory to write output JSON.
    minibatch_size:  Number of questions per LLM batch.
    fresh:           If True, ignore checkpoint and start over.
    prompt_path:     Path to prompt template (default: prompt.txt next to questions).
    limit:           Cap processing to the first N questions (None = all).

  Returns:
    Path to the output JSON file.
  """
  # Load questions
  questions = json.loads(questions_path.read_text(encoding="utf-8"))
  questions = _normalize_questions(questions)
  if limit is not None:
    questions = questions[:limit]
    log.info("Loaded %d questions (limit=%d)", len(questions), limit)
  else:
    log.info("Loaded %d questions", len(questions))

  # Resolve prompt template
  if prompt_path is None:
    prompt_path = questions_path.parent.parent / "prompt.txt"
  template = _load_prompt_template(prompt_path)

  # Step 1: Convert all CSVs to SQLite (idempotent)
  log.info("Converting CSVs to SQLite...")
  sqlite_map: dict[str, tuple[Path, str]] = {}  # csv_file → (sqlite_path, table_name)
  for q in questions:
    csv_file = q["csv_file"]
    if csv_file in sqlite_map:
      continue
    csv_path = csv_dir / csv_file
    if not csv_path.exists():
      log.warning("CSV not found: %s", csv_path)
      sqlite_map[csv_file] = (Path(""), "")
      continue
    try:
      sqlite_path, table_name = convert_to_sqlite(csv_path, sqlite_dir)
      sqlite_map[csv_file] = (sqlite_path, table_name)
    except Exception as e:
      log.error("Failed to convert %s: %s", csv_file, e)
      sqlite_map[csv_file] = (Path(""), "")

  # Step 2: Load checkpoint
  checkpoint_path = output_dir / "checkpoint" / f"{model}_checkpoint.json"
  if fresh and checkpoint_path.exists():
    checkpoint_path.unlink()

  done_ids, accumulated_results, pending_batch = _load_checkpoint(checkpoint_path)
  log.info("Checkpoint: %d already done", len(done_ids))
  if pending_batch:
    log.info("Found pending batch: %s (%d requests)", pending_batch["batch_id"], len(pending_batch["instance_ids"]))

  # Filter remaining questions
  remaining = [q for q in questions if q["instance_id"] not in done_ids]
  log.info("%d questions remaining", len(remaining))

  if not remaining:
    log.info("All questions already processed. Writing output.")
    output_path = output_dir / f"{model}.json"
    _save_output(output_path, accumulated_results)
    return output_path

  # Step 3: Build prompts
  prompts: list[dict] = []
  for q in remaining:
    csv_file = q["csv_file"]
    sqlite_path, table_name = sqlite_map.get(csv_file, (Path(""), ""))
    if not sqlite_path.exists():
      log.warning("Skipping %s: SQLite not available", q["instance_id"])
      # Add error result
      accumulated_results.append({
        **q,
        "sqlite_converted_path": "",
        "sql_answer": "",
        "exec_answer": {"error": "SQLite conversion failed"},
      })
      done_ids.add(q["instance_id"])
      continue

    try:
      schema = build_m_schema(sqlite_path, table_name)
    except Exception as e:
      log.warning("Schema build failed for %s: %s", csv_file, e)
      schema = f"(schema unavailable: {e})"

    prompt = _build_prompt(template, schema, q["question"], q.get("evidence", ""))
    prompts.append({
      "instance_id": q["instance_id"],
      "prompt": prompt,
      "csv_file": csv_file,
      "question": q,
      "sqlite_path": sqlite_path,
    })

  # Step 4: Select backend
  backend = _get_backend(model, concurrent=concurrent)

  # Step 5: Resume pending batch if any (only for async-capable backends)
  if pending_batch and backend.supports_async_batch:
    pending_ids = set(pending_batch["instance_ids"])
    pending_reqs = [r for r in prompts if r["instance_id"] in pending_ids]
    if pending_reqs:
      log.info("Resuming pending batch %s (%d requests)…", pending_batch["batch_id"], len(pending_reqs))
      try:
        llm_results = backend.collect_batch(pending_batch["batch_id"], pending_reqs)
        _process_llm_results(llm_results, pending_reqs, accumulated_results, done_ids)
        pending_batch = None  # cleared — save below without pending_batch key
        _save_checkpoint(checkpoint_path, done_ids, accumulated_results)
        log.info("Pending batch collected. Checkpoint saved: %d done", len(done_ids))
      except Exception as e:
        log.error("Failed to collect pending batch, will re-submit: %s", e)
        pending_batch = None  # drop stale batch_id; ids stay in prompts → re-submitted

    # Remove already-collected ids from prompts
    done_prompt_ids = done_ids
    prompts = [r for r in prompts if r["instance_id"] not in done_prompt_ids]

  # Step 6: Process remaining prompts in minibatches
  minibatches = [prompts[i:i + minibatch_size] for i in range(0, len(prompts), minibatch_size)]
  log.info("Processing %d minibatches (size=%d)", len(minibatches), minibatch_size)

  for batch_idx, minibatch in enumerate(tqdm(minibatches, desc=f"{model} batches")):
    log.info("Batch %d/%d (%d requests)", batch_idx + 1, len(minibatches), len(minibatch))
    raw_reqs = [{"instance_id": r["instance_id"], "prompt": r["prompt"]} for r in minibatch]

    try:
      if backend.supports_async_batch:
        # Save batch_id to checkpoint BEFORE polling — enables resume on interrupt
        batch_id = backend.submit_batch(raw_reqs)
        _save_checkpoint(
          checkpoint_path, done_ids, accumulated_results,
          pending_batch={"batch_id": batch_id, "instance_ids": [r["instance_id"] for r in minibatch]},
        )
        llm_results = backend.collect_batch(batch_id, raw_reqs)
      else:
        llm_results = backend.run_batch(raw_reqs)
    except Exception as e:
      log.error("Batch %d failed: %s", batch_idx + 1, e)
      llm_results = [
        {"instance_id": r["instance_id"], "sql": "", "raw_response": "", "error": str(e)}
        for r in minibatch
      ]

    _process_llm_results(llm_results, minibatch, accumulated_results, done_ids)
    _save_checkpoint(checkpoint_path, done_ids, accumulated_results)  # no pending_batch = cleared
    log.info("Checkpoint saved: %d done", len(done_ids))

  # Write final output
  output_path = output_dir / f"{model}.json"
  _save_output(output_path, accumulated_results)
  log.info("Output written: %s (%d results)", output_path, len(accumulated_results))
  return output_path


def _get_backend(model: str, *, concurrent: bool = False) -> "LLMBackend":
  """Instantiate the appropriate LLM backend."""
  if model == "claude":
    from src.inference.claude import ClaudeBackend
    return ClaudeBackend(concurrent=concurrent)
  elif model == "gemini":
    from src.inference.gemini import GeminiBackend
    return GeminiBackend(concurrent=concurrent)
  elif model == "openai":
    from src.inference.openai_inf import OpenAIBackend
    return OpenAIBackend(concurrent=concurrent)
  else:
    raise ValueError(f"Unknown model: {model!r}. Choose: claude, gemini, openai")
