"""Abstract base class for LLM inference backends."""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed

_SQL_RE = re.compile(r"```(?:sql)?\s*(.*?)\s*```", re.DOTALL | re.IGNORECASE)


def extract_sql(text: str) -> str:
  """Extract SQL from a markdown code block, or return the full text."""
  if not text:
    return ""
  match = _SQL_RE.search(text)
  if match:
    sql = match.group(1).strip()
    # Strip leading comment line like "-- Your SQL query"
    lines = [ln for ln in sql.splitlines() if not ln.strip().startswith("--") or ln.strip().upper().startswith("--")]
    # Keep comment lines that are actual SQL comments (not just placeholder)
    final_lines = []
    for ln in sql.splitlines():
      stripped = ln.strip()
      if stripped.startswith("--") and not any(
        kw in stripped.upper()
        for kw in ("SELECT", "FROM", "WHERE", "GROUP", "ORDER", "HAVING", "LIMIT", "WITH", "JOIN")
      ):
        # Likely a placeholder comment like "-- Your SQL query" — skip
        placeholder_re = re.compile(r"--\s*(your\s+sql|sql\s+query|write\s+your)", re.IGNORECASE)
        if placeholder_re.match(stripped):
          continue
      final_lines.append(ln)
    return "\n".join(final_lines).strip()
  # No code block found: return the full text trimmed
  return text.strip()


class LLMBackend(ABC):
  """Abstract interface for LLM inference backends.

  Each backend implements run_batch which accepts a list of request dicts
  and returns result dicts in the same order (by instance_id).

  Backends that support async submission (e.g. Claude Batch API) override
  supports_async_batch, submit_batch, and collect_batch so the pipeline can
  persist the batch_id to checkpoint and resume after an interruption.
  """

  # --- synchronous interface (required) ------------------------------------

  @abstractmethod
  def run_batch(self, requests: list[dict]) -> list[dict]:
    """Run a batch of inference requests.

    Args:
      requests: list of {instance_id: str, prompt: str}

    Returns:
      list of {instance_id: str, sql: str, raw_response: str, error: str | None}
      Results are NOT guaranteed to be in the same order as requests.
    """
    ...

  # --- async / resumable interface (optional) ------------------------------

  @property
  def supports_async_batch(self) -> bool:
    """True if this backend supports submit_batch / collect_batch."""
    return False

  def submit_batch(self, requests: list[dict]) -> str:
    """Submit a batch and return a batch_id immediately (no waiting)."""
    raise NotImplementedError

  def collect_batch(self, batch_id: str, requests: list[dict]) -> list[dict]:
    """Poll until batch_id is done, then return results in request order."""
    raise NotImplementedError

  # --- concurrent single-request helper ------------------------------------

  def _call_single(self, req: dict) -> dict:
    """Call the provider API for one request. Override in each backend."""
    raise NotImplementedError

  def _run_concurrent(self, requests: list[dict], max_workers: int) -> list[dict]:
    """Fan out requests via ThreadPoolExecutor, preserve original order."""
    results: list[dict] = [{}] * len(requests)
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
      future_to_idx = {pool.submit(self._call_single, req): i for i, req in enumerate(requests)}
      for future in as_completed(future_to_idx):
        i = future_to_idx[future]
        try:
          results[i] = future.result()
        except Exception as e:
          results[i] = {
            "instance_id": requests[i]["instance_id"],
            "sql": "",
            "raw_response": "",
            "error": str(e),
          }
    return results
