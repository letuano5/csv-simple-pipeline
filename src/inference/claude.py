"""Claude Batch API backend.

Uses Anthropic Messages Batches API:
  - client.messages.batches.create(requests=[...])
  - Poll client.messages.batches.retrieve(batch_id) every 30s
  - Stream results via client.messages.batches.results(batch_id)

Model defaults to CLAUDE_MODEL env var, or claude-haiku-4-5-20251001.
Max tokens defaults to CLAUDE_MAX_TOKENS env var, or 1024.
"""

from __future__ import annotations

import logging
import os
import time

import anthropic
from anthropic.types.message_create_params import MessageCreateParamsNonStreaming
from anthropic.types.messages.batch_create_params import Request

from src.inference.base import LLMBackend, extract_sql

log = logging.getLogger(__name__)

_DEFAULT_MODEL = "claude-haiku-4-5-20251001"
_POLL_INTERVAL = 30  # seconds
_DEFAULT_CONCURRENCY = 5  # conservative — Messages API has strict RPM limits


class ClaudeBackend(LLMBackend):
  def __init__(self, *, concurrent: bool = False):
    self.client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    self.model = os.environ.get("CLAUDE_MODEL", _DEFAULT_MODEL)
    self.max_tokens = int(os.environ.get("CLAUDE_MAX_TOKENS", "4096"))
    self.concurrent = concurrent
    self.max_workers = int(os.environ.get("CLAUDE_CONCURRENCY", _DEFAULT_CONCURRENCY))
    log.info(f"Claude model {self.model} with {self.max_tokens} max tokens")

  @property
  def supports_async_batch(self) -> bool:
    # Async batch (submit/collect split) only applies when NOT in concurrent mode
    return not self.concurrent

  def submit_batch(self, requests: list[dict]) -> str:
    """Submit to Anthropic Batch API and return batch_id immediately."""
    log.info("[Claude] Submitting batch of %d requests (model=%s)", len(requests), self.model)
    batch_requests = [
      Request(
        custom_id=req["instance_id"],
        params=MessageCreateParamsNonStreaming(
          model=self.model,
          max_tokens=self.max_tokens,
          messages=[{"role": "user", "content": req["prompt"]}],
        ),
      )
      for req in requests
    ]
    batch = self.client.messages.batches.create(requests=batch_requests)
    log.info("[Claude] Batch submitted: %s", batch.id)
    return batch.id

  def collect_batch(self, batch_id: str, requests: list[dict]) -> list[dict]:
    """Poll batch_id until ended, then return results in request order."""
    while True:
      batch = self.client.messages.batches.retrieve(batch_id)
      status = batch.processing_status
      counts = batch.request_counts
      log.info(
        "[Claude] Batch %s: %s — processing=%d succeeded=%d errored=%d",
        batch_id, status,
        counts.processing, counts.succeeded, counts.errored,
      )
      if status == "ended":
        break
      time.sleep(_POLL_INTERVAL)

    results_map: dict[str, dict] = {}
    for result in self.client.messages.batches.results(batch_id):
      iid = result.custom_id
      if result.result.type == "succeeded":
        raw = result.result.message.content[0].text
        results_map[iid] = {
          "instance_id": iid,
          "sql": extract_sql(raw),
          "raw_response": raw,
          "error": None,
        }
      else:
        err_type = result.result.type
        error_msg = str(getattr(result.result, "error", err_type))
        results_map[iid] = {
          "instance_id": iid,
          "sql": "",
          "raw_response": "",
          "error": f"{err_type}: {error_msg}",
        }
        log.warning("[Claude] Request %s failed: %s", iid, error_msg)

    return [
      results_map.get(req["instance_id"], {
        "instance_id": req["instance_id"],
        "sql": "",
        "raw_response": "",
        "error": "missing from batch results",
      })
      for req in requests
    ]

  def _call_single(self, req: dict) -> dict:
    iid = req["instance_id"]
    try:
      resp = self.client.messages.create(
        model=self.model,
        max_tokens=self.max_tokens,
        messages=[{"role": "user", "content": req["prompt"]}],
      )
      raw = resp.content[0].text
      return {"instance_id": iid, "sql": extract_sql(raw), "raw_response": raw, "error": None}
    except Exception as e:
      log.warning("[Claude] Single request %s failed: %s", iid, e)
      return {"instance_id": iid, "sql": "", "raw_response": "", "error": str(e)}

  def run_batch(self, requests: list[dict]) -> list[dict]:
    if not requests:
      return []
    if self.concurrent:
      log.info("[Claude] Concurrent mode: %d requests (workers=%d)", len(requests), self.max_workers)
      return self._run_concurrent(requests, self.max_workers)
    batch_id = self.submit_batch(requests)
    return self.collect_batch(batch_id, requests)
