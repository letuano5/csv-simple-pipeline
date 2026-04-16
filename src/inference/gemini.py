"""Gemini Batch API backend.

Uses google-genai SDK:
  - client.batches.create(model=..., src=[...])
  - Poll client.batches.get(name=...) every 30s
  - Retrieve results from batch_job.dest.inlined_responses

Model defaults to GEMINI_MODEL env var, or gemini-2.5-flash.
Inline responses preserve order of src list.

Note: Gemini 2.0 Flash deprecated June 1, 2026 → use gemini-2.5-flash.
"""

from __future__ import annotations

import logging
import os
import time

from google import genai

from src.inference.base import LLMBackend, extract_sql

log = logging.getLogger(__name__)

_DEFAULT_MODEL = "gemini-2.5-flash"
_POLL_INTERVAL = 30  # seconds
_COMPLETED_STATES = {"JOB_STATE_SUCCEEDED", "JOB_STATE_FAILED", "JOB_STATE_CANCELLED", "JOB_STATE_EXPIRED"}
_DEFAULT_CONCURRENCY = 10


class GeminiBackend(LLMBackend):
  def __init__(self, *, concurrent: bool = False):
    self.client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
    self.model = os.environ.get("GEMINI_MODEL", _DEFAULT_MODEL)
    self.max_tokens = int(os.environ.get("GEMINI_MAX_TOKENS", "1024"))
    self.concurrent = concurrent
    self.max_workers = int(os.environ.get("GEMINI_CONCURRENCY", _DEFAULT_CONCURRENCY))

  def _call_single(self, req: dict) -> dict:
    iid = req["instance_id"]
    try:
      resp = self.client.models.generate_content(
        model=self.model,
        contents=req["prompt"],
        config={"max_output_tokens": self.max_tokens},
      )
      raw = resp.text or ""
      return {"instance_id": iid, "sql": extract_sql(raw), "raw_response": raw, "error": None}
    except Exception as e:
      log.warning("[Gemini] Single request %s failed: %s", iid, e)
      return {"instance_id": iid, "sql": "", "raw_response": "", "error": str(e)}

  def run_batch(self, requests: list[dict]) -> list[dict]:
    """Submit requests to Gemini Batch API and wait for completion."""
    if not requests:
      return []

    if self.concurrent:
      log.info("[Gemini] Concurrent mode: %d requests (workers=%d)", len(requests), self.max_workers)
      return self._run_concurrent(requests, self.max_workers)

    log.info("[Gemini] Submitting batch of %d requests (model=%s)", len(requests), self.model)

    # Build inline request list; preserve index for result mapping
    inline_reqs = [
      {
        "contents": [{"parts": [{"text": req["prompt"]}]}],
      }
      for req in requests
    ]

    batch_job = self.client.batches.create(
      model=self.model,
      src=inline_reqs,
      config={"display_name": f"csv-pipeline-{int(time.time())}"},
    )
    job_name = batch_job.name
    log.info("[Gemini] Batch created: %s", job_name)

    # Poll until completed
    while True:
      batch_job = self.client.batches.get(name=job_name)
      state = batch_job.state.name
      log.info("[Gemini] Batch %s: state=%s", job_name, state)
      if state in _COMPLETED_STATES:
        break
      time.sleep(_POLL_INTERVAL)

    if batch_job.state.name != "JOB_STATE_SUCCEEDED":
      # All failed
      log.error("[Gemini] Batch %s ended with state %s", job_name, batch_job.state.name)
      return [
        {
          "instance_id": req["instance_id"],
          "sql": "",
          "raw_response": "",
          "error": f"batch state: {batch_job.state.name}",
        }
        for req in requests
      ]

    # Collect inline responses (order preserved)
    responses = batch_job.dest.inlined_responses
    results = []
    for i, req in enumerate(requests):
      iid = req["instance_id"]
      if i < len(responses):
        try:
          raw = responses[i].response.text
          results.append({
            "instance_id": iid,
            "sql": extract_sql(raw),
            "raw_response": raw,
            "error": None,
          })
        except Exception as e:
          results.append({
            "instance_id": iid,
            "sql": "",
            "raw_response": "",
            "error": str(e),
          })
          log.warning("[Gemini] Request %s failed: %s", iid, e)
      else:
        results.append({
          "instance_id": iid,
          "sql": "",
          "raw_response": "",
          "error": "missing from batch responses",
        })

    return results
