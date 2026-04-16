"""OpenAI Batch API backend.

Uses openai SDK:
  1. Upload JSONL via client.files.create(purpose="batch")
  2. client.batches.create(endpoint="/v1/chat/completions", completion_window="24h")
  3. Poll client.batches.retrieve(batch_id) every 60s
  4. Download output via client.files.content(batch.output_file_id)

Model defaults to OPENAI_MODEL env var, or gpt-4o-mini.
"""

from __future__ import annotations

import io
import json
import logging
import os
import time

from openai import OpenAI

from src.inference.base import LLMBackend, extract_sql

log = logging.getLogger(__name__)

_DEFAULT_MODEL = "gpt-4o-mini"
_POLL_INTERVAL = 60  # seconds
_TERMINAL_STATES = {"completed", "failed", "expired", "cancelled"}
_DEFAULT_CONCURRENCY = 20


class OpenAIBackend(LLMBackend):
  def __init__(self, *, concurrent: bool = False):
    self.client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    self.model = os.environ.get("OPENAI_MODEL", _DEFAULT_MODEL)
    self.max_tokens = int(os.environ.get("OPENAI_MAX_TOKENS", "4096"))
    self.concurrent = concurrent
    self.max_workers = int(os.environ.get("OPENAI_CONCURRENCY", _DEFAULT_CONCURRENCY))
    log.info(f"Init OpenAI model {self.model}, max_tokens={self.max_tokens}")

  def _call_single(self, req: dict) -> dict:
    iid = req["instance_id"]
    try:
      resp = self.client.chat.completions.create(
        model=self.model,
        messages=[{"role": "user", "content": req["prompt"]}],
        max_completion_tokens=self.max_tokens,
      )
      raw = resp.choices[0].message.content or ""
      return {"instance_id": iid, "sql": extract_sql(raw), "raw_response": raw, "error": None}
    except Exception as e:
      log.warning("[OpenAI] Single request %s failed: %s", iid, e)
      return {"instance_id": iid, "sql": "", "raw_response": "", "error": str(e)}

  def run_batch(self, requests: list[dict]) -> list[dict]:
    """Submit requests to OpenAI Batch API and wait for completion."""
    if not requests:
      return []

    if self.concurrent:
      log.info("[OpenAI] Concurrent mode: %d requests (workers=%d)", len(requests), self.max_workers)
      return self._run_concurrent(requests, self.max_workers)

    log.info("[OpenAI] Submitting batch of %d requests (model=%s)", len(requests), self.model)

    # Build JSONL content
    lines = [
      json.dumps({
        "custom_id": req["instance_id"],
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {
          "model": self.model,
          "messages": [{"role": "user", "content": req["prompt"]}],
          "max_tokens": self.max_tokens,
        },
      })
      for req in requests
    ]
    jsonl_bytes = "\n".join(lines).encode("utf-8")

    # Upload JSONL file
    batch_file = self.client.files.create(
      file=("batch.jsonl", io.BytesIO(jsonl_bytes), "application/jsonl"),
      purpose="batch",
    )
    log.info("[OpenAI] File uploaded: %s", batch_file.id)

    # Create batch
    batch = self.client.batches.create(
      input_file_id=batch_file.id,
      endpoint="/v1/chat/completions",
      completion_window="24h",
    )
    batch_id = batch.id
    log.info("[OpenAI] Batch created: %s", batch_id)

    # Poll until terminal state
    while True:
      batch = self.client.batches.retrieve(batch_id)
      status = batch.status
      counts = batch.request_counts
      log.info(
        "[OpenAI] Batch %s: %s — total=%d completed=%d failed=%d",
        batch_id, status,
        counts.total, counts.completed, counts.failed,
      )
      if status in _TERMINAL_STATES:
        break
      time.sleep(_POLL_INTERVAL)

    if batch.status != "completed":
      log.error("[OpenAI] Batch %s ended with status %s", batch_id, batch.status)
      return [
        {
          "instance_id": req["instance_id"],
          "sql": "",
          "raw_response": "",
          "error": f"batch status: {batch.status}",
        }
        for req in requests
      ]

    # Download output JSONL
    output_content = self.client.files.content(batch.output_file_id).read().decode("utf-8")

    # Parse results
    results_map: dict[str, dict] = {}
    for line in output_content.splitlines():
      if not line.strip():
        continue
      try:
        r = json.loads(line)
        iid = r["custom_id"]
        resp = r.get("response", {})
        if resp.get("status_code") == 200:
          raw = resp["body"]["choices"][0]["message"]["content"]
          results_map[iid] = {
            "instance_id": iid,
            "sql": extract_sql(raw),
            "raw_response": raw,
            "error": None,
          }
        else:
          err = resp.get("body", {}).get("error", {}).get("message", "unknown error")
          results_map[iid] = {
            "instance_id": iid,
            "sql": "",
            "raw_response": "",
            "error": err,
          }
          log.warning("[OpenAI] Request %s failed: %s", iid, err)
      except Exception as e:
        log.warning("[OpenAI] Failed to parse result line: %s", e)

    # Return in original request order
    return [
      results_map.get(req["instance_id"], {
        "instance_id": req["instance_id"],
        "sql": "",
        "raw_response": "",
        "error": "missing from batch output",
      })
      for req in requests
    ]
