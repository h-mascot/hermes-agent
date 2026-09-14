"""Authenticated API boundary for immutable execution of paused cron sources."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re

from cron import jobs as cron_jobs
from cron.executions import (
    create_or_replay_paused_snapshot_execution,
    execution_cancel_requested,
    finish_execution,
    get_execution,
    get_paused_snapshot_occurrence,
    has_active_execution,
    has_active_paused_snapshot,
    request_execution_cancel,
)

_KEY_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")


def _digest(job: dict) -> str:
    payload = json.dumps(job, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return hashlib.sha256(payload).hexdigest()


def _paused(job: dict) -> bool:
    return not bool(job.get("enabled", True)) and job.get("state") == "paused"


def _error(text: str, status: int):
    from aiohttp import web
    return web.json_response({"error": text}, status=status)


def _receipt(record: dict, *, replayed: bool = False, status_url: str | None = None) -> dict:
    status = record.get("status")
    outcome = (
        "cancelled" if status in {"completed", "failed"} and
        str(record.get("error") or "").startswith("Execution cancelled")
        else status
    )
    error = record.get("error")
    try:
        from gateway.platforms.api_server import _redact_api_error_text
        error = _redact_api_error_text(error) if error else error
    except Exception:
        error = "[redacted error unavailable]" if error else None
    result = {
        "execution_id": record.get("id"), "job_id": record.get("job_id"),
        "occurrence_key": record.get("occurrence_key"),
        "snapshot_sha256": record.get("snapshot_sha256"), "status": status,
        "cancel_requested_at": record.get("cancel_requested_at"),
        "outcome": outcome, "claimed_at": record.get("claimed_at"),
        "started_at": record.get("started_at"), "finished_at": record.get("finished_at"),
        "delivery_outcome": record.get("delivery_outcome"), "error": error,
    }
    if status_url:
        result["status_url"] = status_url
    if replayed:
        result["replayed"] = True
    else:
        result["replayed"] = False
    return result


def _snapshot_locked(job_id: str):
    job = cron_jobs.get_job(job_id)
    if not job:
        return None, None
    return job, _digest(job)


def _legacy_claim(job: dict) -> bool:
    """A pre-ledger fire/run claim is an admission gap; fail closed until it clears."""
    return bool(job.get("fire_claim") or job.get("run_claim"))


async def handle_capability(adapter, request):
    guard = adapter._cron_request_guard(request, need_job_id=True)
    job_id, err = guard
    if err:
        return err
    from aiohttp import web
    with cron_jobs._fire_job_lock(job_id) as acquired:
        if not acquired:
            return _error("cron fire fence unavailable", 503)
        with cron_jobs._jobs_lock():
            job, digest = _snapshot_locked(job_id)
            if job is None:
                return _error("Job not found", 404)
            eligible = _paused(job) and not has_active_execution(job_id) and not _legacy_claim(job)
    reason = None if eligible else ("job_not_paused" if not _paused(job) else "execution_active")
    return web.json_response({
        "protocol": "hermes.paused-source-execution.v1", "supported": True,
        "job_id": job_id, "snapshot_sha256": digest, "eligible": eligible,
        "reason": reason, "requires_expected_digest": True,
        "supports_cancel": True, "source_unchanged": True,
    })


async def handle_execute(adapter, request):
    guard = adapter._cron_request_guard(request, need_job_id=True, check_draining=True)
    job_id, err = guard
    if err:
        return err
    from aiohttp import web
    try:
        body = await request.json()
    except Exception:
        return _error("Invalid JSON", 400)
    key = body.get("occurrence_key") if isinstance(body, dict) else None
    expected = body.get("expected_snapshot_sha256") if isinstance(body, dict) else None
    if not isinstance(key, str) or not _KEY_RE.fullmatch(key):
        return _error("Invalid occurrence_key", 400)
    if not isinstance(expected, str) or not _DIGEST_RE.fullmatch(expected):
        return _error("Invalid expected_snapshot_sha256", 400)
    if set(body) != {"occurrence_key", "expected_snapshot_sha256"}:
        return _error("Unknown execution request field", 400)
    from gateway.platforms import api_server as api
    with api._reserve_pending_api_work(adapter) as reservation:
        with cron_jobs._fire_job_lock(job_id) as acquired:
            if not acquired:
                return _error("cron fire fence unavailable", 503)
            with cron_jobs._jobs_lock():
                existing = get_paused_snapshot_occurrence(job_id, key)
                if existing is not None:
                    if existing.get("snapshot_sha256") != expected:
                        return _error("occurrence key is already bound to a different snapshot", 409)
                    status_url = f"/api/jobs/{job_id}/executions/{existing['id']}"
                    return web.json_response(
                        _receipt(existing, replayed=True, status_url=status_url), status=200)
                job, digest = _snapshot_locked(job_id)
                if job is None:
                    return _error("Job not found", 404)
                if not _paused(job):
                    return _error("Job is not paused", 409)
                if has_active_execution(job_id) or _legacy_claim(job):
                    return _error("An execution is already active for this job", 409)
                if expected != digest:
                    return _error("Snapshot digest mismatch", 412)
                try:
                    record, replayed = create_or_replay_paused_snapshot_execution(
                        job_id, occurrence_key=key, snapshot_sha256=digest)
                except ValueError as exc:
                    return _error(str(exc), 409)
                except RuntimeError as exc:
                    return _error(str(exc), 409)
        status_url = f"/api/jobs/{job_id}/executions/{record['id']}"
        if replayed:
            return web.json_response(_receipt(record, replayed=True, status_url=status_url), status=200)
        runner = adapter.gateway_runner or request.app.get("gateway_runner")
        adapters = getattr(runner, "adapters", None) or None
        loop = asyncio.get_running_loop()
        try:
            task = asyncio.create_task(asyncio.to_thread(
                _run_snapshot, dict(job, execution_id=record["id"]), adapters, loop))
            reservation["detached"] = True
            task.add_done_callback(lambda t: api._release_pending_api_work(adapter, reservation))
            adapter._track_background_task(task, tolerate_missing=True)
        except Exception:
            finish_execution(record["id"], success=False, error="Execution registration failed")
            return _error("Execution registration failed", 503)
        return web.json_response(_receipt(record, status_url=status_url), status=202)


def _run_snapshot(job, adapters, loop):
    from cron.scheduler import run_one_job
    return run_one_job(job, adapters=adapters, loop=loop, source_immutable=True)


async def handle_status(adapter, request):
    guard = adapter._cron_request_guard(request, need_job_id=True)
    job_id, err = guard
    if err:
        return err
    execution_id = request.match_info["execution_id"]
    record = get_execution(execution_id)
    if not record or str(record.get("job_id")) != str(job_id):
        return _error("Execution not found", 404)
    from aiohttp import web
    return web.json_response(_receipt(record, status_url=request.path))


async def handle_cancel(adapter, request):
    guard = adapter._cron_request_guard(request, need_job_id=True)
    job_id, err = guard
    if err:
        return err
    execution_id = request.match_info["execution_id"]
    record = get_execution(execution_id)
    if not record or str(record.get("job_id")) != str(job_id):
        return _error("Execution not found", 404)
    record, newly = request_execution_cancel(execution_id)
    from aiohttp import web
    return web.json_response(_receipt(record, status_url=request.path), status=202 if newly else 200)
