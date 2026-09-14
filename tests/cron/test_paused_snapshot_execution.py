"""Durable paused-source execution admission contracts."""


def _point(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))


def test_keyed_claim_replays_and_retains_tombstone(monkeypatch, tmp_path):
    _point(monkeypatch, tmp_path)
    from cron import executions

    row, replayed = executions.create_or_replay_paused_snapshot_execution(
        "job", occurrence_key="occ-1", snapshot_sha256="a" * 64)
    assert not replayed
    executions.mark_execution_running(row["id"])
    executions.finish_execution(row["id"], success=True)
    same, replayed = executions.create_or_replay_paused_snapshot_execution(
        "job", occurrence_key="occ-1", snapshot_sha256="a" * 64)
    assert replayed and same["id"] == row["id"]
    assert executions.has_active_paused_snapshot("job") is False


def test_active_paused_snapshot_blocks_legacy_mutation(monkeypatch, tmp_path):
    _point(monkeypatch, tmp_path)
    from cron.jobs import create_job, get_job, resume_job
    from cron.executions import create_or_replay_paused_snapshot_execution

    job = create_job(prompt="x", schedule="every 5m")
    from cron.jobs import pause_job
    pause_job(job["id"])
    create_or_replay_paused_snapshot_execution(job["id"], occurrence_key="o", snapshot_sha256="b" * 64)
    before = get_job(job["id"])
    try:
        resume_job(job["id"])
    except ValueError as exc:
        assert "paused snapshot" in str(exc)
    else:
        raise AssertionError("resume unexpectedly crossed active execution fence")
    assert get_job(job["id"]) == before


def test_cancel_request_is_idempotent(monkeypatch, tmp_path):
    _point(monkeypatch, tmp_path)
    from cron.executions import create_or_replay_paused_snapshot_execution, request_execution_cancel

    row, _ = create_or_replay_paused_snapshot_execution(
        "job", occurrence_key="o", snapshot_sha256="c" * 64)
    updated, first = request_execution_cancel(row["id"])
    again, second = request_execution_cancel(row["id"])
    assert first is True and second is False
    assert updated["cancel_requested_at"] == again["cancel_requested_at"]


def test_pending_cancel_is_not_reported_as_terminal_cancelled():
    from gateway.platforms.api_server_cron_execution import _receipt

    receipt = _receipt({
        "id": "e", "job_id": "j", "status": "running",
        "cancel_requested_at": "now", "error": None,
    })
    assert receipt["status"] == "running"
    assert receipt["outcome"] == "running"
    assert receipt["cancel_requested_at"] == "now"


def test_rearm_cannot_cross_active_snapshot_fence(monkeypatch, tmp_path):
    _point(monkeypatch, tmp_path)
    from cron.jobs import create_job, pause_job, rearm_oneshot
    from cron.executions import create_or_replay_paused_snapshot_execution

    job = create_job(prompt="x", schedule="in 30m")
    pause_job(job["id"])
    create_or_replay_paused_snapshot_execution(
        job["id"], occurrence_key="rearm", snapshot_sha256="e" * 64)
    try:
        rearm_oneshot(job["id"], "in 45m")
    except ValueError as exc:
        assert "paused snapshot" in str(exc)
    else:
        raise AssertionError("rearm unexpectedly crossed active execution fence")


def test_running_legacy_execution_blocks_snapshot_admission(monkeypatch, tmp_path):
    _point(monkeypatch, tmp_path)
    from cron.jobs import create_job, pause_job
    from cron.executions import create_execution, mark_execution_running
    from gateway.platforms.api_server_cron_execution import _digest, _legacy_claim

    job = create_job(prompt="x", schedule="every 5m")
    legacy = create_execution(job["id"], source="builtin")
    mark_execution_running(legacy["id"])
    pause_job(job["id"])
    assert _legacy_claim(job) is False
    from cron.executions import has_active_execution
    assert has_active_execution(job["id"]) is True
    assert _digest(job)


def test_immutable_handoff_failure_does_not_mutate_source(monkeypatch, tmp_path):
    _point(monkeypatch, tmp_path)
    from cron import scheduler
    from cron.jobs import create_job, pause_job
    from cron.executions import create_or_replay_paused_snapshot_execution

    job = create_job(prompt="x", schedule="every 5m")
    pause_job(job["id"])
    from gateway.platforms.api_server_cron_execution import _digest
    from cron.jobs import get_job
    row, _ = create_or_replay_paused_snapshot_execution(
        job["id"], occurrence_key="handoff", snapshot_sha256=_digest(get_job(job["id"]))
    )
    before = (tmp_path / "cron" / "jobs.json").read_bytes()
    monkeypatch.setattr(scheduler, "_launch_external_cron_worker", lambda _job: (_ for _ in ()).throw(RuntimeError("synthetic")))
    scheduler.run_one_job({**job, "execution_id": row["id"], "_source_immutable": True}, source_immutable=True)
    assert (tmp_path / "cron" / "jobs.json").read_bytes() == before


def test_error_redaction_fails_closed(monkeypatch):
    import gateway.platforms.api_server as api
    from gateway.platforms.api_server_cron_execution import _receipt

    monkeypatch.setattr(api, "_redact_api_error_text", lambda _text: (_ for _ in ()).throw(RuntimeError()))
    receipt = _receipt({"id": "e", "job_id": "j", "status": "failed", "error": "secret"})
    assert receipt["error"] == "[redacted error unavailable]"


# --- exact-review repair: any-active admission exclusion inside the ledger transaction -----


def test_standard_claimed_execution_blocks_paused_creation(monkeypatch, tmp_path):
    """A standard (ledger-visible) claimed execution must block paused creation at the
    ledger boundary itself — the API precheck is advisory only."""
    _point(monkeypatch, tmp_path)
    from cron.executions import (
        create_execution, create_or_replay_paused_snapshot_execution,
        get_paused_snapshot_occurrence, has_active_paused_snapshot)

    create_execution("job", source="builtin")
    try:
        create_or_replay_paused_snapshot_execution(
            "job", occurrence_key="occ", snapshot_sha256="a" * 64)
    except RuntimeError as exc:
        assert "an execution is already active" in str(exc)
    else:
        raise AssertionError("paused creation crossed an active standard execution")
    assert get_paused_snapshot_occurrence("job", "occ") is None
    assert has_active_paused_snapshot("job") is False


def test_running_legacy_execution_blocks_paused_creation(monkeypatch, tmp_path):
    """Any-kind active (a running legacy/standard row) blocks paused creation, while the
    historical standard-execution concurrency — a second standard claim while another
    standard row is still active (crashed owner awaiting recovery) — stays legal.  That
    legality is exactly why a DB-wide active-unique index is unsafe here."""
    _point(monkeypatch, tmp_path)
    from cron.executions import (
        create_execution, create_or_replay_paused_snapshot_execution,
        mark_execution_running)

    first = create_execution("job", source="builtin")
    assert mark_execution_running(first["id"]) is not None
    try:
        create_or_replay_paused_snapshot_execution(
            "job", occurrence_key="occ", snapshot_sha256="b" * 64)
    except RuntimeError as exc:
        assert "an execution is already active" in str(exc)
    else:
        raise AssertionError("paused creation crossed a running legacy execution")
    # Standard reclaim semantics are untouched: another standard claim is still admitted.
    second = create_execution("job", source="builtin")
    assert second["status"] == "claimed"


def test_failed_paused_claim_rolls_back_completely(monkeypatch, tmp_path):
    """A rejected paused claim must leave no partial row behind — and once the blocking
    standard execution terminalizes, the very same occurrence key must be claimable."""
    _point(monkeypatch, tmp_path)
    from cron.executions import (
        create_execution, create_or_replay_paused_snapshot_execution, finish_execution,
        get_paused_snapshot_occurrence, has_active_paused_snapshot, list_executions)

    standard = create_execution("job", source="builtin")
    with __import__("pytest").raises(RuntimeError):
        create_or_replay_paused_snapshot_execution(
            "job", occurrence_key="occ", snapshot_sha256="c" * 64)
    assert get_paused_snapshot_occurrence("job", "occ") is None
    assert has_active_paused_snapshot("job") is False
    assert [row for row in list_executions(job_id="job")
            if row.get("execution_kind") == "paused_snapshot"] == []

    assert finish_execution(standard["id"], success=True) is not None
    row, replayed = create_or_replay_paused_snapshot_execution(
        "job", occurrence_key="occ", snapshot_sha256="c" * 64)
    assert replayed is False and row["status"] == "claimed"


def test_terminal_prior_rows_do_not_block_new_occurrence(monkeypatch, tmp_path):
    """Only active rows exclude admission: terminal standard history and terminal paused
    tombstones (other keys) leave a fresh occurrence claim valid."""
    _point(monkeypatch, tmp_path)
    from cron.executions import (
        create_execution, create_or_replay_paused_snapshot_execution, finish_execution,
        mark_execution_running)

    done = create_execution("job", source="builtin")
    mark_execution_running(done["id"])
    finish_execution(done["id"], success=False, error="boom")
    tomb, _ = create_or_replay_paused_snapshot_execution(
        "job", occurrence_key="old", snapshot_sha256="d" * 64)
    finish_execution(tomb["id"], success=True)

    fresh, replayed = create_or_replay_paused_snapshot_execution(
        "job", occurrence_key="new", snapshot_sha256="e" * 64)
    assert replayed is False and fresh["status"] == "claimed"


def test_occurrence_replay_is_idempotent_over_legacy_active_row(monkeypatch, tmp_path):
    """Replaying an already-claimed occurrence is a pure durable read: it stays idempotent
    even when a legacy active standard row coexists on disk (a DB written before the mutual
    guards existed must not wedge idempotent replay)."""
    _point(monkeypatch, tmp_path)
    from cron.executions import (
        _transaction, create_or_replay_paused_snapshot_execution,
        get_paused_snapshot_occurrence)

    row, _ = create_or_replay_paused_snapshot_execution(
        "job", occurrence_key="occ", snapshot_sha256="f" * 64)
    # Legacy on-disk state: an active standard row the guards never saw (pre-upgrade DB).
    with _transaction() as conn:
        conn.execute(
            """INSERT INTO executions
               (id, job_id, source, process_id, pid, status, claimed_at, execution_kind)
               VALUES ('legacy', 'job', 'builtin', 'gone', 0, 'claimed', ?, 'standard')""",
            (row["claimed_at"],))

    same, replayed = create_or_replay_paused_snapshot_execution(
        "job", occurrence_key="occ", snapshot_sha256="f" * 64)
    assert replayed is True and same["id"] == row["id"]
    assert get_paused_snapshot_occurrence("job", "occ")["id"] == row["id"]


# --- focused forward-port regressions (current-main seams) -------------------------------------


def test_standard_fire_claim_rejected_while_snapshot_active(monkeypatch, tmp_path):
    """Synchronized standard-vs-paused race: the shared per-job fire fence must keep a
    standard ``claim_job_for_fire`` from crossing an active paused snapshot in either order."""
    _point(monkeypatch, tmp_path)
    from cron.jobs import create_job, pause_job, claim_job_for_fire
    from cron.executions import create_or_replay_paused_snapshot_execution

    job = create_job(prompt="x", schedule="every 5m")
    pause_job(job["id"])
    _, replayed = create_or_replay_paused_snapshot_execution(
        job["id"], occurrence_key="race", snapshot_sha256="f" * 64)
    assert not replayed
    assert claim_job_for_fire(job["id"], force=True) is False


def test_legacy_execution_claim_rejected_while_snapshot_active(monkeypatch, tmp_path):
    """Any standard ledger claim (``create_execution``) fails closed while a paused
    snapshot is active for the job."""
    _point(monkeypatch, tmp_path)
    from cron.jobs import create_job, pause_job
    from cron.executions import create_execution, create_or_replay_paused_snapshot_execution

    job = create_job(prompt="x", schedule="every 5m")
    pause_job(job["id"])
    create_or_replay_paused_snapshot_execution(
        job["id"], occurrence_key="guard", snapshot_sha256="0" * 64)
    try:
        create_execution(job["id"], source="builtin")
    except RuntimeError as exc:
        assert "paused snapshot" in str(exc)
    else:
        raise AssertionError("standard create_execution crossed the paused-snapshot fence")


def test_cancel_before_start_finishes_failed_without_touching_source(monkeypatch, tmp_path):
    """A cancel requested before the worker starts must terminalize the attempt honestly
    and leave the paused source untouched."""
    _point(monkeypatch, tmp_path)
    from cron import scheduler
    from cron.jobs import create_job, get_job, pause_job
    from cron.executions import (
        create_or_replay_paused_snapshot_execution, get_execution, request_execution_cancel)

    job = create_job(prompt="x", schedule="every 5m")
    pause_job(job["id"])
    row, _ = create_or_replay_paused_snapshot_execution(
        job["id"], occurrence_key="cancel-first", snapshot_sha256="1" * 64)
    request_execution_cancel(row["id"])
    started = []
    monkeypatch.setattr(scheduler, "run_job", lambda *_a, **_k: started.append("ran") or (True, "out", "final", None))
    marked = []
    monkeypatch.setattr(scheduler, "mark_job_run", lambda *_a, **_k: marked.append("marked"))

    before = (tmp_path / "cron" / "jobs.json").read_bytes()
    assert scheduler.run_one_job({**job, "execution_id": row["id"]}, source_immutable=True) is True

    assert started == []  # cancelled before the agent was ever built
    assert marked == []
    record = get_execution(row["id"])
    assert record["status"] == "failed"
    assert record["error"] == "Execution cancelled before start"
    assert (tmp_path / "cron" / "jobs.json").read_bytes() == before
    assert get_job(job["id"])["state"] == "paused"


def test_midrun_cancel_terminalizes_without_delivery_or_source_write(monkeypatch, tmp_path):
    """A cooperative cancel observed mid-run (via the run's cancel_event) must skip
    delivery, terminalize the ledger row honestly, and leave jobs.json untouched."""
    _point(monkeypatch, tmp_path)
    import time
    from cron import scheduler
    from cron.jobs import create_job, get_job, pause_job
    from cron.executions import (
        create_or_replay_paused_snapshot_execution, get_execution, request_execution_cancel)

    job = create_job(prompt="x", schedule="every 5m")
    pause_job(job["id"])
    row, _ = create_or_replay_paused_snapshot_execution(
        job["id"], occurrence_key="midrun", snapshot_sha256="9" * 64)

    def fake_run_job(j, cancel_event=None, **_kw):
        deadline = time.time() + 10
        while time.time() < deadline:
            if cancel_event is not None and cancel_event.is_set():
                return (False, "out", "partial", "Execution cancelled")
            time.sleep(0.02)
        return (True, "out", "final", None)  # pragma: no cover - cancel always wins here

    monkeypatch.setattr(scheduler, "run_job", fake_run_job)
    delivered = []
    monkeypatch.setattr(
        scheduler, "_deliver_result", lambda j, c, **_kw: delivered.append(c) or None)
    monkeypatch.setattr(scheduler, "save_job_output", lambda jid, out: f"/tmp/{jid}.txt")

    def delayed_cancel():
        time.sleep(0.3)
        request_execution_cancel(row["id"])

    import threading
    threading.Thread(target=delayed_cancel).start()
    before = (tmp_path / "cron" / "jobs.json").read_bytes()
    assert scheduler.run_one_job({**job, "execution_id": row["id"]}, source_immutable=True) is True

    record = get_execution(row["id"])
    assert record["status"] == "failed"
    assert record["error"] == "Execution cancelled"
    assert delivered == []
    assert (tmp_path / "cron" / "jobs.json").read_bytes() == before
    assert get_job(job["id"])["state"] == "paused"


def test_immutable_run_preserves_run_job_parity_and_source(monkeypatch, tmp_path):
    """FakeAgent parity: the immutable path drives the SAME execute→save→deliver body,
    never calls claim_dispatch/mark_job_run, and terminalizes the ledger row."""
    _point(monkeypatch, tmp_path)
    from cron import scheduler
    from cron.jobs import create_job, pause_job
    from cron.executions import create_or_replay_paused_snapshot_execution, get_execution

    job = create_job(prompt="x", schedule="every 5m")
    pause_job(job["id"])
    row, _ = create_or_replay_paused_snapshot_execution(
        job["id"], occurrence_key="parity", snapshot_sha256="2" * 64)

    run_calls, saved, delivered = [], [], []
    monkeypatch.setattr(
        scheduler, "run_job",
        lambda j, **_k: run_calls.append(j["id"]) or (True, "out", "final response", None))
    monkeypatch.setattr(
        scheduler, "save_job_output", lambda jid, out: saved.append(jid) or f"/tmp/{jid}.txt")
    monkeypatch.setattr(
        scheduler, "_deliver_result",
        lambda j, content, **_kw: delivered.append((j["id"], content)) or None)
    dispatched, marked = [], []
    monkeypatch.setattr(scheduler, "claim_dispatch", lambda jid: dispatched.append(jid) or True)
    monkeypatch.setattr(scheduler, "mark_job_run", lambda *a, **k: marked.append(a) or True)

    before = (tmp_path / "cron" / "jobs.json").read_bytes()
    assert scheduler.run_one_job({**job, "execution_id": row["id"]}, source_immutable=True) is True

    assert run_calls == [job["id"]]
    assert saved == [job["id"]]
    assert delivered and delivered[0][0] == job["id"]
    assert dispatched == []  # source store bookkeeping is fenced
    assert marked == []
    record = get_execution(row["id"])
    assert record["status"] == "completed"
    assert record["delivery_outcome"] == "suppressed"  # deliver=local default lane
    assert (tmp_path / "cron" / "jobs.json").read_bytes() == before


def test_dead_owner_recovery_marks_unknown_and_replay_is_honest(monkeypatch, tmp_path):
    """Death-after-claim: restart recovery marks the attempt unknown and a same-key
    replay returns that record — never a second execution."""
    _point(monkeypatch, tmp_path)
    from cron import executions

    row, replayed = executions.create_or_replay_paused_snapshot_execution(
        "dead", occurrence_key="dead-1", snapshot_sha256="3" * 64)
    assert not replayed
    executions.mark_execution_running(row["id"])
    # Simulate the owning process dying after the run started (side effects unknown).
    monkeypatch.setattr(executions, "_PROCESS_ID", "dead-process")
    monkeypatch.setattr(executions, "_owner_is_live", lambda _pid, _started: False)
    assert executions.recover_interrupted_executions() == 1
    assert executions.get_execution(row["id"])["status"] == "unknown"

    monkeypatch.setattr(executions, "_PROCESS_ID", "replacement-process")
    same, replayed = executions.create_or_replay_paused_snapshot_execution(
        "dead", occurrence_key="dead-1", snapshot_sha256="3" * 64)
    assert replayed and same["id"] == row["id"]
    assert same["status"] == "unknown"
    # A different snapshot digest on the same key is refused outright.
    try:
        executions.create_or_replay_paused_snapshot_execution(
            "dead", occurrence_key="dead-1", snapshot_sha256="4" * 64)
    except ValueError:
        pass
    else:
        raise AssertionError("occurrence key rebound to a different snapshot")
