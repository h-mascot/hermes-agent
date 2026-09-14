"""Cross-process legacy-fire exclusion uses the real jobs/fire fence."""

import os
import subprocess
import sys
import time


def _run_py(script: str, *args, env=None):
    env = dict(env or os.environ)
    env["PYTHONPATH"] = os.getcwd() + os.pathsep + env.get("PYTHONPATH", "")
    return subprocess.run(
        [sys.executable, "-c", script, *args], env=env, text=True, capture_output=True)


def test_other_process_cannot_resume_active_snapshot(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    from cron.jobs import create_job, pause_job
    from cron.executions import create_or_replay_paused_snapshot_execution

    job = create_job(prompt="x", schedule="every 5m")
    pause_job(job["id"])
    create_or_replay_paused_snapshot_execution(
        job["id"], occurrence_key="cross", snapshot_sha256="d" * 64)
    script = (
        "from cron.jobs import resume_job; "
        f"\ntry: resume_job({job['id']!r})\n"
        "except ValueError as e: print(e); raise SystemExit(0)\n"
        "raise SystemExit(3)"
    )
    result = _run_py(script)
    assert result.returncode == 0, result.stderr
    assert "paused snapshot" in result.stdout


# --- symmetric admission boundary: standard vs paused across real SQLite writers --------------

_PAUSED_ADMITTER = (
    "import sys, time\n"
    "from pathlib import Path\n"
    "from cron.executions import create_or_replay_paused_snapshot_execution, finish_execution\n"
    "row, _ = create_or_replay_paused_snapshot_execution(\n"
    "    sys.argv[1], occurrence_key='inter-a', snapshot_sha256='a' * 64)\n"
    "print('created', row['id'], flush=True)\n"
    "go = Path(sys.argv[2])\n"
    "while not go.exists():\n"
    "    time.sleep(0.02)\n"
    "assert finish_execution(row['id'], success=True) is not None\n"
    "print('finished', flush=True)\n"
)

_STANDARD_ADMITTER_WITH_WINDOW = (
    "import sys, time\n"
    "from cron import executions\n"
    "_real = executions._process_start_time\n"
    "def _slow(pid):\n"
    "    # Real production seam between create_execution's guard read and its INSERT.\n"
    "    time.sleep(2.0)\n"
    "    return _real(pid)\n"
    "executions._process_start_time = _slow\n"
    "try:\n"
    "    row = executions.create_execution(sys.argv[1], source='builtin')\n"
    "    print('created', row['id'])\n"
    "except RuntimeError as exc:\n"
    "    print('rejected', exc)\n"
    "    raise SystemExit(3)\n"
)


def test_paused_first_excludes_standard_admission_cross_process(tmp_path, monkeypatch):
    """Interleaving A (paused wins the writer): a paused claim committed by another process
    must fail standard admission closed; once it terminalizes, the standard claim recovers."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    from cron.jobs import create_job, pause_job
    from cron.executions import create_execution, get_execution, list_executions

    job = create_job(prompt="x", schedule="every 5m")
    pause_job(job["id"])
    marker = tmp_path / "go"
    env = dict(os.environ)
    env["PYTHONPATH"] = os.getcwd() + os.pathsep + env.get("PYTHONPATH", "")
    admitter = subprocess.Popen(
        [sys.executable, "-c", _PAUSED_ADMITTER, job["id"], str(marker)],
        env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        created_line = admitter.stdout.readline().strip()
        assert created_line.startswith("created"), (created_line, admitter.stderr.read())
        paused_id = created_line.split()[1]

        try:
            create_execution(job["id"], source="builtin")
        except RuntimeError as exc:
            assert "paused snapshot" in str(exc)
        else:
            raise AssertionError("standard admission crossed an active paused claim")
        assert [row for row in list_executions(job_id=job["id"])
                if row.get("execution_kind") == "standard"] == []  # no partial claim

        marker.touch()  # let the owner terminalize its own claim
    finally:
        out, err = admitter.communicate(timeout=60)
    assert admitter.returncode == 0, (admitter.returncode, out, err)

    assert get_execution(paused_id)["status"] == "completed"
    recovered = create_execution(job["id"], source="builtin")
    assert recovered["status"] == "claimed"  # standard recovery semantics preserved


def test_standard_window_excludes_paused_admission_cross_process(tmp_path, monkeypatch):
    """Interleaving B (standard holds the read-to-insert window): a paused admission landing
    inside that window must never coexist with the standard claim — exactly one admissible
    execution class may win, and the loser leaves no partial row."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    from cron.jobs import create_job, pause_job
    from cron.executions import (
        create_or_replay_paused_snapshot_execution, list_executions)

    job = create_job(prompt="x", schedule="every 5m")
    pause_job(job["id"])

    env = dict(os.environ)
    env["PYTHONPATH"] = os.getcwd() + os.pathsep + env.get("PYTHONPATH", "")
    writer = subprocess.Popen(
        [sys.executable, "-c", _STANDARD_ADMITTER_WITH_WINDOW, job["id"]],
        env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        time.sleep(0.5)  # the writer's guard read has passed; its 2s seam window is open
        paused_error = None
        try:
            create_or_replay_paused_snapshot_execution(
                job["id"], occurrence_key="inter-b", snapshot_sha256="b" * 64)
        except RuntimeError as exc:
            paused_error = exc
    finally:
        out, err = writer.communicate(timeout=60)

    assert writer.returncode in (0, 3), (writer.returncode, err)
    standard_created = writer.returncode == 0
    rows = list_executions(job_id=job["id"], limit=50)
    standard_rows = [row for row in rows if row.get("execution_kind") == "standard"]
    paused_rows = [row for row in rows if row.get("execution_kind") == "paused_snapshot"]

    # Exactly one admissible class won; never both.
    assert not (standard_created and paused_error is None), (
        "both standard and paused admission succeeded inside the writer window")
    if standard_created:
        assert paused_error is not None
        assert "already active" in str(paused_error)
        assert paused_rows == []  # rejected admission left no partial claim
    else:
        assert "paused snapshot" in (out or "") or writer.returncode == 3
        assert standard_rows == []


_ADMISSION_HAMMER = (
    "import sys, time\n"
    "from cron import executions\n"
    "job = sys.argv[1]\n"
    "kind = sys.argv[2]\n"
    "created = rejected = 0\n"
    "for i in range(25):\n"
    "    try:\n"
    "        if kind == 'standard':\n"
    "            row = executions.create_execution(job, source='builtin')\n"
    "        else:\n"
    "            row, _ = executions.create_or_replay_paused_snapshot_execution(\n"
    "                job, occurrence_key=f'h{i}', snapshot_sha256='c' * 64)\n"
    "        created += 1\n"
    "        executions.finish_execution(row['id'], success=True)\n"
    "    except RuntimeError:\n"
    "        rejected += 1\n"
    "    except Exception as exc:\n"
    "        print('unexpected', type(exc).__name__, exc)\n"
    "        raise SystemExit(4)\n"
    "    time.sleep(0.02)\n"
    "print(f'done created={created} rejected={rejected}')\n"
)

_BOTH_ACTIVE_SAMPLER = (
    "import sqlite3, sys, time\n"
    "from pathlib import Path\n"
    "from hermes_constants import get_hermes_home\n"
    "db = Path(get_hermes_home()).resolve() / 'cron' / 'executions.db'\n"
    "conn = sqlite3.connect(db, timeout=2.0)\n"  # plain read connection: no schema-init DDL
    "conn.execute('PRAGMA busy_timeout=2000')\n"
    "deadline = time.time() + float(sys.argv[1])\n"
    "observations = both = 0\n"
    "while time.time() < deadline:\n"
    "    row = conn.execute(\n"
    "        \"SELECT SUM(kind='paused_snapshot'), SUM(kind='standard') FROM (\"\n"
    "        \" SELECT execution_kind AS kind FROM executions WHERE job_id=? \"\n"
    "        \" AND status IN ('claimed','running'))\", (sys.argv[2],)).fetchone()\n"
    "    observations += 1\n"
    "    if row[0] and row[1]:\n"
    "        both += 1\n"
    "    time.sleep(0.01)\n"
    "conn.close()\n"
    "print(f'sampled={observations} both_active={both}')\n"
)


def test_symmetric_admission_hammer_never_coexists_nor_deadlocks(tmp_path, monkeypatch):
    """Two real writer processes hammer both admission classes on one job against real
    SQLite: both must finish inside the bound (no deadlock), no unexpected errors, and a
    third sampler never observes standard and paused claims active at the same time."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    from cron.jobs import create_job, pause_job
    from cron.executions import list_executions

    job = create_job(prompt="x", schedule="every 5m")
    pause_job(job["id"])

    env = dict(os.environ)
    env["PYTHONPATH"] = os.getcwd() + os.pathsep + env.get("PYTHONPATH", "")
    sampler = subprocess.Popen(
        [sys.executable, "-c", _BOTH_ACTIVE_SAMPLER, "6", job["id"]],
        env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(0.1)
    hammer_std = subprocess.Popen(
        [sys.executable, "-c", _ADMISSION_HAMMER, job["id"], "standard"],
        env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    hammer_paused = subprocess.Popen(
        [sys.executable, "-c", _ADMISSION_HAMMER, job["id"], "paused"],
        env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    out_std, err_std = hammer_std.communicate(timeout=60)
    out_paused, err_paused = hammer_paused.communicate(timeout=60)
    out_sampler, err_sampler = sampler.communicate(timeout=60)

    assert hammer_std.returncode == 0, (err_std, out_std)
    assert hammer_paused.returncode == 0, (err_paused, out_paused)
    assert "unexpected" not in out_std and "unexpected" not in out_paused
    assert sampler.returncode == 0, (err_sampler, out_sampler)
    assert "both_active=0" in out_sampler, out_sampler

    leftover = [row for row in list_executions(job_id=job["id"], limit=100)
                if row["status"] in ("claimed", "running")]
    assert leftover == []  # every admitted row was terminalized; no wedged partial claim
