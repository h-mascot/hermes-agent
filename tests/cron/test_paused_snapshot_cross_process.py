"""Cross-process legacy-fire exclusion uses the real jobs/fire fence."""

import os
import subprocess
import sys


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
    env = os.environ.copy()
    env["PYTHONPATH"] = os.getcwd() + os.pathsep + env.get("PYTHONPATH", "")
    result = subprocess.run([sys.executable, "-c", script], env=env, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr
    assert "paused snapshot" in result.stdout
