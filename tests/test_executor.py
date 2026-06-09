from __future__ import annotations

import os

import pytest
from snakemake_interface_common.exceptions import WorkflowError

from snakemake_executor_plugin_finished.executor import Executor


class FakeDag:
    """Minimal DAG stub exposing derived target files."""

    def __init__(self, targets, dependencies=None):
        self.derived_targetfiles = targets
        self.dependencies = dependencies or {}


class FakeScheduler:
    """Capture scheduler callbacks invoked by the executor."""

    def __init__(self):
        self.submitted = []
        self.finished = []
        self.errored = []

    def submit_callback(self, job):
        self.submitted.append(job)

    def finish_callback(self, job):
        self.finished.append(job)

    def error_callback(self, job):
        self.errored.append(job)


class FakePersistence:
    """Record calls that mark jobs as finished."""

    def __init__(self):
        self.finished_jobs = []

    def finished(self, job):
        self.finished_jobs.append(job)
        return ("finished", job)


class FakeWorkflow:
    """Workflow stub with persistence and scheduler hooks."""

    class _ExecutorSettings:
        def __init__(self, no_touch=False):
            self.no_touch = no_touch

    def __init__(self, targets, touch=False, no_touch=False, dependencies=None):
        self.dag = FakeDag(targets, dependencies=dependencies)
        self.persistence = FakePersistence()
        self.scheduler = FakeScheduler()
        self.async_runs = []
        self.touch = touch
        self.executor_settings = self._ExecutorSettings(no_touch=no_touch)

    def async_run(self, value):
        self.async_runs.append(value)


class FakeLogger:
    """Logger stub that records info messages."""

    def __init__(self):
        self.messages = []

    def info(self, message):
        self.messages.append(message)


class FakeJob:
    """Job stub exposing outputs and optional targetfile."""

    def __init__(self, name, outputs, targetfile=None):
        self.name = name
        self.output = outputs
        self.targetfile = targetfile
        self.is_local = True

    def log_info(self):
        pass

    def log_error(self, msg=None, **kwargs):
        pass


def test_executor_requires_targets():
    # The executor requires at least one target from the DAG.
    workflow = FakeWorkflow(targets=[])
    with pytest.raises(WorkflowError, match="No targets specified"):
        Executor(workflow, FakeLogger())


def test_run_job_marks_finished_and_clears_remaining(tmp_path):
    # When the target exists, the executor marks the job finished and clears it.
    target = tmp_path / "output.txt"
    target.write_text("data")
    workflow = FakeWorkflow(targets=[str(target)])
    logger = FakeLogger()
    executor = Executor(workflow, logger)
    job = FakeJob(name="job1", outputs=[str(target)], targetfile=str(target))

    executor.run_job(job)

    assert workflow.persistence.finished_jobs == [job]
    assert workflow.async_runs == [("finished", job)]
    assert workflow.scheduler.submitted == [job]
    assert workflow.scheduler.finished == [job]
    assert executor._targets.remaining == set()
    assert logger.messages == [f"Job '{job.name}' marked as finished and touched for: {target}"]
    executor.shutdown()


def test_run_job_touches_directory_by_default(tmp_path):
    # Directory targets are touched by default.
    target_dir = tmp_path / "dir"
    target_dir.mkdir()
    workflow = FakeWorkflow(targets=[str(target_dir)])
    executor = Executor(workflow, FakeLogger())
    job = FakeJob(name="job1", outputs=[str(target_dir)], targetfile=str(target_dir))

    executor.run_job(job)

    assert (target_dir / ".snakemake_timestamp").exists()


def test_run_job_creates_timestamp_for_directory_with_touch_enabled(tmp_path):
    # Directory targets get a .snakemake_timestamp marker in touch mode.
    target_dir = tmp_path / "dir"
    target_dir.mkdir()
    workflow = FakeWorkflow(targets=[str(target_dir)], touch=True)
    executor = Executor(workflow, FakeLogger())
    job = FakeJob(name="job1", outputs=[str(target_dir)], targetfile=str(target_dir))

    executor.run_job(job)

    assert (target_dir / ".snakemake_timestamp").exists()


def test_run_job_restores_file_mtime_with_no_touch_enabled(tmp_path):
    # File target mtimes are restored after postprocess touches in no-touch mode.
    target = tmp_path / "output.txt"
    target.write_text("data")
    original_mtime_ns = target.stat().st_mtime_ns
    workflow = FakeWorkflow(targets=[str(target)], no_touch=True)
    executor = Executor(workflow, FakeLogger())
    job = FakeJob(name="job1", outputs=[str(target)], targetfile=str(target))

    executor.run_job(job)
    os.utime(target, ns=(original_mtime_ns + 1_000_000_000, original_mtime_ns + 1_000_000_000))
    assert target.stat().st_mtime_ns != original_mtime_ns
    executor.handle_job_success(job)

    assert target.stat().st_mtime_ns == original_mtime_ns


def test_run_job_removes_directory_marker_with_no_touch_enabled(tmp_path):
    # Directory timestamp markers created by postprocess are removed in no-touch mode.
    target_dir = tmp_path / "dir"
    target_dir.mkdir()
    marker = target_dir / ".snakemake_timestamp"
    workflow = FakeWorkflow(targets=[str(target_dir)], no_touch=True)
    executor = Executor(workflow, FakeLogger())
    job = FakeJob(name="job1", outputs=[str(target_dir)], targetfile=str(target_dir))

    executor.run_job(job)
    marker.touch()
    assert marker.exists()
    executor.handle_job_success(job)

    assert not marker.exists()


def test_run_job_restores_dependency_output_mtime_with_no_touch_enabled(tmp_path):
    # No-touch also restores timestamps for dependency jobs, not only explicit targets.
    dep = tmp_path / "dep.txt"
    dep.write_text("dep")
    original_mtime_ns = dep.stat().st_mtime_ns
    requested_target = tmp_path / "final.txt"
    workflow = FakeWorkflow(targets=[str(requested_target)], no_touch=True)
    executor = Executor(workflow, FakeLogger())
    dep_job = FakeJob(name="dep", outputs=[str(dep)], targetfile=None)

    executor.run_job(dep_job)
    os.utime(dep, ns=(original_mtime_ns + 1_000_000_000, original_mtime_ns + 1_000_000_000))
    assert dep.stat().st_mtime_ns != original_mtime_ns
    executor.handle_job_success(dep_job)

    assert dep.stat().st_mtime_ns == original_mtime_ns


def test_run_job_raises_on_missing_target(tmp_path):
    # Missing targets cause a WorkflowError before any finish callbacks.
    target = tmp_path / "missing.txt"
    workflow = FakeWorkflow(targets=[str(target)])
    executor = Executor(workflow, FakeLogger())
    job = FakeJob(name="job1", outputs=[str(target)], targetfile=str(target))

    with pytest.raises(WorkflowError, match="Target files do not exist"):
        executor.run_job(job)

    assert workflow.persistence.finished_jobs == []
    assert executor._targets.remaining == {str(target)}


def test_run_dependency_job_raises_on_missing_output(tmp_path):
    target = tmp_path / "final.txt"
    target.write_text("final")
    dep = tmp_path / "dep.txt"
    workflow = FakeWorkflow(targets=[str(target)])
    executor = Executor(workflow, FakeLogger())
    dep_job = FakeJob(name="dep", outputs=[str(dep)])

    with pytest.raises(WorkflowError, match="Dependency files do not exist"):
        executor.run_job(dep_job)

    assert workflow.persistence.finished_jobs == []
    assert executor._targets.remaining == {str(target)}


def test_run_dependency_job_accepts_existing_output(tmp_path):
    target = tmp_path / "final.txt"
    target.write_text("final")
    dep = tmp_path / "dep.txt"
    dep.write_text("dep")
    workflow = FakeWorkflow(targets=[str(target)])
    executor = Executor(workflow, FakeLogger())
    dep_job = FakeJob(name="dep", outputs=[str(dep)])

    executor.run_job(dep_job)

    assert workflow.persistence.finished_jobs == []
    assert workflow.scheduler.finished == [dep_job]
    assert executor._targets.remaining == {str(target)}
