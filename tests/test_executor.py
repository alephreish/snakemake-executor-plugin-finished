from __future__ import annotations

import pytest
from snakemake_interface_common.exceptions import WorkflowError

from snakemake_executor_plugin_finished.executor import Executor


class FakeDag:
    """Minimal DAG stub exposing derived target files."""

    def __init__(self, targets):
        self.derived_targetfiles = targets


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

    def __init__(self, targets):
        self.dag = FakeDag(targets)
        self.persistence = FakePersistence()
        self.scheduler = FakeScheduler()
        self.async_runs = []

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
    executor.shutdown()


def test_run_job_creates_timestamp_for_directory(tmp_path):
    # Directory targets get a .snakemake_timestamp marker when touched.
    target_dir = tmp_path / "dir"
    target_dir.mkdir()
    workflow = FakeWorkflow(targets=[str(target_dir)])
    executor = Executor(workflow, FakeLogger())
    job = FakeJob(name="job1", outputs=[str(target_dir)], targetfile=str(target_dir))

    executor.run_job(job)

    assert (target_dir / ".snakemake_timestamp").exists()


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
