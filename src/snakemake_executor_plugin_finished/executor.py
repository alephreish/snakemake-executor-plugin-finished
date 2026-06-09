from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Dict, Iterable, List, Set

from snakemake_interface_common.exceptions import WorkflowError
from snakemake_interface_executor_plugins.executors.base import (
    AbstractExecutor,
    SubmittedJobInfo,
)
from snakemake_interface_executor_plugins.jobs import JobExecutorInterface
from snakemake_interface_executor_plugins.logging import LoggerExecutorInterface
from snakemake_interface_executor_plugins.workflow import WorkflowExecutorInterface


@dataclass
class FinishedTargets:
    """Tracks requested targets and which remain unmatched."""

    requested: Set[str]
    remaining: Set[str]


@dataclass
class TimestampState:
    path: Path
    existed: bool
    atime_ns: int | None = None
    mtime_ns: int | None = None


class Executor(AbstractExecutor):
    """Mark targets as finished without executing jobs."""

    _targets: FinishedTargets
    _pending_timestamp_restore: Dict[int, List[TimestampState]]

    def __init__(
        self, workflow: WorkflowExecutorInterface, logger: LoggerExecutorInterface
    ) -> None:
        super().__init__(workflow, logger)
        targets = self._resolve_targets()
        if not targets:
            raise WorkflowError(
                "No targets specified for finished executor. "
                "Provide targets as positional arguments."
            )
        self._targets = FinishedTargets(requested=targets, remaining=set(targets))
        self._pending_timestamp_restore = {}

    def _normalize_targets(self, targets: Iterable[str]) -> Set[str]:
        return {str(target) for target in targets}

    def _resolve_targets(self) -> Set[str]:
        dag = self.workflow.dag
        targets = getattr(dag, "derived_targetfiles", None)
        if targets is None:
            targets = getattr(dag, "targetfiles", None)
        if not targets:
            return set()
        return self._normalize_targets(targets)

    def _job_target(self, job: JobExecutorInterface) -> str | None:
        target = getattr(job, "targetfile", None)
        if target is None:
            return None
        return str(target)

    def _job_outputs(self, job: JobExecutorInterface) -> Set[str]:
        return {str(output) for output in job.output}

    def _missing_outputs(self, job: JobExecutorInterface) -> List[str]:
        return sorted(output for output in self._job_outputs(job) if not Path(output).exists())

    def _touch_target(self, target: str) -> None:
        target_path = Path(target)
        target_path.touch()
        if target_path.is_dir():
            (target_path / ".snakemake_timestamp").touch()

    def _touch_enabled(self) -> bool:
        return not self._no_touch_requested()

    def _no_touch_requested(self) -> bool:
        settings = getattr(self.workflow, "executor_settings", None)
        if settings is None:
            return False
        no_touch = getattr(settings, "no_touch", False)
        return isinstance(no_touch, bool) and no_touch

    def _capture_timestamp_state(self, target: str) -> List[TimestampState]:
        target_path = Path(target)
        if not target_path.exists():
            return []
        if target_path.is_dir():
            marker = target_path / ".snakemake_timestamp"
            if marker.exists():
                stat = marker.stat()
                return [
                    TimestampState(
                        path=marker,
                        existed=True,
                        atime_ns=stat.st_atime_ns,
                        mtime_ns=stat.st_mtime_ns,
                    )
                ]
            return [TimestampState(path=marker, existed=False)]

        stat = target_path.stat()
        return [
            TimestampState(
                path=target_path,
                existed=True,
                atime_ns=stat.st_atime_ns,
                mtime_ns=stat.st_mtime_ns,
            )
        ]

    def _restore_timestamp_state(self, states: List[TimestampState]) -> None:
        for state in states:
            if state.existed:
                if not state.path.exists():
                    raise WorkflowError(
                        f"Cannot restore timestamp because path vanished: {state.path}"
                    )
                assert state.atime_ns is not None
                assert state.mtime_ns is not None
                os.utime(state.path, ns=(state.atime_ns, state.mtime_ns))
                continue

            if state.path.exists():
                if state.path.is_file():
                    state.path.unlink()
                else:
                    raise WorkflowError(
                        "Cannot remove newly created timestamp marker because it is "
                        f"not a file: {state.path}"
                    )

    def run_job(self, job: JobExecutorInterface) -> None:
        job_info = SubmittedJobInfo(job=job)
        self.report_job_submission(job_info)

        job_targets = set()
        target = self._job_target(job)
        if target is not None:
            job_targets.add(target)
        job_targets.update(self._job_outputs(job))
        matches = self._targets.remaining.intersection(job_targets)
        missing_outputs = self._missing_outputs(job)
        if missing_outputs:
            if matches:
                missing_targets = sorted(match for match in matches if match in missing_outputs)
                if missing_targets:
                    raise WorkflowError(
                        "Target files do not exist: " + ", ".join(missing_targets)
                    )
            raise WorkflowError(
                "Dependency files do not exist: " + ", ".join(missing_outputs)
            )
        if self._no_touch_requested():
            states = []
            for output in sorted(self._job_outputs(job)):
                states.extend(self._capture_timestamp_state(output))
            if states:
                self._pending_timestamp_restore[id(job)] = states
        if matches:
            self.workflow.async_run(self.workflow.persistence.finished(job))
            touch_enabled = self._touch_enabled()
            if touch_enabled:
                for match in sorted(matches):
                    self._touch_target(match)
            self.logger.info(
                "Job "
                f"'{job.name}' marked as finished"
                f"{' and touched' if touch_enabled else ''}"
                f" for: {', '.join(sorted(matches))}"
            )
            self._targets.remaining.difference_update(matches)

        self.report_job_success(job_info)

    def shutdown(self) -> None:
        if self._targets.remaining:
            missing = sorted(self._targets.remaining)
            raise WorkflowError(
                f"Something went wrong, the following targets were not found: {missing}"
            )

    def cancel(self) -> None:
        pass

    def handle_job_success(self, job: JobExecutorInterface) -> None:
        states = self._pending_timestamp_restore.pop(id(job), None)
        if states is not None:
            self._restore_timestamp_state(states)

    def handle_job_error(self, job: JobExecutorInterface) -> None:
        self._pending_timestamp_restore.pop(id(job), None)
