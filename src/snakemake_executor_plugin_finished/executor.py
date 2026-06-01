from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Set

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


class Executor(AbstractExecutor):
    """Mark targets as finished without executing jobs."""

    _targets: FinishedTargets

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

    def _touch_target(self, target: str) -> None:
        target_path = Path(target)
        target_path.touch()
        if target_path.is_dir():
            (target_path / ".snakemake_timestamp").touch()

    def run_job(self, job: JobExecutorInterface) -> None:
        job_info = SubmittedJobInfo(job=job)
        self.report_job_submission(job_info)

        job_targets = set()
        target = self._job_target(job)
        if target is not None:
            job_targets.add(target)
        job_targets.update(self._job_outputs(job))
        matches = self._targets.remaining.intersection(job_targets)
        if matches:
            missing = sorted(match for match in matches if not Path(match).exists())
            if missing:
                raise WorkflowError("Target files do not exist: " + ", ".join(missing))
            self.workflow.async_run(self.workflow.persistence.finished(job))
            for match in sorted(matches):
                self._touch_target(match)
            self.logger.info(
                f"Job '{job.name}' marked as finished for: {', '.join(sorted(matches))}"
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
        pass

    def handle_job_error(self, job: JobExecutorInterface) -> None:
        pass
