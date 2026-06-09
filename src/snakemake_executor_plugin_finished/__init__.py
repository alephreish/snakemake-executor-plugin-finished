from dataclasses import dataclass, field

from snakemake_interface_executor_plugins.settings import (
    CommonSettings,
    ExecutorSettingsBase,
)

from .executor import Executor


@dataclass
class ExecutorSettings(ExecutorSettingsBase):
    no_touch: bool = field(
        default=False,
        metadata={
            "help": (
                "Do not touch output timestamps after marking jobs as finished. "
                "By default, outputs are touched."
            )
        },
    )


common_settings = CommonSettings(
    non_local_exec=False,
    dryrun_exec=False,
    implies_no_shared_fs=False,
    job_deploy_sources=False,
    touch_exec=True,
    pass_envvar_declarations_to_cmd=False,
    auto_deploy_default_storage_provider=False,
)

__all__ = ["Executor", "ExecutorSettings", "common_settings"]
