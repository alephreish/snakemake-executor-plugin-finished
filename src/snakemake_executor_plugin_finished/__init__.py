from snakemake_interface_executor_plugins.settings import CommonSettings

from .executor import Executor

common_settings = CommonSettings(
    non_local_exec=False,
    dryrun_exec=False,
    implies_no_shared_fs=False,
    job_deploy_sources=False,
    touch_exec=True,
    pass_envvar_declarations_to_cmd=False,
    auto_deploy_default_storage_provider=False,
)

__all__ = ["Executor", "common_settings"]
