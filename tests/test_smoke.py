from snakemake_executor_plugin_finished import ExecutorSettings, common_settings


def test_common_settings_defaults():
    # Ensure the plugin supports touch mode and is not configured as dry-run.
    assert common_settings.dryrun_exec is False
    assert common_settings.touch_exec is True


def test_executor_settings_defaults():
    # The plugin touches by default; no-touch is opt-in.
    assert ExecutorSettings().no_touch is False
