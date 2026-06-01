from snakemake_executor_plugin_finished import common_settings


def test_common_settings_defaults():
    # Ensure the plugin is configured to touch outputs and not act as dry-run.
    assert common_settings.dryrun_exec is False
    assert common_settings.touch_exec is True
