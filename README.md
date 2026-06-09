# snakemake-executor-plugin-finished

Snakemake executor plugin that marks specified targets as finished without running jobs. This provides a way to tell Snakemake that the corresponding targets should not be re-created, e.g. in cases of cosmetic changes to the code, non-essential modifications of the environment definition or other modifications that trigger a re-run but are known in advance to have no effect on the output. 

## Installation

```bash
pip install snakemake-executor-plugin-finished
```

or

```bash
pip install git+https://github.com/alephreish/snakemake-executor-plugin-finished
```

## Usage

```bash
snakemake --executor finished <target1> <target2>
```

The executor marks matching jobs as finished and fails if any requested target files or their transitive dependency outputs do not exist.
By default, it touches matching outputs (including `.snakemake_timestamp` for directory targets).
Use `--finished-no-touch` to preserve existing output timestamps.
