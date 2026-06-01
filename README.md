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

The executor will mark the matching jobs as finished, touch the outputs, and fail if any requested target files do not exist. If nothing triggers a re-run, no modifications are made to the metadata or timestamps of the correspondign outputs.
