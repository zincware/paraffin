[![zincware](https://img.shields.io/badge/Powered%20by-zincware-darkcyan)](https://github.com/zincware)
[![PyPI version](https://badge.fury.io/py/paraffin.svg)](https://badge.fury.io/py/paraffin)

# paraffin

Paraffin, derived from the Latin phrase `parum affinis` meaning
`little related`, is a Python package designed to run [DVC](https://dvc.org)
stages in parallel. While DVC does not currently support this directly, Paraffin
provides an effective workaround. For more details, refer to the DVC
documentation on
[parallel stage execution](https://dvc.org/doc/command-reference/repro#parallel-stage-execution).

> [!WARNING]
> `paraffin` is still very experimental. Do not use it for production workflows.

## Installation

Install Paraffin via pip:

```bash
pip install paraffin
```

## Usage

To use Paraffin, you can run the following to queue up the execution of these DVC stages.

```bash
paraffin <stage name> <stage name> ... <stage name>
# run max 4 jobs in parallel
celery -A paraffin.worker worker --loglevel=WARNING --concurrency=4
```

If you have `pip install dash` you can also access the dashboard by running

```bash
paraffin --dashboard <stage names>
```

For more information, run:

```bash
paraffin --help
```

## Labels

You can run `paraffin` in multiple processes (e.g. on different hardware with a
shared file system). To specify where a `stage` should run, you can assign
labels to each worker.

```
paraffin --labels GPU # on a GPU node
paraffin --label CPU intel # on a CPU node
```

To configure the stages you need to create a `paraffin.yaml` file as follows:

```yaml
labels:
    GPU_TASK:
        - GPU
    CPU_TASK:
        - CPU
    SPECIAL_CPU_TASK:
        - CPU
        - intel
```

All `stages` that are not part of the `paraffin.yaml` will choose any of the
available workers.

> [!TIP]
> If you are building Python-based workflows with DVC, consider trying
> our other project [ZnTrack](https://zntrack.readthedocs.io/) for a more
> Pythonic way to define workflows.
