[![zincware](https://img.shields.io/badge/Powered%20by-zincware-darkcyan)](https://github.com/zincware)
[![PyPI version](https://badge.fury.io/py/paraffin.svg)](https://badge.fury.io/py/paraffin)
[![Discord](https://img.shields.io/discord/1034511611802689557)](https://discord.gg/7ncfwhsnm4)

# paraffin

Paraffin, derived from the Latin phrase `parum affinis` meaning
`little related`, is a Python package designed to run [DVC](https://dvc.org)
stages in parallel. While DVC does not currently support this directly, Paraffin
provides an effective workaround. For more details, refer to the DVC
documentation on
[parallel stage execution](https://dvc.org/doc/command-reference/repro#parallel-stage-execution).

> [!WARNING]
> `paraffin` is still very experimental.
> Do not use it for production workflows.

## Installation

Install Paraffin via pip:

```bash
pip install paraffin
```

## Usage

https://github.com/user-attachments/assets/c248e669-7737-450b-9fd7-5b9b8e82605a

### paraffin submit
You can submit your current DVC workflow to a database file `paraffin.db` for later execution.

> [!TIP]
> The paraffin submit command supports globing patterns.
```bash
paraffin submit # submit all stages
paraffin submit C_AddNodeNumbers "A*" # select which stages to submit
paraffin submit --help # more information
```

### paraffin worker
A submitted job will be executed by paraffin workers.
To start a worker you can run `paraffin worker`.
The worker will pick up all the jobs in the workeres queue and close once finished.
You can specify the number of stages a worker should process in parallel by using `paraffin worker --jobs <n>`.
Alternatively, you can start more workers by running the command multiple times.

```bash
paraffin worker
paraffin worker --help # more information
```

### paraffin ui
Paraffin ships with a web application for visualizing the progress.
You can start it using
```bash
paraffin ui
paraffin ui --help # more information
```
The UI allows you to visualize the progress in real-time, restart jobs and manage workers.

https://github.com/user-attachments/assets/034325fd-7035-434f-9eb8-b47ae4ecbb86

## Queue Labels

To fine-tune execution, you can assign stages to specific Celery queues, allowing you to manage execution across different environments or hardware setups.
Define queues in a `paraffin.yaml` file:

```yaml
queue:
    "B_X*": BQueue
    "A_X_AddNodeNumbers": AQueue
```
Then, start a worker with specified queues, such as celery (default) and AQueue:
```bash
paraffin worker -q AQueue,default
```
All `stages` not assigned to a queue in `paraffin.yaml` will default to the `default` queue.


> [!TIP]
> If you are building Python-based workflows with DVC, consider trying
> our other project [ZnTrack](https://zntrack.readthedocs.io/) for a more
> Pythonic way to define workflows.
