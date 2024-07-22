[![zincware](https://img.shields.io/badge/Powered%20by-zincware-darkcyan)](https://github.com/zincware)
[![PyPI version](https://badge.fury.io/py/paraffin.svg)](https://badge.fury.io/py/paraffin)

# paraffin

Paraffin, derived from the Latin phrase `parum affinis` meaning
`little related`, is a Python package designed to run [DVC](https://dvc.org)
stages in parallel. While DVC does not currently support this directly, Paraffin
provides an effective workaround. For more details, refer to the DVC
documentation on
[parallel stage execution](https://dvc.org/doc/command-reference/repro#parallel-stage-execution).

> \[!WARNING\] Although DVC supports running multiple `dvc repro` commands
> simultaneously, spawning many workers that finish at the same time may lead to
> DVC lock issues and unexpected failures.

## Installation

Install Paraffin via pip:

```bash
pip install paraffin
```

## Usage

To use Paraffin, you can run the following to run up to 4 DVC stages in
parallel:

```bash
paraffin -n 4 <stage names>
```

If you have `pip install dash` you can also access the dashboard by running

```bash
paraffin --dashboard <stage names>
```

For more information, run:

```bash
paraffin --help
```

> \[!TIP\] If you are building Python-based workflows with DVC, consider trying
> our other project [ZnTrack](https://zntrack.readthedocs.io/) for a more
> Pythonic way to define workflows.
