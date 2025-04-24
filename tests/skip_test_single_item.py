import random

import zntrack
from typer.testing import CliRunner

from paraffin.cli import app

runner = CliRunner()


class A(zntrack.Node):
    metrics: dict = zntrack.metrics()

    def run(self):
        self.metrics = {"random": random.random()}


class B(zntrack.Node):
    a: A = zntrack.deps()
    metrics: dict = zntrack.metrics()

    def run(self):
        self.metrics = {"random": self.a.metrics["random"]}


def test_single_item(proj_path, check_finished):
    project = zntrack.Project()

    with project:
        a = A()
        B(a=a)

    project.build()

    result = runner.invoke(app, "submit --single-item B")
    assert result.exit_code == 0
    result = runner.invoke(app, "worker")
    # single item submit should fail, as B depends on A which is missing
    assert "ERROR: failed to reproduce 'B'" in result.stdout

    result = runner.invoke(app, "submit")
    assert result.exit_code == 0
    result = runner.invoke(app, "worker")
    assert result.exit_code == 0

    assert check_finished()

    assert A.from_rev().metrics["random"] == B.from_rev().metrics["random"]
