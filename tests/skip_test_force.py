import random

import zntrack
from typer.testing import CliRunner

from paraffin.cli import app

runner = CliRunner()


class RNG(zntrack.Node):
    metrics: dict = zntrack.metrics()

    def run(self):
        self.metrics = {"random": random.random()}


def test_force_rerun(proj_path):
    project = zntrack.Project()

    with project:
        _ = RNG()

    project.build()

    result = runner.invoke(app, "submit")
    assert result.exit_code == 0
    result = runner.invoke(app, "worker")
    assert result.exit_code == 0

    res_a = RNG.from_rev().metrics["random"]

    result = runner.invoke(app, "submit")
    assert result.exit_code == 0
    result = runner.invoke(app, "worker")
    assert result.exit_code == 0

    # without force
    assert RNG.from_rev().metrics["random"] == res_a

    result = runner.invoke(app, "submit --force")
    assert result.exit_code == 0
    result = runner.invoke(app, "worker")
    assert result.exit_code == 0

    # with force
    assert RNG.from_rev().metrics["random"] != res_a
