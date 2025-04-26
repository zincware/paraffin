"""Test an issue with full path instead of file dependencies."""

import os
import subprocess

import git
import pytest
from typer.testing import CliRunner

from paraffin.cli import app

runner = CliRunner()


CONTENT = """
import zntrack
import pathlib
import zntrack.examples
import random
import subprocess


class ReadFile(zntrack.Node):
    path: pathlib.Path = zntrack.deps_path()
    data: float = zntrack.outs()

    def run(self):
        with open(self.path / "data.csv") as f:
            self.data = sum(map(float, f.read().split(",")))


if __name__ == "__main__":
    data = pathlib.Path("data")
    data.mkdir(exist_ok=True)
    data_file1 = pathlib.Path("data/data.csv")
    data_file1.unlink(missing_ok=True)
    with data_file1.open("w") as f:
        f.write(",".join(str(random.randint(1, 10000)) for _ in range(5)))

    subprocess.check_call(["dvc", "add", "data"])

    proj = zntrack.Project()
    with proj:
        a = ReadFile(path=pathlib.Path("data"))

    proj.build()
"""


@pytest.fixture
def repo(tmp_path):
    os.chdir(tmp_path)
    repo = git.Repo.init(tmp_path)
    # run dvc init in the repo
    subprocess.run(["dvc", "init"], cwd=tmp_path)

    (tmp_path / "main.py").write_text(CONTENT)

    subprocess.check_call(["python", "main.py"])
    # make a git commit

    repo.index.add(["main.py"])
    repo.index.commit("Initial commit")

    return repo


def test_053(repo):
    # subprocess.check_call(["dvc repro"], shell=True)
    result = runner.invoke(app, "submit")
    assert result.exit_code == 0

    result = runner.invoke(app, "worker")
    assert result.exit_code == 0
