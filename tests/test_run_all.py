import pathlib
import subprocess
import os

import dvc.cli
import pytest
import zntrack
import zntrack.examples
from typer.testing import CliRunner

from paraffin.cli import app

runner = CliRunner()

@pytest.fixture(scope="session", autouse=True)
def set_env():
    os.environ["PARAFFIN_CONCURRENCY"] = "1"
    os.environ["PARAFFIN_SHUTDOWN_AFTER_FINISHED"] = "true"

class ReadFile(zntrack.Node):
    path: pathlib.Path = zntrack.deps_path()
    data: float = zntrack.outs()

    def run(self):
        with open(self.path) as f:
            self.data = sum(map(float, f.read().split(",")))


@pytest.fixture
def proj02(proj_path) -> zntrack.Project:
    """Project with two independent groups of nodes for testing."""

    with zntrack.Project() as proj:
        data_path = pathlib.Path("data")
        data_path.mkdir()
        data_file = data_path / "data.csv"
        data_file.write_text("1,2,3")
        a_1 = ReadFile(path=data_file, name="a_1")
        b_1 = ReadFile(path=data_file, name="b_1")

        a_2 = zntrack.examples.AddNodeAttributes(a=a_1.data, b=a_1.data, name="a_2")
        b_2 = zntrack.examples.AddNodeAttributes(a=b_1.data, b=b_1.data, name="b_2")

    proj.build()
    dvc.cli.main(["add", "data/data.csv"])

    return proj


def check_finished(names: list[str] | None = None) -> bool:
    cmd = ["dvc", "status"]
    for name in names or []:
        cmd.append(name)
    result = subprocess.run(cmd, capture_output=True, check=True)
    return result.stdout.decode().strip() == "Data and pipelines are up to date."


def test_check_finished(proj01):
    subprocess.check_call(["dvc", "repro", "A_X_ParamsToOuts"])
    assert check_finished(["A_X_ParamsToOuts"])
    assert not check_finished(["A_Y_ParamsToOuts"])
    subprocess.check_call(["dvc", "repro", "A_Y_ParamsToOuts"])
    assert check_finished(["A_Y_ParamsToOuts"])
    assert not check_finished()


def test_run_all(proj01, caplog):
    result = runner.invoke(app)
    assert result.exit_code == 0
    # assert f"Running {len(proj01)} stages" in caplog.text

    assert check_finished()


def test_run_selection(proj01, caplog):
    result = runner.invoke(app, ["A_X_ParamsToOuts"])
    assert result.exit_code == 0
    # assert "Running 1 stages" in caplog.text
    # caplog.clear()

    assert check_finished(["A_X_ParamsToOuts"])
    assert not check_finished(["A_Y_ParamsToOuts"])

    result = runner.invoke(app, ["A_Y_ParamsToOuts"])
    assert result.exit_code == 0
    # assert "Running 1 stages" in caplog.text
    # caplog.clear()

    assert check_finished(["A_Y_ParamsToOuts"])
    assert not check_finished()

    result = runner.invoke(app, ["B_X_AddNodeNumbers", "B_Y_AddNodeNumbers"])
    assert result.exit_code == 0
    # assert "Running 6 stages" in caplog.text
    # caplog.clear()

    assert not check_finished()
    assert check_finished(
        [
            "B_X_AddNodeNumbers",
            "B_Y_AddNodeNumbers",
            "B_X_ParamsToOuts",
            "B_Y_ParamsToOuts",
            "B_X_ParamsToOuts_1",
            "B_Y_ParamsToOuts_1",
        ]
    )


def test_run_selection_glob(proj01, caplog):
    result = runner.invoke(app, ["A_X_*"])
    assert result.exit_code == 0
    # assert "Running 0 stages" in caplog.text
    # caplog.clear()

    result = runner.invoke(app, ["--glob", "A_X_*"])
    assert result.exit_code == 0
    # assert "Running 3 stages" in caplog.text
    # caplog.clear()

    assert check_finished(
        ["A_X_ParamsToOuts", "A_X_ParamsToOuts_1", "A_X_AddNodeNumbers"]
    )


def test_run_datafile(proj02, caplog):
    result = runner.invoke(app, ["--glob", "a*"])
    assert result.exit_code == 0
    # assert "Running 2 stages" in caplog.text
    # caplog.clear()
    assert check_finished(["a_1", "a_2"])

    result = runner.invoke(app, ["--glob", "b*"])
    assert result.exit_code == 0
    # assert "Running 2 stages" in caplog.text
    # caplog.clear()
    assert check_finished(["b_1", "b_2"])

    assert zntrack.from_rev("a_2").c == 12
    assert zntrack.from_rev("b_2").c == 12

    # modify data file and run to check changed outputs
    data_file = pathlib.Path("data/data.csv")
    data_file.unlink()
    data_file.write_text("4,5,6")

    result = runner.invoke(app, ["--glob", "a*"])
    # assert "Running 2 stages" in caplog.text
    # caplog.clear()
    assert result.exit_code == 0
    assert check_finished(["a_1", "a_2"])
    assert not check_finished(["b_1", "b_2"])

    assert zntrack.from_rev("a_2").c == 30
    assert zntrack.from_rev("b_2").c == 12

    result = runner.invoke(app, ["--glob", "b*"])
    # assert "Running 2 stages" in caplog.text
    # caplog.clear()
    assert check_finished(["b_1", "b_2"])
    assert result.exit_code == 0
    assert zntrack.from_rev("b_1").data == 15
    assert zntrack.from_rev("b_2").c == 30
