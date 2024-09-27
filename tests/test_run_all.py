import subprocess

from typer.testing import CliRunner

from paraffin.cli import app
import dvc.cli
import pathlib
import pytest
import zntrack

runner = CliRunner()




class ReadFile(zntrack.Node):
    path: pathlib.Path = zntrack.deps_path()

    def run(self):
        pass

@pytest.fixture
def proj02(proj_path) -> zntrack.Project:
    """Project with two independent groups of nodes for testing."""

    with zntrack.Project() as proj:
        data_path = pathlib.Path("data")
        data_path.mkdir()
        data_file = data_path / "data.csv"
        data_file.write_text("1,2,3\n4,5,6\n")
        ReadFile(path=data_file, name="a")
        ReadFile(path=data_file, name="b_1")
        ReadFile(path=data_file, name="b_2")

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


def test_run_all(proj01):
    result = runner.invoke(app)
    assert result.exit_code == 0
    assert f"Running {len(proj01)} stages" in result.stdout

    assert check_finished()


def test_run_selection(proj01):
    result = runner.invoke(app, ["A_X_ParamsToOuts"])
    assert result.exit_code == 0
    assert "Running 1 stages" in result.stdout

    assert check_finished(["A_X_ParamsToOuts"])
    assert not check_finished(["A_Y_ParamsToOuts"])

    result = runner.invoke(app, ["A_Y_ParamsToOuts"])
    assert result.exit_code == 0
    assert "Running 1 stages" in result.stdout

    assert check_finished(["A_Y_ParamsToOuts"])
    assert not check_finished()

    result = runner.invoke(app, ["B_X_AddNodeNumbers", "B_Y_AddNodeNumbers"])
    assert result.exit_code == 0
    assert "Running 6 stages" in result.stdout
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


def test_run_selection_glob(proj01):
    result = runner.invoke(app, ["A_X_*"])
    assert result.exit_code == 0
    assert "Running 0 stages" in result.stdout

    result = runner.invoke(app, ["--glob", "A_X_*"])
    assert result.exit_code == 0
    assert "Running 3 stages" in result.stdout

    assert check_finished(
        ["A_X_ParamsToOuts", "A_X_ParamsToOuts_1", "A_X_AddNodeNumbers"]
    )


def test_run_datafile(proj02):
    result = runner.invoke(app, ["a"])
    assert result.exit_code == 0
    # assert "Running 1 stages" in result.stdout
    assert check_finished(
        ["a"]
    )

    result = runner.invoke(app, ["--glob", "b*"])
    assert result.exit_code == 0
    assert check_finished(["b_1", "b_2"])
