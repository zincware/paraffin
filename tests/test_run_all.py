import subprocess

from typer.testing import CliRunner

from paraffin.cli import app

runner = CliRunner()


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
    assert f"Running {len(proj01.graph)} stages" in result.stdout

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
