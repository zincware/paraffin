import zntrack.examples
from typer.testing import CliRunner

from paraffin.cli import app

runner = CliRunner()


def test_run_job(proj_path, check_finished):
    project = zntrack.Project()

    with project:
        a = zntrack.examples.ParamsToOuts(params=1)
        b = zntrack.examples.ParamsToOuts(params=2)
        c = zntrack.examples.AddNodeNumbers(numbers=[a, b])
        d = zntrack.examples.AddNodeAttributes(a=c.sum, b=c.sum)

        # independent node
        e = zntrack.examples.ParamsToOuts(params=3)

    project.build()

    result = runner.invoke(app, "submit")
    assert result.exit_code == 0

    # run a node without predecessors
    result = runner.invoke(app, f"worker --stage {a.name} --experiment 1")
    assert result.exit_code == 0
    assert check_finished([a.name])

    #

    # run a node with predecessors that have not been run yet
    result = runner.invoke(app, f"worker --stage {d.name} --experiment 1")
    assert result.exit_code == 0
    assert check_finished([d.name])
    assert not check_finished([e.name])


def test_run_job_submit_twice(proj_path, check_finished):
    project = zntrack.Project()

    with project:
        a = zntrack.examples.ParamsToOuts(params=1)
        b = zntrack.examples.ParamsToOuts(params=2)
        c = zntrack.examples.AddNodeNumbers(numbers=[a, b])
        d = zntrack.examples.AddNodeAttributes(a=c.sum, b=c.sum)

        # independent node
        e = zntrack.examples.ParamsToOuts(params=3)

    project.build()

    result = runner.invoke(app, "submit")
    assert result.exit_code == 0
    result = runner.invoke(app, "submit")
    assert result.exit_code == 0

    result = runner.invoke(app, f"worker --stage {d.name} --experiment 1")
    assert result.exit_code == 0
    assert check_finished([d.name])
    assert not check_finished([e.name])
