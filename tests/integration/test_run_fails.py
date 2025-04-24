import zntrack
from typer.testing import CliRunner

from paraffin.cli import app
from paraffin.db.app import get_stage_status
from paraffin.dvc import StageStatus

runner = CliRunner()


class FailingNode(zntrack.Node):
    def run(self):
        raise Exception("This is a test exception")


def test_run_fails(proj_path):
    project = zntrack.Project()

    with project:
        failing_node = FailingNode()

    project.build()

    result = runner.invoke(app, "submit")
    assert result.exit_code == 0
    result = runner.invoke(app, ["worker"])
    assert result.exit_code == 0

    status = get_stage_status(
        db_url="sqlite:///paraffin.db", stage_name=failing_node.name
    )
    assert status == StageStatus.FAILED
