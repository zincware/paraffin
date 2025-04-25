import zntrack
from sqlmodel import create_engine
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

    engine = create_engine("sqlite:///paraffin.db")

    status = get_stage_status(engine=engine, stage_name=failing_node.name)
    assert status == StageStatus.FAILED
