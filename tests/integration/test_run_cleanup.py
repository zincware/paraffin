from pathlib import Path

import zntrack
from typer.testing import CliRunner

from paraffin.cli import app

runner = CliRunner()


class CleanupCheckNode(zntrack.Node):
    text: str = zntrack.params()
    file: Path = zntrack.outs_path(zntrack.nwd / "output.txt")
    metrics_file: Path = zntrack.metrics_path(zntrack.nwd / "metrics.json")
    plots_file: Path = zntrack.plots_path(zntrack.nwd / "plots.json")

    def run(self):
        assert not self.file.exists()
        assert not self.metrics_file.exists()
        assert not self.plots_file.exists()

        self.file.parent.mkdir(parents=True, exist_ok=True)
        self.file.write_text(self.text)
        self.metrics_file.write_text(self.text)
        self.plots_file.write_text(self.text)


def test_run_cleanup(proj_path, check_finished):
    project = zntrack.Project()

    with project:
        node = CleanupCheckNode(text="Hello World!")

    project.build()

    result = runner.invoke(app, "submit")
    assert result.exit_code == 0
    result = runner.invoke(app, ["worker"])
    assert result.exit_code == 0
    result = runner.invoke(app, ["commit"])
    assert result.exit_code == 0
    assert check_finished()

    node.text = "Lorem Ipsum"
    project.build()
    assert node.file.exists()

    # Now we are testing the cleanup
    result = runner.invoke(app, "submit")
    assert result.exit_code == 0
    assert not node.file.exists()  # < file should be deleted at submit

    result = runner.invoke(app, ["worker"])
    assert result.exit_code == 0
    result = runner.invoke(app, ["commit"])
    assert result.exit_code == 0
    assert check_finished()
