import os
import subprocess

import git
import pytest
import yaml
from typer.testing import CliRunner

from paraffin.cli import app

runner = CliRunner()


DVC_YAML = {
    "stages": {
        "00a_syncdata": {
            "outs": ["output"],
            "cmd": [
                """bash -euo pipefail << EOF

bash src/00_syncdata.sh

EOF"""
            ],
        }
    }
}


@pytest.fixture
def repo(tmp_path):
    os.chdir(tmp_path)
    repo = git.Repo.init(tmp_path)
    # run dvc init in the repo
    subprocess.run(["dvc", "init"], cwd=tmp_path)
    (tmp_path / "dvc.yaml").write_text(yaml.dump(DVC_YAML))
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "00_syncdata.sh").write_text(
        """#!/bin/bash
echo "Hello, World!" > output
"""
    )
    repo.index.add(["dvc.yaml", "src/00_syncdata.sh"])
    repo.index.commit("Initial commit")
    return repo


def test_051(repo):
    # Test an issue where `cmd` is a list instead of a string
    subprocess.check_call(["dvc repro"], shell=True)
    subprocess.check_call(["dvc freeze 00a_syncdata"], shell=True)

    result = runner.invoke(app, "submit")
    if result.exit_code != 0:
        print(result.stdout)
        assert result.exit_code == 0

    result = runner.invoke(app, "worker")
    if result.exit_code != 0:
        raise AssertionError(f"{result.exit_code} - {result.stdout}")
