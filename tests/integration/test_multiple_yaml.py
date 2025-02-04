"""Test repository with multiple dvc.yaml files."""

import os
import subprocess

import git
import pytest
import yaml
from typer.testing import CliRunner

from paraffin.cli import app

runner = CliRunner()

dvc_yaml_dict = {
    "stages": {
        "01_preprocessing": {
            "outs": ["output"],
            "cmd": [
                """bash -euo pipefail << EOF

# add bash code here
mkdir -p output && echo "Hello World" > output/hello.txt

EOF"""
            ],
        }
    }
}


@pytest.fixture
def nested_project(tmp_path):
    subprocess.check_call(["git", "init"], cwd=tmp_path)
    subprocess.check_call(["dvc", "init"], cwd=tmp_path)

    (tmp_path / "dvc.yaml").touch()
    exp1 = tmp_path / "exp1"
    exp1.mkdir()

    (exp1 / "dvc.yaml").write_text(yaml.dump(dvc_yaml_dict))

    repo = git.Repo(tmp_path)
    repo.git.add(all=True)
    repo.index.commit("Initial commit")

    return tmp_path


def test_053(nested_project):
    os.chdir(nested_project)
    # subprocess.check_call(["dvc repro"], shell=True)

    result = runner.invoke(app, "submit")
    assert result.exit_code == 0

    result = runner.invoke(app, "worker")
    assert result.exit_code == 0

    assert (
        nested_project / "exp1" / "output" / "hello.txt"
    ).read_text() == "Hello World\n"
