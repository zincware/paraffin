import os
import pathlib
import shutil
import subprocess

import dvc.cli
import git
import pytest
import zntrack.examples


@pytest.fixture
def check_finished():
    def func(names: list[str] | None = None, exclusive: bool = False) -> bool:
        if exclusive:
            raise NotImplementedError
        cmd = ["dvc", "status"]
        for name in names or []:
            cmd.append(name)
        result = subprocess.run(cmd, capture_output=True, check=True)
        finished = (
            result.stdout.decode().strip() == "Data and pipelines are up to date."
        )
        if not finished:
            print(result.stdout.decode())
        return finished

    return func


@pytest.fixture
def proj_path(tmp_path, request) -> pathlib.Path:
    """temporary directory for testing DVC calls

    Parameters
    ----------
    tmp_path
    request: https://docs.pytest.org/en/6.2.x/reference.html#std-fixture-request

    Returns
    -------
    path to temporary directory

    """
    shutil.copy(request.module.__file__, tmp_path)
    os.chdir(tmp_path)
    git.Repo.init()
    dvc.cli.main(["init"])
    git.Repo().index.commit("Initial commit")

    return tmp_path


@pytest.fixture
def proj01(proj_path) -> zntrack.Project:
    """Project with two independent groups of nodes for testing.

    ```mermaid
    flowchart TD
        node0["data/data.csv"]
        node1["A_SumNodeAttributes"]
        node2["A_X_AddNodeNumbers"]
        node3["A_X_ParamsToOuts"]
        node4["A_X_ParamsToOuts_1"]
        node5["A_Y_AddNodeNumbers"]
        node6["A_Y_ParamsToOuts"]
        node7["A_Y_ParamsToOuts_1"]
        node2-->node1
        node3-->node2
        node4-->node2
        node5-->node1
        node6-->node5
        node7-->node5
        node8["B_SumNodeAttributes"]
        node9["B_X_AddNodeNumbers"]
        node10["B_X_ParamsToOuts"]
        node11["B_X_ParamsToOuts_1"]
        node12["B_Y_AddNodeNumbers"]
        node13["B_Y_ParamsToOuts"]
        node14["B_Y_ParamsToOuts_1"]
        node9-->node8
        node10-->node9
        node11-->node9
        node12-->node8
        node13-->node12
        node14-->node12
    ```
    """
    PARAM = 1  # noqa N806

    with zntrack.Project() as proj:
        for x in ["A", "B"]:
            results = []
            for y in ["X", "Y"]:
                with proj.group(x, y):
                    n1 = zntrack.examples.ParamsToOuts(params=PARAM)
                    n2 = zntrack.examples.ParamsToOuts(params=PARAM)
                    res = zntrack.examples.AddNodeNumbers(numbers=[n1, n2])
                    results.append(res.sum)
            with proj.group(x):
                zntrack.examples.SumNodeAttributes(inputs=results, shift=0)

    proj.build()

    assert len(proj) == 14

    return proj


def proj02(proj_path) -> zntrack.Project:
    PARAM = 1  # noqa N806
    project = zntrack.Project()

    with project.group("A"):
        n = zntrack.examples.ParamsToOuts(params=PARAM)
    for idx in range(3):
        with project.group("A", str(idx)):
            _ = zntrack.examples.AddNodeNumbers(numbers=[n])

    project.build()

    assert len(project) == 4

    return project
