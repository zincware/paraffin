import pathlib
import shutil

import dvc.api
import pytest
import zntrack.examples

from paraffin.dvc import StageStatus, get_status


@pytest.fixture
def proj_single_node(proj_path):
    project = zntrack.Project()

    with project:
        stage1 = zntrack.examples.AddNumbers(a=1, b=2)

    project.build()

    return project


@pytest.fixture
def proj_nested_nodes(
    proj_path,
) -> tuple[
    zntrack.Project,
    list[zntrack.examples.AddNumbers | zntrack.examples.SumNodeAttributes],
]:
    project = zntrack.Project()

    with project:
        stage1 = zntrack.examples.AddNumbers(a=1, b=2)
        stage2 = zntrack.examples.AddNumbers(a=1, b=2)
        nested_1 = zntrack.examples.SumNodeAttributes(
            inputs=[stage1.c, stage2.c], shift=0
        )
        nested_2 = zntrack.examples.SumNodeAttributes(inputs=[nested_1.output], shift=0)
        nested_3 = zntrack.examples.SumNodeAttributes(inputs=[nested_2.output], shift=0)

    project.build()

    return project, [stage1, stage2, nested_1, nested_2, nested_3]


def test_stage_unfinished(proj_single_node):
    status = get_status()
    assert len(status) == 1
    stage = next(n for n in status if n.addressing == "AddNumbers")
    assert stage.status == StageStatus.PENDING

    fs = dvc.api.DVCFileSystem()
    repo = fs.repo
    # # with repo.lock:
    status = repo.status()
    assert "AddNumbers" in status
    # stages = repo.stage.collect()
    # assert len(stages) == 1
    # for stage in stages:
    #     with stage.repo.lock:
    #         assert stage.changed() is True


def test_stage_finished(proj_single_node):
    proj_single_node.repro()

    status = get_status()
    assert len(status) == 1
    stage = next(n for n in status if n.addressing == "AddNumbers")
    assert stage.status == StageStatus.COMPLETED

    fs = dvc.api.DVCFileSystem()
    repo = fs.repo
    # # with repo.lock:
    status = repo.status()
    assert status == {}
    # # stages = repo.stage.collect()
    # # assert len(stages) == 1
    # # for stage in stages:
    # #     with stage.repo.lock:
    # #         assert stage.changed() is False


@pytest.mark.parametrize("rmlock", [True, False])
def test_stage_cached(proj_single_node, rmlock):
    proj_single_node.repro()
    shutil.rmtree("nodes", ignore_errors=True)
    dvc_lock_path = pathlib.Path("dvc.lock")
    if rmlock:
        dvc_lock_path.unlink(missing_ok=True)
    # TODO: another check if the dvc.lock is removed!

    status = get_status()
    assert len(status) == 1
    stage = next(n for n in status if n.addressing == "AddNumbers")
    assert stage.status == StageStatus.COMPLETED

    fs = dvc.api.DVCFileSystem()
    repo = fs.repo
    status = repo.status()
    # assert "AddNumbers" in status

    # stages = repo.stage.collect()
    # for stage in stages:
    #     with stage.repo.lock:
    #         try:
    #             stage.repo.stage_cache.restore(stage)
    #             # FYI, there is also stage.commit() which is like Stage.save(), but also saves file to the cache (i.e. commit).
    #             # Stage.dump() is what saves the stage to dvc.yaml and dvc.lock file.
    #             # (dump has update_pipeline=True|False and update_lock=True|False arguments to save to only one or to both of the files).
    #             stage.save()
    #             stage.dump()
    #         except RunCacheNotFoundError:
    #             raise ValueError("Unable to restore the stage")

    # status = repo.status()
    assert status == {}


def test_stage_cached_rm_cache(proj_single_node):
    proj_single_node.repro()
    shutil.rmtree("nodes", ignore_errors=True)
    pathlib.Path("dvc.lock").unlink(missing_ok=True)
    shutil.rmtree(".dvc/cache", ignore_errors=True)

    status = get_status()
    # assert status == {
    #     "AddNumbers": StageStatus.QUEUED,
    # }
    assert len(status) == 1
    stage = next(n for n in status if n.addressing == "AddNumbers")
    assert stage.status == StageStatus.PENDING

    # # TODO: another check if the dvc.lock is removed!

    fs = dvc.api.DVCFileSystem()
    repo = fs.repo
    status = repo.status()
    assert "AddNumbers" in status

    # cache_available = None

    # stages = repo.stage.collect()
    # for stage in stages: # TODO: test later, that the order is correct!
    #     with stage.repo.lock:
    #         try:
    #             stage.repo.stage_cache.restore(stage)
    #             cache_available = True
    #         except RunCacheNotFoundError:
    #             cache_available = False
    #     # with stage.repo.lock:
    #     #     assert stage.changed() is False

    # assert cache_available is False
    # status = repo.status()
    # assert "AddNumbers" in status


def test_stage_nested_unfinished(proj_nested_nodes):
    status = get_status()
    proj, nodes = proj_nested_nodes
    for node in nodes:
        stage = next(s for s in status if s.addressing == node.name)
        assert stage.status == StageStatus.PENDING
    assert len(status) == 5


def test_stage_nested_finished(proj_nested_nodes):
    proj, nodes = proj_nested_nodes
    proj.repro()
    status = get_status()
    for node in nodes:
        stage = next(s for s in status if s.addressing == node.name)
        assert stage.status == StageStatus.COMPLETED, (
            f"Stage {node.name} is not completed"
        )
    assert len(status) == 5

    fs = dvc.api.DVCFileSystem()
    repo = fs.repo
    # # with repo.lock:
    status = repo.status()
    assert status == {}


def test_stage_nested_cached(proj_nested_nodes):
    proj, nodes = proj_nested_nodes
    proj.repro()
    shutil.rmtree("nodes", ignore_errors=True)
    dvc_lock_path = pathlib.Path("dvc.lock")
    dvc_lock_path.unlink(missing_ok=True)

    status = get_status()
    for node in nodes:
        stage = next(s for s in status if s.addressing == node.name)
        assert stage.status == StageStatus.COMPLETED, (
            f"Stage {node.name} is not completed"
        )
    assert len(status) == 5

    fs = dvc.api.DVCFileSystem()
    repo = fs.repo
    # # with repo.lock:
    status = repo.status()
    assert status == {}


def test_stage_nested_cached_rm_cache(proj_nested_nodes):
    proj, nodes = proj_nested_nodes
    proj.repro()
    shutil.rmtree("nodes", ignore_errors=True)
    dvc_lock_path = pathlib.Path("dvc.lock")
    dvc_lock_path.unlink(missing_ok=True)
    shutil.rmtree(".dvc/cache", ignore_errors=True)

    status = get_status()
    for node in nodes:
        stage = next(s for s in status if s.addressing == node.name)
        assert stage.status == StageStatus.PENDING, f"Stage {node.name} is not pending"
    assert len(status) == 5

    fs = dvc.api.DVCFileSystem()
    repo = fs.repo
    # # with repo.lock:
    status = repo.status()
    for node in nodes:
        assert node.name in status


def test_stage_nested_update_params_end(proj_nested_nodes):
    proj, nodes = proj_nested_nodes
    proj.repro()
    # update downstream node
    nodes[-1].shift = 1
    proj.build()

    status = get_status()
    for idx, node in enumerate(nodes):
        stage = next(s for s in status if s.addressing == node.name)
        if idx == len(nodes) - 1:
            assert stage.status == StageStatus.PENDING, (
                f"Stage {node.name} is not pending"
            )
        else:
            assert stage.status == StageStatus.COMPLETED, (
                f"Stage {node.name} is not completed"
            )


def test_stage_nested_update_params_between(proj_nested_nodes):
    proj, nodes = proj_nested_nodes
    proj.repro()
    # update downstream node
    nodes[-3].shift = 1
    proj.build()

    status = get_status()
    for idx, node in enumerate(nodes):
        stage = next(s for s in status if s.addressing == node.name)
        if idx >= len(nodes) - 2:
            # successors (two nodes) of the updated node
            assert stage.status == StageStatus.UNKNOWN, (
                f"Stage {node.name} is not unknown"
            )
        elif idx >= len(nodes) - 3:
            # this is the node that was updated
            assert stage.status == StageStatus.PENDING, (
                f"Stage {node.name} is not pending"
            )
        else:
            assert stage.status == StageStatus.COMPLETED, (
                f"Stage {node.name} is not completed"
            )
