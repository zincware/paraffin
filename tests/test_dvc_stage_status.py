import dvc.api
import zntrack.examples
import pytest
import dvc.api
import shutil
from dvc.stage.cache import RunCacheNotFoundError
import pathlib
import subprocess

from paraffin.dvc import get_status, StageStatus

@pytest.fixture
def proj(proj_path):
    project = zntrack.Project()

    with project:
        stage1 = zntrack.examples.AddNumbers(
            a=1, b=2
        )
    
    project.build()

    return project

def test_stage_unfinished(proj):
    status = get_status()
    assert len(status) == 1
    stage = next(n for n in status if n.addressing == "AddNumbers")
    assert stage.status == StageStatus.QUEUED

    fs = dvc.api.DVCFileSystem()
    repo = fs.repo
    # # with repo.lock:
    status = repo.status()
    assert 'AddNumbers' in status
    # stages = repo.stage.collect()
    # assert len(stages) == 1
    # for stage in stages:
    #     with stage.repo.lock:
    #         assert stage.changed() is True

    
def test_stage_finished(proj):
    proj.repro()

    status = get_status()
    assert len(status) == 1
    stage = next(n for n in status if n.addressing == "AddNumbers")
    assert stage.status == StageStatus.FINISHED

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
def test_stage_cached(proj, rmlock):
    proj.repro()
    shutil.rmtree("nodes", ignore_errors=True)
    dvc_lock_path = pathlib.Path("dvc.lock")
    if rmlock:
        dvc_lock_path.unlink(missing_ok=True)
    # TODO: another check if the dvc.lock is removed!

    status = get_status()
    assert len(status) == 1
    stage = next(n for n in status if n.addressing == "AddNumbers")
    assert stage.status == StageStatus.FINISHED

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

def test_stage_cached_rm_cache(proj):
    proj.repro()
    shutil.rmtree("nodes", ignore_errors=True)
    pathlib.Path("dvc.lock").unlink(missing_ok=True)
    shutil.rmtree(".dvc/cache", ignore_errors=True)

    status = get_status()
    # assert status == {
    #     "AddNumbers": StageStatus.QUEUED,
    # }
    assert len(status) == 1
    stage = next(n for n in status if n.addressing == "AddNumbers")
    assert stage.status == StageStatus.QUEUED

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
