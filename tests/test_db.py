import threading
from unittest.mock import MagicMock, patch

import networkx as nx
import pytest
import zntrack.examples
from sqlalchemy import Engine
from sqlmodel import Session, SQLModel, create_engine, select

from paraffin.db.app import claim_stage, register_worker, save_graph_to_db
from paraffin.db.models import Stage, Worker
from paraffin.dvc import StageStatus, get_status
from paraffin.worker import run_job


@pytest.fixture
def db_engine(proj_path) -> Engine:
    project = zntrack.Project()

    with project:
        _ = zntrack.examples.ParamsToOuts(params=1)
    project.build()

    db_path = "sqlite:///:memory:"
    status_graph: nx.DiGraph = get_status()

    engine = create_engine(db_path)
    SQLModel.metadata.create_all(engine)

    save_graph_to_db(
        engine=engine,
        graph=status_graph,
    )

    return engine


def test_db_graph_conversion(db_engine):
    worker = register_worker(
        name="test_worker",
        machine="test_machine",
        engine=db_engine,
        cwd="test_cwd",
        pid=1234,
    )

    # claim a stage
    with Session(db_engine) as session:
        # select all stages
        stage = claim_stage(
            session=session,
            status=[StageStatus.PENDING],
        )
        assert stage is not None
        # get a worker
        worker = session.exec(select(Worker).where(Worker.id == worker.id)).one()
        job = stage.attach_job(worker=worker)
        session.add(job)
        session.commit()
        session.refresh(job)
        session.refresh(stage)
        session.refresh(worker)

    # now assert that the stage is running

    with Session(db_engine) as session:
        # select all stages
        stage = session.exec(select(Stage).where(Stage.id == stage.id)).one()
        assert stage.status == StageStatus.RUNNING
        assert stage.jobs[0].worker_id == worker.id

    # now try to claim a second stage

    with Session(db_engine) as session:
        # select all stages
        stage_2 = claim_stage(
            session=session,
            status=[StageStatus.PENDING],
        )
        assert stage_2 is None  # all stages are running, max_workers = 1

    # now finish the stage

    # Setup
    shutdown_event = threading.Event()

    # Create a mock Popen object
    mock_proc = MagicMock()
    # Simulate .poll() returning None a few times, then 0 (process finished)
    mock_proc.poll.side_effect = [None, None, 0]
    mock_proc.returncode = 0

    with patch("subprocess.Popen", return_value=mock_proc) as mock_popen:
        # Call your function here that runs the subprocess
        result = run_job(
            engine=db_engine,
            shutdown_event=shutdown_event,
            stage=stage,
            worker=worker,
            job=job,
        )

        # Assertions
        mock_popen.assert_called_once()
        assert result is True  # Or whatever result you expect

    # check that the stage is finished

    with Session(db_engine) as session:
        # select all stages
        stage = session.exec(select(Stage).where(Stage.id == stage.id)).one()
        assert stage.status == StageStatus.FINISHED
        assert stage.jobs[0].worker_id == worker.id
