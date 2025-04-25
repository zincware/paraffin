import dataclasses
import threading
from unittest.mock import MagicMock, patch

import networkx as nx
import pytest
import zntrack.examples
from sqlalchemy import Engine
from sqlmodel import Session, SQLModel, create_engine, select

from paraffin.db.app import save_graph_to_db
from paraffin.db.models import Stage, Worker
from paraffin.dvc import StageStatus, get_status


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


@pytest.fixture
def db_engine_parallel(proj_path) -> Engine:
    project = zntrack.Project()

    with project:
        _ = zntrack.examples.ParamsToOuts(params=1)
    project.build()

    db_path = "sqlite:///:memory:"
    status_graph: nx.DiGraph = get_status()
    mapping = {}
    for node in status_graph:
        mapping[node] = dataclasses.replace(
            node,
            max_workers=2,
        )
    status_graph = nx.relabel_nodes(status_graph, mapping)

    engine = create_engine(db_path)
    SQLModel.metadata.create_all(engine)

    save_graph_to_db(
        engine=engine,
        graph=status_graph,
    )

    return engine


def test_db(db_engine: Engine):
    worker = Worker.register(
        name="test_worker",
        machine="test_machine",
        engine=db_engine,
        cwd="test_cwd",
        pid=1234,
    )

    # claim a stage
    with Session(db_engine) as session:
        stage = session.exec(select(Stage).where(Stage.id == 1)).one()
        assert stage.status == StageStatus.PENDING
        assert stage.finished_at is None
        assert stage.started_at is None
        assert stage.jobs == []

        # select all stages
        job = Stage.claim(
            session=session,
            worker_id=worker.id,
        )
        assert job is not None
        assert job.stage is not None
        assert job.stage.finished_at is None
        assert job.stage.started_at is not None

        finished_stage = Stage.claim_finished(session=session)
        assert finished_stage is None

    # now assert that the stage is running

    with Session(db_engine) as session:
        # select all stages
        stage = session.exec(select(Stage).where(Stage.id == stage.id)).one()
        assert stage.status == StageStatus.RUNNING
        assert stage.jobs[0].worker_id == worker.id

    # now try to claim a second stage

    with Session(db_engine) as session:
        # select all stages
        stage_2 = Stage.claim(
            session=session,
            worker_id=worker.id,
        )
        assert stage_2 is None  # all stages are running, max_workers = 1

    # now finish the stage

    shutdown_event = threading.Event()
    mock_proc = MagicMock()
    mock_proc.poll.side_effect = [None, None, 0]
    mock_proc.returncode = 0

    with patch("subprocess.Popen", return_value=mock_proc) as mock_popen:
        # Call your function here that runs the subprocess
        result = job.run(
            engine=db_engine,
            shutdown_event=shutdown_event,
            worker=worker,
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
        assert stage.finished_at is not None
        assert stage.started_at is not None

        finished_stage = Stage.claim_finished(session=session)
        assert finished_stage is not None
        assert finished_stage.id == stage.id


def test_db_parallel_finished(db_engine_parallel: Engine):
    w1 = Worker.register(
        name="test_worker",
        machine="test_machine",
        engine=db_engine_parallel,
        cwd="test_cwd",
        pid=1234,
    )
    w2 = Worker.register(
        name="test_worker",
        machine="test_machine",
        engine=db_engine_parallel,
        cwd="test_cwd",
        pid=1234,
    )
    w3 = Worker.register(
        name="test_worker",
        machine="test_machine",
        engine=db_engine_parallel,
        cwd="test_cwd",
        pid=1234,
    )

    # claim a stage
    with Session(db_engine_parallel) as session:
        # select all stages
        job_1 = Stage.claim(
            session=session,
            worker_id=w1.id,
        )  # TODO: this should probably return a job?
        assert job_1 is not None
        job_2 = Stage.claim(
            session=session,
            worker_id=w2.id,
        )
        assert job_2 is not None
        assert job_2.id != job_1.id
        assert job_2.stage.id == job_1.stage.id

        job_3 = Stage.claim(
            session=session,
            worker_id=w3.id,
        )
        assert job_3 is None

        assert job_1.worker_id != job_2.worker_id
        assert job_1.worker_id == w1.id
        assert job_2.worker_id == w2.id
        assert job_1.id != job_2.id

        session.commit()
        session.refresh(job_1)
        session.refresh(job_2)

    with Session(db_engine_parallel) as session:
        stage_1 = session.exec(select(Stage).where(Stage.id == 1)).one()
        assert stage_1.status == StageStatus.RUNNING
        assert stage_1.started_at == job_1.started_at
        assert stage_1.finished_at is None
        assert stage_1.jobs[0].id == job_1.id
        assert stage_1.jobs[1].id == job_2.id
        assert len(stage_1.jobs) == 2

    job_1.set_finished(engine=db_engine_parallel)
    job_2.set_finished(engine=db_engine_parallel)

    with Session(db_engine_parallel) as session:
        stage_1 = session.exec(select(Stage).where(Stage.id == 1)).one()
        assert stage_1.status == StageStatus.FINISHED
        assert stage_1.finished_at is not None

    # try to claim a stage again
    with Session(db_engine_parallel) as session:
        # select all stages
        job_3 = Stage.claim(
            session=session,
            worker_id=w1.id,
        )
        assert job_3 is None


def test_db_parallel_first_failed(db_engine_parallel: Engine):
    w1 = Worker.register(
        name="test_worker",
        machine="test_machine",
        engine=db_engine_parallel,
        cwd="test_cwd",
        pid=1234,
    )
    w2 = Worker.register(
        name="test_worker",
        machine="test_machine",
        engine=db_engine_parallel,
        cwd="test_cwd",
        pid=1234,
    )

    # claim a stage
    with Session(db_engine_parallel) as session:
        # select all stages
        job_1 = Stage.claim(
            session=session,
            worker_id=w1.id,
        )
        job_2 = Stage.claim(
            session=session,
            worker_id=w2.id,
        )

        session.commit()
        session.refresh(job_1)
        session.refresh(job_2)

    assert job_1 is not None
    assert job_2 is not None

    job_1.set_failed(engine=db_engine_parallel)
    job_2.set_finished(engine=db_engine_parallel)

    with Session(db_engine_parallel) as session:
        stage_1 = session.exec(select(Stage).where(Stage.id == 1)).one()
        assert stage_1.status == StageStatus.FAILED
        assert stage_1.finished_at is not None
        assert stage_1.started_at is not None
        assert len(stage_1.jobs) == 2
        assert stage_1.jobs[0].status == StageStatus.FAILED
        assert stage_1.jobs[1].status == StageStatus.FINISHED


def test_db_parallel_last_failed(db_engine_parallel: Engine):
    w1 = Worker.register(
        name="test_worker",
        machine="test_machine",
        engine=db_engine_parallel,
        cwd="test_cwd",
        pid=1234,
    )
    w2 = Worker.register(
        name="test_worker",
        machine="test_machine",
        engine=db_engine_parallel,
        cwd="test_cwd",
        pid=1234,
    )

    # claim a stage
    with Session(db_engine_parallel) as session:
        # select all stages
        job_1 = Stage.claim(
            session=session,
            worker_id=w1.id,
        )
        job_2 = Stage.claim(
            session=session,
            worker_id=w2.id,
        )

        session.commit()
        session.refresh(job_1)
        session.refresh(job_2)

    assert job_1 is not None
    assert job_2 is not None

    job_1.set_finished(engine=db_engine_parallel)
    job_2.set_failed(engine=db_engine_parallel)

    with Session(db_engine_parallel) as session:
        stage_1 = session.exec(select(Stage).where(Stage.id == 1)).one()
        assert stage_1.status == StageStatus.FAILED
        assert stage_1.finished_at is not None
        assert stage_1.started_at is not None
        assert len(stage_1.jobs) == 2
        assert stage_1.jobs[0].status == StageStatus.FINISHED
        assert stage_1.jobs[1].status == StageStatus.FAILED


def test_db_unfinished_continue(db_engine: Engine):
    w1 = Worker.register(
        name="test_worker",
        machine="test_machine",
        engine=db_engine,
        cwd="test_cwd",
        pid=1234,
    )

    with Session(db_engine) as session:
        # select all stages
        job_1 = Stage.claim(
            session=session,
            worker_id=w1.id,
        )

        session.commit()
        session.refresh(job_1)

    assert job_1 is not None

    job_1.set_unfinished(engine=db_engine)

    with Session(db_engine) as session:
        stage_1 = session.exec(select(Stage).where(Stage.id == 1)).one()
        assert stage_1.status == StageStatus.UNFINISHED
        assert stage_1.assigned_workers == 0

    # claim again and finish this time

    with Session(db_engine) as session:
        # select all stages
        job_2 = Stage.claim(
            session=session,
            worker_id=w1.id,
        )
        assert job_2 is not None
        assert job_2.stage is not None
        assert job_2.stage.assigned_workers == 1

        session.commit()
        session.refresh(job_2)

    assert job_2.id != job_1.id

    job_2.set_finished(engine=db_engine)

    with Session(db_engine) as session:
        stage_1 = session.exec(select(Stage).where(Stage.id == 1)).one()
        assert stage_1.status == StageStatus.FINISHED
        assert stage_1.finished_at is not None
        assert stage_1.started_at is not None
        assert len(stage_1.jobs) == 2
        assert stage_1.jobs[0].status == StageStatus.UNFINISHED
        assert stage_1.jobs[1].status == StageStatus.FINISHED
        assert stage_1.assigned_workers == 0


def test_db_unfinished_failed(db_engine: Engine):
    w1 = Worker.register(
        name="test_worker",
        machine="test_machine",
        engine=db_engine,
        cwd="test_cwd",
        pid=1234,
    )

    with Session(db_engine) as session:
        # select all stages
        job_1 = Stage.claim(
            session=session,
            worker_id=w1.id,
        )

        session.commit()
        session.refresh(job_1)

    assert job_1 is not None

    job_1.set_unfinished(engine=db_engine)

    with Session(db_engine) as session:
        stage_1 = session.exec(select(Stage).where(Stage.id == 1)).one()
        assert stage_1.status == StageStatus.UNFINISHED
        assert stage_1.assigned_workers == 0

    # claim again and finish this time

    with Session(db_engine) as session:
        # select all stages
        job_2 = Stage.claim(
            session=session,
            worker_id=w1.id,
        )
        assert job_2 is not None
        assert job_2.stage is not None
        assert job_2.stage.assigned_workers == 1

        session.commit()
        session.refresh(job_2)

    assert job_2.id != job_1.id

    job_2.set_failed(engine=db_engine)

    with Session(db_engine) as session:
        stage_1 = session.exec(select(Stage).where(Stage.id == 1)).one()
        assert stage_1.status == StageStatus.FAILED
        assert stage_1.finished_at is not None
        assert stage_1.started_at is not None
        assert len(stage_1.jobs) == 2
        assert stage_1.jobs[0].status == StageStatus.UNFINISHED
        assert stage_1.jobs[1].status == StageStatus.FAILED


def test_db_parallel_unfinished_first_finished(db_engine_parallel: Engine):
    w1 = Worker.register(
        name="test_worker",
        machine="test_machine",
        engine=db_engine_parallel,
        cwd="test_cwd",
        pid=1234,
    )
    w2 = Worker.register(
        name="test_worker",
        machine="test_machine",
        engine=db_engine_parallel,
        cwd="test_cwd",
        pid=1234,
    )

    # claim a stage
    with Session(db_engine_parallel) as session:
        # select all stages
        job_1 = Stage.claim(
            session=session,
            worker_id=w1.id,
        )
        job_2 = Stage.claim(
            session=session,
            worker_id=w2.id,
        )

        session.commit()
        session.refresh(job_1)
        session.refresh(job_2)

    assert job_1 is not None
    assert job_2 is not None

    job_1.set_finished(engine=db_engine_parallel)
    job_2.set_unfinished(engine=db_engine_parallel)

    with Session(db_engine_parallel) as session:
        stage_1 = session.exec(select(Stage).where(Stage.id == 1)).one()
        assert stage_1.status == StageStatus.UNFINISHED
        assert stage_1.finished_at is None
        assert stage_1.started_at is not None
        assert len(stage_1.jobs) == 2
        assert stage_1.jobs[0].status == StageStatus.FINISHED
        assert stage_1.jobs[1].status == StageStatus.UNFINISHED
        assert stage_1.assigned_workers == 0


def test_db_parallel_unfinished_last_finished(db_engine_parallel: Engine):
    w1 = Worker.register(
        name="test_worker",
        machine="test_machine",
        engine=db_engine_parallel,
        cwd="test_cwd",
        pid=1234,
    )
    w2 = Worker.register(
        name="test_worker",
        machine="test_machine",
        engine=db_engine_parallel,
        cwd="test_cwd",
        pid=1234,
    )

    # claim a stage
    with Session(db_engine_parallel) as session:
        # select all stages
        job_1 = Stage.claim(
            session=session,
            worker_id=w1.id,
        )
        job_2 = Stage.claim(
            session=session,
            worker_id=w2.id,
        )

        session.commit()
        session.refresh(job_1)
        session.refresh(job_2)

    assert job_1 is not None
    assert job_2 is not None

    job_1.set_unfinished(engine=db_engine_parallel)
    job_2.set_finished(engine=db_engine_parallel)
    # the last job will only finish if everything is finished!

    with Session(db_engine_parallel) as session:
        stage_1 = session.exec(select(Stage).where(Stage.id == 1)).one()
        assert stage_1.status == StageStatus.FINISHED
        assert stage_1.finished_at is not None
        assert stage_1.started_at is not None
        assert len(stage_1.jobs) == 2
        assert stage_1.jobs[0].status == StageStatus.UNFINISHED
        assert stage_1.jobs[1].status == StageStatus.FINISHED
