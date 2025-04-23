# import datetime
# import fnmatch
import datetime
import json
import typing as t

import networkx as nx

# import networkx as nx
# from dvc.stage.cache import _get_cache_hash
from sqlmodel import (
    Session,
    SQLModel,
    create_engine,
    select,
    text,
)

from paraffin.db.models import (
    Experiment,
    Job,
    Stage,
    StageDependency,
    Worker,
    WorkerStatus,
)
from paraffin.dvc import StageDC, StageStatus

# from paraffin.lock import clean_lock
# from paraffin.stage import PipelineStageDC
# from paraffin.utils import get_group


def save_graph_to_db(graph: nx.DiGraph, db_url: str) -> None:
    engine = create_engine(db_url)
    SQLModel.metadata.create_all(engine)

    # TODO
    commit = "test"
    origin = "test"
    machine = "test"

    with Session(engine) as session:
        experiment = Experiment(base=commit, origin=origin, machine=machine)
        session.add(experiment)
        session.commit()

        for node in nx.topological_sort(graph):
            node: StageDC
            if node.cmd is None:
                continue  # skip everything that is not a PipelineStage
            job = Stage(
                cmd=json.dumps(node.cmd),
                name=node.addressing,
                queue="default",
                status=node.status,
                experiment_id=experiment.id,
                cache=False,
                force=False,
            )
            session.add(job)

            for parent in graph.predecessors(node):
                parent_job = session.exec(
                    select(Stage)
                    .where(Stage.experiment_id == experiment.id)
                    .where(Stage.name == parent.addressing)
                ).one()
                session.add(StageDependency(parent_id=parent_job.id, child_id=job.id))
        session.commit()


def update_job(
    db_url: str,
    stage_id: int,
    status: StageStatus,
) -> None:
    """
    Update the status of a job in the database.
    """
    engine = create_engine(db_url)
    with Session(engine) as session:
        statement = select(Stage).where(Stage.id == stage_id)
        results = session.exec(statement)
        stage = results.one()
        stage.status = status
        session.add(stage)
        session.commit()


def claim_stage(session: Session, status: list[StageStatus]) -> t.Optional[Stage]:
    result = session.exec(
        text(f"""
            UPDATE stage
            SET status = '{StageStatus.RUNNING}'
            WHERE id = (
                SELECT id FROM stage
                WHERE status IN ({",".join(f"'{s}'" for s in status)})
                LIMIT 1
            )
            RETURNING id
        """),
    )
    row = result.first()
    if row:
        return session.exec(select(Stage).where(Stage.id == row[0])).one()
    return None


def get_job(
    db_url: str,
    worker_id: int,
    status: list[StageStatus],
    queues: list | None = None,
    experiment: int | None = None,
    stage_name: str | None = None,
) -> tuple[Stage, Job] | None:
    engine = create_engine(db_url)
    with Session(bind=engine) as session:
        worker = session.exec(select(Worker).where(Worker.id == worker_id)).one()
        stage = claim_stage(session, status=status)

        if stage and _all_parents_completed(stage):
            job = stage.attach_job(worker)
            session.add(job)
            session.add(stage)
            session.commit()
            session.refresh(stage)
            session.refresh(job)
            return stage, job

    return None


def _all_parents_completed(stage: Stage) -> bool:
    """
    Check if all parents of a job are completed.
    """
    return all(
        parent.status in [StageStatus.COMPLETED, StageStatus.FINISHED]
        for parent in stage.parents
    )


def register_worker(name: str, machine: str, db_url: str, cwd: str, pid: int) -> int:
    engine = create_engine(db_url)
    with Session(engine) as session:
        worker = Worker(name=name, machine=machine, cwd=cwd, pid=pid)
        session.add(worker)
        session.commit()
        return worker.id


def close_worker(id: int, db_url: str) -> None:
    engine = create_engine(db_url)
    with Session(engine) as session:
        worker = session.exec(select(Worker).where(Worker.id == id)).one()
        worker.status = WorkerStatus.OFFLINE
        worker.last_seen = datetime.datetime.now()
        worker.finished_at = datetime.datetime.now()
        session.add(worker)
        session.commit()
