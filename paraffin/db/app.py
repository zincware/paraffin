# import datetime
# import fnmatch
import datetime
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
    ExperimentStatus,
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


def query_existing_experiments(
    db_url: str, status: StageStatus, graph: nx.DiGraph
) -> list[Stage]:
    # TODO
    commit = "test"
    origin = "test"
    machine = "test"

    engine = create_engine(db_url)
    SQLModel.metadata.create_all(engine)

    stages = []

    with Session(engine) as session:
        statement = select(Experiment).where(
            Experiment.base == commit,
            Experiment.origin == origin,
            Experiment.machine == machine,
            Experiment.status == ExperimentStatus.ACTIVE,
        )
        results = session.exec(statement)
        experiments = results.all()
        for experiment in experiments:
            # find all jobs that are running, unfinished or finished
            statement = select(Stage).where(
                Stage.experiment_id == experiment.id,
                Stage.status == status,
                Stage.name.in_([node.addressing for node in graph]),
            )
            results = session.exec(statement)
            stages.extend(results.all())
    return stages


def update_existing_experiment_stages(db_url: str) -> None:
    # TODO: instead of updating the stages we can keep that information and update the experiment!
    commit = "test"
    origin = "test"
    machine = "test"

    engine = create_engine(db_url)
    SQLModel.metadata.create_all(engine)

    with Session(engine) as session:
        statement = select(Experiment).where(
            Experiment.base == commit,
            Experiment.origin == origin,
            Experiment.machine == machine,
            Experiment.status == ExperimentStatus.ACTIVE,
        )
        results = session.exec(statement).all()
        for experiment in results:
            # find all jobs that are running, unfinished or finished
            experiment.status = ExperimentStatus.INACTIVE
            session.add(experiment)
        session.commit()


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
                cmd=node.cmd,
                name=node.addressing,
                queue="default",
                status=node.status,
                experiment_id=experiment.id,
                cache=False,
                force=False,
                path=node.path,
                lockfile_content=node.lockfile,
            )
            session.add(job)

            for parent in graph.predecessors(node):
                parent_job = session.exec(
                    select(Stage)
                    .where(Stage.experiment_id == experiment.id)
                    .where(Stage.name == parent.addressing)
                ).all()
                if len(parent_job) == 1:
                    # if the previous stage is not PipelineStage, we skip it
                    session.add(
                        StageDependency(parent_id=parent_job[0].id, child_id=job.id)
                    )
        session.commit()


def update_job(
    db_url: str,
    stage_id: int,
    status: StageStatus,
    **kwargs: t.Any,
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
        for key, value in kwargs.items():
            if hasattr(stage, key):
                setattr(stage, key, value)
        session.add(stage)
        session.commit()


def claim_stage(session: Session, status: list[StageStatus]) -> t.Optional[Stage]:
    # TODO
    commit = "test"
    origin = "test"
    machine = "test"

    result = session.exec(
        text(f"""
        UPDATE stage
        SET status = '{StageStatus.RUNNING}'
        WHERE id = (
            SELECT s.id FROM stage s
            JOIN experiment e ON s.experiment_id = e.id
            WHERE s.status IN ({",".join(f"'{s}'" for s in status)})
              AND e.status = '{ExperimentStatus.ACTIVE}'
              AND e.base = '{commit}'
              AND e.machine = '{machine}'
              AND e.origin = '{origin}'
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


def register_worker(
    name: str,
    machine: str,
    db_url: str,
    cwd: str,
    pid: int,
    requires_dvc_lock: bool = False,
) -> int:
    engine = create_engine(db_url)
    with Session(engine) as session:
        worker = Worker(
            name=name,
            machine=machine,
            cwd=cwd,
            pid=pid,
            requires_dvc_lock=requires_dvc_lock,
        )
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


def get_stage_status(
    db_url: str,
    stage_name: str | None = None,
) -> StageStatus:
    """
    Get the status of a stage in the database.
    """
    engine = create_engine(db_url)
    with Session(engine) as session:
        statement = select(Stage).where(Stage.name == stage_name)
        results = session.exec(statement)
        stage = results.one()
        return stage.status


def export_db_to_graph(db_url: str, experiment_id: int = 1) -> nx.DiGraph:
    engine = create_engine(db_url)
    SQLModel.metadata.create_all(engine)

    with Session(engine) as session:
        statement = select(Stage).where(Stage.experiment_id == experiment_id)
        results = session.exec(statement)
        stages = results.all()

        graph = nx.DiGraph()
        stage_nodes = {}

        # Create and store StageDC nodes
        for stage in stages:
            node = StageDC(
                addressing=stage.name,
                status=stage.status,
                cmd=stage.cmd,
                path=stage.path,
                lockfile=stage.lockfile_content,
            )
            graph.add_node(node)
            stage_nodes[stage.name] = node

        # Add edges between StageDC nodes
        for stage in stages:
            for parent in stage.parents:
                graph.add_edge(stage_nodes[parent.name], stage_nodes[stage.name])

    return graph


def list_experiments(db_url: str) -> list[dict]:
    # return [{"created_at": 1234567890, "base": "test", "origin": "test", "id": "1", "machine": "test"}]
    engine = create_engine(db_url)

    with Session(engine) as session:
        statement = select(Experiment)
        results = session.exec(statement)
        experiments = results.all()
        return [
            {
                "created_at": experiment.created_at,
                "base": experiment.base,
                "origin": experiment.origin,
                "id": experiment.id,
                "machine": experiment.machine,
                "status": experiment.status,
            }
            for experiment in experiments
        ]


def list_stages(db_url: str, experiment_id: int) -> list[dict]:
    engine = create_engine(db_url)

    with Session(engine) as session:
        statement = select(Stage).where(Stage.experiment_id == experiment_id)
        results = session.exec(statement)
        stages = results.all()
        return [
            {
                "id": stage.id,
                "name": stage.name,
                "status": stage.status,
            }
            for stage in stages
        ]


def get_stage_by_id(db_url: str, stage_id: int) -> StageDC:
    engine = create_engine(db_url)

    with Session(engine) as session:
        statement = select(Stage).where(Stage.id == stage_id)
        results = session.exec(statement)
        stage = results.one()
        return StageDC(
            addressing=stage.name,
            status=stage.status,
            cmd=stage.cmd,
            path=stage.path,
            lockfile=stage.lockfile_content,
        )
