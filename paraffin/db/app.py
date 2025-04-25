# import datetime
# import fnmatch
import typing as t

import networkx as nx
from sqlalchemy import Engine

# import networkx as nx
# from dvc.stage.cache import _get_cache_hash
from sqlmodel import (
    Session,
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
)
from paraffin.dvc import StageDC, StageStatus

# from paraffin.lock import clean_lock
# from paraffin.stage import PipelineStageDC
# from paraffin.utils import get_group


def query_existing_experiments(
    engine: Engine, status: StageStatus, graph: nx.DiGraph
) -> list[Stage]:
    # TODO
    commit = "test"
    origin = "test"
    machine = "test"

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


def update_existing_experiment_stages(engine: Engine) -> None:
    # TODO: instead of updating the stages we can keep that information and update the experiment!
    commit = "test"
    origin = "test"
    machine = "test"

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


def save_graph_to_db(graph: nx.DiGraph, engine: Engine) -> None:
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
                max_workers=node.max_workers,
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
    engine: Engine,
    stage_id: int,
    status: StageStatus,
    **kwargs: t.Any,
) -> None:
    """
    Update the status of a job in the database.
    """
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


def claim_stage_parallel(
    session: Session,
) -> t.Optional[Stage]:
    """Claim a stage for parallel execution."""
    commit = "test"
    origin = "test"
    machine = "test"

    result = session.exec(
        text(f"""
            UPDATE stage
            SET assigned_workers = assigned_workers + 1
            WHERE stage.id = (
                SELECT stage.id
                FROM stage
            JOIN experiment ON stage.experiment_id = experiment.id
            WHERE stage.status = '{StageStatus.RUNNING}'
            AND experiment.status = '{ExperimentStatus.ACTIVE}'
            AND experiment.base = '{commit}'
            AND experiment.machine = '{machine}'
            AND experiment.origin = '{origin}'
            AND stage.assigned_workers < stage.max_workers
            LIMIT 1
        )
        RETURNING id;
    """),
    )
    row = result.first()
    if row:
        return session.exec(select(Stage).where(Stage.id == row[0])).one()
    return None


def get_job(
    engine: Engine,
    worker: Worker,
    status: list[StageStatus],
    queues: list | None = None,
    experiment: int | None = None,
    stage_name: str | None = None,
) -> tuple[Stage, Job] | None:
    with Session(bind=engine) as session:
        worker = session.exec(select(Worker).where(Worker.id == worker.id)).one()
        stage = claim_stage(session, status=status)
        # TODO: don't we need to rollback the SET status = '{StageStatus.RUNNING}' if _all_parents_completed is false?
        if stage and _all_parents_completed(stage):
            job = stage.attach_job(worker)
            session.add(job)
            session.add(stage)
            session.commit()
            session.refresh(stage)
            session.refresh(job)
            return stage, job
        else:
            parallel_stage = claim_stage_parallel(session)
            if parallel_stage and _all_parents_completed(parallel_stage):
                print(f"Claimed stage {parallel_stage.name} for parallel execution")
                job = parallel_stage.attach_job(worker)
                session.add(job)
                session.add(parallel_stage)
                session.commit()
                session.refresh(parallel_stage)
                session.refresh(job)
                return parallel_stage, job

    return None


def _all_parents_completed(stage: Stage) -> bool:
    """
    Check if all parents of a job are completed.
    """
    return all(
        parent.status in [StageStatus.COMPLETED, StageStatus.FINISHED]
        for parent in stage.parents
    )


def get_stage_status(
    engine: Engine,
    stage_name: str | None = None,
) -> StageStatus:
    """
    Get the status of a stage in the database.
    """
    with Session(engine) as session:
        statement = select(Stage).where(Stage.name == stage_name)
        results = session.exec(statement)
        stage = results.one()
        return stage.status


def export_db_to_graph(engine: Engine, experiment_id: int = 1) -> nx.DiGraph:
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


def list_experiments(engine: Engine) -> list[dict]:
    # return [{"created_at": 1234567890, "base": "test", "origin": "test", "id": "1", "machine": "test"}]
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


def list_stages(engine: Engine, experiment_id: int) -> list[dict]:
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


def get_stage_by_id(engine: Engine, stage_id: int) -> StageDC:
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
