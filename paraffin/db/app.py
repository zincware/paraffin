import datetime
import fnmatch
import json

import networkx as nx
from dvc.stage.cache import _get_cache_hash
from sqlmodel import (
    Session,
    SQLModel,
    create_engine,
    or_,
    select,
)

from paraffin.db.models import Experiment, Job, Stage, StageDependency, Worker
from paraffin.lock import clean_lock
from paraffin.stage import PipelineStageDC
from paraffin.utils import get_group


def save_graph_to_db(
    graph: nx.DiGraph,
    queues: dict[str, str],
    commit: str,
    origin: str,
    machine: str,
    cache: bool,
    db_url: str,
) -> None:
    engine = create_engine(db_url)
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        experiment = Experiment(base=commit, origin=origin, machine=machine)
        session.add(experiment)
        session.commit()
        for node in nx.topological_sort(graph):
            node: PipelineStageDC
            queue = "default"
            # use fnmatch to match the node name with the custom queues
            for pattern, q in queues.items():
                if fnmatch.fnmatch(node.name, pattern):
                    queue = q
                    break
            status = "pending" if node.changed else "cached"

            job = Stage(
                cmd=json.dumps(node.cmd),
                name=node.name,
                queue=queue,
                status=status,
                experiment_id=experiment.id,
                cache=cache,
                force=node.force,
            )
            # if completed, we can look for the lock and dependency_hash
            if status == "completed":
                # TODO: get the lock and dependency_hash from the stage
                pass

            session.add(job)
            # add dependencies
            for parent in graph.predecessors(node):
                parent_job = session.exec(
                    select(Stage)
                    .where(Stage.experiment_id == experiment.id)
                    .where(Stage.name == parent.name)
                ).one()
                session.add(StageDependency(parent_id=parent_job.id, child_id=job.id))

        session.commit()


def list_experiments(db_url: str, commit: str | None) -> list[dict]:
    engine = create_engine(db_url)
    with Session(engine) as session:
        if commit is not None:
            exps = session.exec(
                select(Experiment).where(Experiment.base == commit)
            ).all()
        else:
            exps = session.exec(select(Experiment)).all()
        return [exp.model_dump() for exp in exps]


def session_to_graph(session: Session, experiment_id: int | None) -> nx.DiGraph:
    """
    Create a directed graph from jobs in the database session,
      keeping Job objects as node data.
    """
    statement = select(Stage)
    if experiment_id:
        statement = statement.where(Stage.experiment_id == experiment_id)
    jobs = session.exec(statement).all()

    graph = nx.DiGraph()
    for job in jobs:
        graph.add_node(job.id, data=job)
        for parent in job.parents:
            graph.add_edge(parent.id, job.id)

    return graph


def db_to_graph(db_url: str, experiment_id: int = 1) -> nx.DiGraph:
    """
    Create a directed graph from the database for a specific experiment,
      resolving Job objects to dictionaries.
    """
    engine = create_engine(db_url)
    with Session(engine) as session:
        # Create the graph using the open session
        graph = session_to_graph(session, experiment_id)

        # Resolve Job objects to dictionaries
        resolved_graph = nx.DiGraph()
        for job_id, node_data in graph.nodes(data=True):
            stage: Stage = node_data["data"]
            resolved_graph.add_node(
                job_id,
                name=stage.name,
                cmd=json.loads(stage.cmd),
                status=stage.status,
                queue=stage.queue,
                lock=json.loads(stage.lockfile_content)
                if stage.lockfile_content
                else None,
                dependency_hash=stage.dependency_hash,
                group=get_group(stage.name)[0],
            )

        # Add edges from the original graph
        resolved_graph.add_edges_from(graph.edges(data=True))

        return resolved_graph


def get_job(
    db_url: str,
    worker_id: int,
    queues: list | None = None,
    experiment: int | None = None,
    stage_name: str | None = None,
) -> tuple[Stage, Job] | None:
    """
    Get the next job where status is 'pending' and all parents are 'completed'.
    """
    engine = create_engine(db_url)
    with Session(engine) as session:
        worker = session.exec(select(Worker).where(Worker.id == worker_id)).one()
        if stage_name is None:
            stages = _fetch_pending_jobs(session, experiment, queues)
        else:
            stages = _fetch_jobs_by_name(session, experiment, queues, stage_name)

        for stage in stages:
            if _all_parents_completed(stage):
                # TODO check if the number of workers on the
                #  job are less than max_workers
                job = stage.attach_job(worker)
                session.add(job)
                session.add(stage)
                session.commit()
                session.refresh(job)
                session.refresh(stage)
                return stage, job

    return None


def _fetch_pending_jobs(
    session: Session, experiment: int | None, queues: list | None
) -> list[Stage]:
    """
    Fetch jobs with 'pending' or 'cached' status, optionally
    filtered by experiment and queues.
    """
    statement = select(Stage).where(
        or_(Stage.status == "pending", Stage.status == "cached")
    )
    if experiment:
        statement = statement.where(Stage.experiment_id == experiment)
    if queues:
        statement = statement.where(Stage.queue.in_(queues))
    statement = statement.with_for_update()
    return session.exec(statement).all()


def _fetch_jobs_by_name(
    session: Session, experiment: int | None, queues: list | None, job_name: str
) -> list[Stage]:
    """
    Fetch jobs by name, including their predecessors, and filter by status and queues.
    """
    graph = session_to_graph(session, experiment)
    statement = select(Stage).where(Stage.name == job_name)
    if experiment:
        statement = statement.where(Stage.experiment_id == experiment)
    jobs = session.exec(statement).all()

    results = []
    for job in jobs:
        predecessors = nx.ancestors(graph, job.id)
        results.extend(
            [graph.nodes[node]["data"] for node in graph.nodes if node in predecessors]
        )
        results.append(job)

    # Filter results that are not "pending" or "cached"
    results = [job for job in results if job.status in ["pending", "cached"]]
    if queues:
        results = [job for job in results if job.queue in queues]
    return results


def _all_parents_completed(stage: Stage) -> bool:
    """
    Check if all parents of a job are completed.
    """
    return all(parent.status == "completed" for parent in stage.parents)


def complete_job(
    stage_id: int,
    lock: dict,
    db_url: str,
    worker_id: int,
    status: str = "completed",
    stderr: str = "",
    stdout: str = "",
):
    engine = create_engine(db_url)
    with Session(engine) as session:
        statement = select(Stage).where(Stage.id == stage_id)
        results = session.exec(statement)
        stage = results.one()
        stage.status = status
        stage.lockfile_content = json.dumps(lock)
        # TODO: this only works for a single worker
        job = session.exec(
            select(Job)
            .where(Job.stage_id == stage_id)
            .where(Job.worker_id == worker_id)
        ).one()
        job.finished_at = datetime.datetime.now()
        if stage.capture_stderr:
            job.stderr = stderr
        if stage.capture_stdout:
            job.stdout = stdout
        stage.finished_at = datetime.datetime.now()
        # We only write the dependency_hash to the database
        #  once the job has finished successfully!
        if status == "completed":
            stage.dependency_hash = _get_cache_hash(clean_lock(lock), key=False)
        session.add(stage)
        session.add(job)
        session.commit()


def update_job_status(
    job_name: str, experiment_id: int, status: str, db_url: str, force: bool
) -> int:
    engine = create_engine(db_url)
    with Session(engine) as session:
        statement = (
            select(Stage)
            .where(Stage.experiment_id == experiment_id)
            .where(Stage.name == job_name)
        )
        results = session.exec(statement)
        job = results.one()
        if job.status == "completed" and not force:
            return -1
        job.status = status
        if force:
            job.force = True
        session.add(job)
        session.commit()
    return 0


def get_job_dump(job_name: str, experiment_id: int, db_url: str) -> dict[str, str]:
    engine = create_engine(db_url)
    with Session(engine) as session:
        statement = (
            select(Stage)
            .where(Stage.experiment_id == experiment_id)
            .where(Stage.name == job_name)
        )
        results = session.exec(statement)
        stage = results.one()
        data = stage.model_dump()
        if len(stage.jobs) == 1:
            data.update({"worker": stage.jobs[0].worker.model_dump()})
        return data


def find_cached_job(db_url: str, deps_cache: str = "") -> Stage | None:
    engine = create_engine(db_url)
    with Session(engine) as session:
        statement = select(Stage).where(Stage.dependency_hash == deps_cache)
        results = session.exec(statement)
        if res := results.first():
            session.refresh(res)
            return res
    return None


def register_worker(name: str, machine: str, db_url: str, cwd: str, pid: int) -> int:
    engine = create_engine(db_url)
    with Session(engine) as session:
        worker = Worker(name=name, machine=machine, cwd=cwd, pid=pid)
        session.add(worker)
        session.commit()
        return worker.id


def update_worker(id: int, status: str, db_url: str) -> None:
    engine = create_engine(db_url)
    with Session(engine) as session:
        worker = session.exec(select(Worker).where(Worker.id == id)).one()
        worker.status = status
        worker.last_seen = datetime.datetime.now()
        session.add(worker)
        session.commit()


def close_worker(id: int, db_url: str) -> None:
    engine = create_engine(db_url)
    with Session(engine) as session:
        worker = session.exec(select(Worker).where(Worker.id == id)).one()
        worker.status = "offline"
        worker.last_seen = datetime.datetime.now()
        worker.finished_at = datetime.datetime.now()
        session.add(worker)
        session.commit()


def list_workers(db_url: str, id: int | None = None) -> list[dict]:
    engine = create_engine(db_url)
    with Session(engine) as session:
        if id is None:
            statement = select(Worker).where(Worker.status != "offline")
            workers = session.exec(statement).all()
        else:
            workers = session.exec(select(Worker).where(Worker.id == id)).all()
        data = []
        for worker in workers:
            _data = worker.model_dump()
            _data["jobs"] = [job.id for job in worker.jobs]
            data.append(_data)
        return data


def get_jobs(db_url: str, experiment_id: int) -> dict[str, int]:
    """Get the number of jobs in each status for a specific experiment."""
    engine = create_engine(db_url)
    with Session(engine) as session:
        statement = select(Stage).where(Stage.experiment_id == experiment_id)
        jobs = session.exec(statement).all()

        status = {"pending": 0, "running": 0, "completed": 0, "cached": 0, "failed": 0}
        for job in jobs:
            status[job.status] += 1

        return status
