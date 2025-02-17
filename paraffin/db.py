import datetime
import fnmatch
import json
from typing import List, Literal, Optional

import networkx as nx
from dvc.stage.cache import _get_cache_hash
from sqlmodel import (
    Field,
    Relationship,
    Session,
    SQLModel,
    String,
    create_engine,
    or_,
    select,
)

from paraffin.lock import clean_lock
from paraffin.stage import PipelineStageDC
from paraffin.utils import get_group


class Worker(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    machine: str
    status: Literal["running", "idle", "offline"] = Field(
        sa_type=String, default="idle"
    )
    last_seen: datetime.datetime = Field(default_factory=datetime.datetime.now)

    # Relationships
    jobs: List["Job"] = Relationship(back_populates="worker")
    cwd: str = ""  # Current working directory
    pid: int = 0  # Process ID
    started_at: datetime.datetime = Field(default_factory=datetime.datetime.now)
    finished_at: Optional[datetime.datetime] = None


class JobDependency(SQLModel, table=True):
    parent_id: Optional[int] = Field(foreign_key="job.id", primary_key=True)
    child_id: Optional[int] = Field(foreign_key="job.id", primary_key=True)


class Experiment(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    base: str  # Commit this experiment is based on
    origin: str = "local"  # Origin of the repository, e.g. https://...
    machine: str = "local"  # Machine where the experiment was submitted from
    created_at: datetime.datetime = Field(default_factory=datetime.datetime.now)

    # Relationships
    jobs: List["Job"] = Relationship(back_populates="experiment")


class Job(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    cmd: str  # Command to execute
    status: Literal["pending", "running", "completed", "cached", "failed"] = Field(
        sa_type=String, default="pending"
    )
    queue: str = "default"  # Queue name(s)
    lock: str = ""  # JSON string of lockfile for the stage
    deps_hash: str = ""  # Hash of the dependencies
    experiment_id: Optional[int] = Field(foreign_key="experiment.id")
    stderr: str = ""  # stderr output
    stdout: str = ""  # stdout output
    capture_stderr: bool = True  # Capture stderr output
    capture_stdout: bool = True  # Capture stdout output
    started_at: Optional[datetime.datetime] = None
    finished_at: Optional[datetime.datetime] = None
    worker_id: Optional[int] = Field(foreign_key="worker.id", default=None)
    cache: bool = False  # Use the paraffin cache for this job
    force: bool = False  # rerun the job even if cached

    # Relationships
    experiment: Optional[Experiment] = Relationship(back_populates="jobs")
    worker: Optional[Worker] = Relationship(back_populates="jobs")
    parents: List["Job"] = Relationship(
        link_model=JobDependency,
        back_populates="children",
        sa_relationship_kwargs={
            "primaryjoin": "Job.id==JobDependency.child_id",
            "secondaryjoin": "Job.id==JobDependency.parent_id",
        },
    )
    children: List["Job"] = Relationship(
        link_model=JobDependency,
        back_populates="parents",
        sa_relationship_kwargs={
            "primaryjoin": "Job.id==JobDependency.parent_id",
            "secondaryjoin": "Job.id==JobDependency.child_id",
        },
    )


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

            job = Job(
                cmd=json.dumps(node.cmd),
                name=node.name,
                queue=queue,
                status=status,
                experiment_id=experiment.id,
                cache=cache,
                force=node.force,
            )
            # if completed, we can look for the lock and deps_hash
            if status == "completed":
                # TODO: get the lock and deps_hash from the stage
                pass

            session.add(job)
            # add dependencies
            for parent in graph.predecessors(node):
                parent_job = session.exec(
                    select(Job)
                    .where(Job.experiment_id == experiment.id)
                    .where(Job.name == parent.name)
                ).one()
                session.add(JobDependency(parent_id=parent_job.id, child_id=job.id))

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
    statement = select(Job)
    if experiment_id:
        statement = statement.where(Job.experiment_id == experiment_id)
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
            job = node_data["data"]
            resolved_graph.add_node(
                job_id,
                name=job.name,
                cmd=json.loads(job.cmd),
                status=job.status,
                queue=job.queue,
                lock=json.loads(job.lock) if job.lock else None,
                deps_hash=job.deps_hash,
                group=get_group(job.name)[0],
            )

        # Add edges from the original graph
        resolved_graph.add_edges_from(graph.edges(data=True))

        return resolved_graph


def get_job(
    db_url: str,
    worker_id: int,
    queues: list | None = None,
    experiment: int | None = None,
    job_name: str | None = None,
) -> dict | None:
    """
    Get the next job where status is 'pending' and all parents are 'completed'.
    """
    engine = create_engine(db_url)
    with Session(engine) as session:
        if job_name is None:
            results = _fetch_pending_jobs(session, experiment, queues)
        else:
            results = _fetch_jobs_by_name(session, experiment, queues, job_name)

        for job in results:
            if _all_parents_completed(job):
                _update_job_status(session, job, worker_id)
                return _job_to_dict(job)
    return None


def _fetch_pending_jobs(
    session: Session, experiment: int | None, queues: list | None
) -> list:
    """
    Fetch jobs with 'pending' or 'cached' status, optionally
    filtered by experiment and queues.
    """
    statement = select(Job).where(or_(Job.status == "pending", Job.status == "cached"))
    if experiment:
        statement = statement.where(Job.experiment_id == experiment)
    if queues:
        statement = statement.where(Job.queue.in_(queues))
    statement = statement.with_for_update()
    return session.exec(statement).all()


def _fetch_jobs_by_name(
    session: Session, experiment: int | None, queues: list | None, job_name: str
) -> list:
    """
    Fetch jobs by name, including their predecessors, and filter by status and queues.
    """
    graph = session_to_graph(session, experiment)
    statement = select(Job).where(Job.name == job_name)
    if experiment:
        statement = statement.where(Job.experiment_id == experiment)
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


def _all_parents_completed(job) -> bool:
    """
    Check if all parents of a job are completed.
    """
    return all(parent.status == "completed" for parent in job.parents)


def _update_job_status(session: Session, job, worker_id: int) -> None:
    """
    Update the job status to 'running' and set the worker ID and start time.
    """
    job.status = "running"
    job.started_at = datetime.datetime.now()
    job.worker_id = worker_id
    session.add(job)
    session.commit()


def _job_to_dict(job) -> dict:
    """
    Convert a Job object to a dictionary.
    """
    return {
        "id": job.id,
        "name": job.name,
        "cmd": json.loads(job.cmd),
        "queue": job.queue,
        "status": job.status,
        "cache": job.cache,
        "force": job.force,
    }


def complete_job(
    job_id: int,
    lock: dict,
    db_url: str,
    status: str = "completed",
    stderr: str = "",
    stdout: str = "",
):
    engine = create_engine(db_url)
    with Session(engine) as session:
        statement = select(Job).where(Job.id == job_id)
        results = session.exec(statement)
        job = results.one()
        job.status = status
        job.lock = json.dumps(lock)
        if job.capture_stderr:
            job.stderr = stderr
        if job.capture_stdout:
            job.stdout = stdout
        job.finished_at = datetime.datetime.now()
        # We only write the deps_hash to the database
        #  once the job has finished successfully!
        if status == "completed":
            job.deps_hash = _get_cache_hash(clean_lock(lock), key=False)
        session.add(job)
        session.commit()


def update_job_status(
    job_name: str, experiment_id: int, status: str, db_url: str, force: bool
) -> int:
    engine = create_engine(db_url)
    with Session(engine) as session:
        statement = (
            select(Job)
            .where(Job.experiment_id == experiment_id)
            .where(Job.name == job_name)
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
            select(Job)
            .where(Job.experiment_id == experiment_id)
            .where(Job.name == job_name)
        )
        results = session.exec(statement)
        job = results.one()
        data = job.model_dump()
        if job.worker:
            data.update({"worker": job.worker.model_dump()})
        return data


def find_cached_job(db_url: str, deps_cache: str = "") -> dict:
    engine = create_engine(db_url)
    with Session(engine) as session:
        statement = select(Job).where(Job.deps_hash == deps_cache)
        results = session.exec(statement)
        if res := results.first():
            return res.model_dump()
    return {}


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
        statement = select(Job).where(Job.experiment_id == experiment_id)
        jobs = session.exec(statement).all()

        status = {"pending": 0, "running": 0, "completed": 0, "cached": 0, "failed": 0}
        for job in jobs:
            status[job.status] += 1

        return status
