import datetime
import fnmatch
import json
from typing import List, Optional

import networkx as nx
from dvc.stage.cache import _get_cache_hash
from sqlmodel import Field, Relationship, Session, SQLModel, create_engine, select

from paraffin.stage import PipelineStageDC


class JobDependency(SQLModel, table=True):
    parent_id: Optional[int] = Field(foreign_key="job.id", primary_key=True)
    child_id: Optional[int] = Field(foreign_key="job.id", primary_key=True)


class Experiment(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    base: str  # Commit this experiment is based on
    origin: str = "local"  # Origin of the repository, e.g. https://...
    machine: str = "local"  # Machine where the experiment was submitted from
    created_at: datetime.datetime = Field(default_factory=datetime.datetime.now)


class Job(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    cmd: str  # Command to execute
    status: str = "pending"  # pending, running, completed, failed
    queue: str = "default"  # Queue name(s)
    lock: str = ""  # JSON string of lockfile for the stage
    deps_lock: str = ""  # JSON string of lockfile for the dependencies
    deps_hash: str = ""  # Hash of the dependencies
    experiment_id: Optional[int] = Field(foreign_key="experiment.id")
    stderr: str = ""  # stderr output
    stdout: str = ""  # stdout output

    # Relationships
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
    graph: nx.DiGraph, queues: dict[str, str], commit: str, origin: str, machine: str
) -> None:
    engine = create_engine("sqlite:///jobs.db")
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
            job = Job(
                cmd=node.cmd,
                name=node.name,
                queue=queue,
                status="pending" if node.changed else "completed",
                experiment_id=experiment.id,
            )
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


def list_experiments(db: str = "sqlite:///jobs.db", commit: str = "HEAD") -> list[dict]:
    engine = create_engine(db)
    with Session(engine) as session:
        exps = session.exec(select(Experiment).where(Experiment.base == commit)).all()
        return [exp.model_dump() for exp in exps]


def db_to_graph(db: str = "sqlite:///jobs.db", experiment_id: int = 1) -> nx.DiGraph:
    engine = create_engine(db)
    with Session(engine) as session:
        # TODO: select the correct experiment!
        jobs = session.exec(select(Job).where(Job.experiment_id == experiment_id)).all()
        graph = nx.DiGraph()
        for job in jobs:
            graph.add_node(
                job.id,
                **{
                    "name": job.name,
                    "cmd": job.cmd,
                    "status": job.status,
                    "queue": job.queue,
                    "lock": json.loads(job.lock) if job.lock else None,
                    "deps_lock": json.loads(job.deps_lock) if job.deps_lock else None,
                    "deps_hash": job.deps_hash,
                    "group": get_group(job.name),
                },
            )
            for parent in job.parents:
                graph.add_edge(parent.id, job.id)

    return graph


def get_group(name: str) -> list[str]:
    """Extract the group from the job name."""
    parts = name.split("_")
    # check if parts[-1] is a number
    if parts[-1].isdigit():
        return parts[:-2]
    return parts[:-1]


def get_job(db: str = "sqlite:///jobs.db", queues: list | None = None) -> dict | None:
    """
    Get the next job where status is 'pending' and all parents are 'completed'.
    """
    engine = create_engine(db)
    with Session(engine) as session:
        # Fetch jobs with 'pending' status, eagerly loading their parents
        statement = select(Job).where(Job.status == "pending")
        if queues:
            statement = statement.where(Job.queue.in_(queues))
        results = session.exec(statement)

        # Process each job to check if all parents are completed
        for job in results:
            if all(parent.status == "completed" for parent in job.parents):
                job.status = "running"
                session.add(job)
                session.commit()
                return {
                    "id": job.id,
                    "name": job.name,
                    "cmd": job.cmd,
                    "queue": job.queue,
                    "status": job.status,
                }
    return None


def set_job_deps_lock(job_id: int, lock: dict, db: str = "sqlite:///jobs.db"):
    engine = create_engine(db)

    reduced_lock = {}

    if x := lock.get("cmd"):
        reduced_lock["cmd"] = x
    if x := lock.get("params"):
        reduced_lock["params"] = x
    if x := lock.get("deps"):
        reduced_lock["deps"] = x

    with Session(engine) as session:
        statement = select(Job).where(Job.id == job_id)
        results = session.exec(statement)
        job = results.one()
        job.deps_lock = json.dumps(reduced_lock)
        job.deps_hash = _get_cache_hash(reduced_lock)
        session.add(job)
        session.commit()


def complete_job(
    job_id: int,
    lock: dict,
    db: str = "sqlite:///jobs.db",
    status: str = "completed",
    stderr: str = "",
    stdout: str = "",
):
    engine = create_engine(db)
    with Session(engine) as session:
        statement = select(Job).where(Job.id == job_id)
        results = session.exec(statement)
        job = results.one()
        job.status = status
        job.lock = json.dumps(lock)
        job.stderr = stderr
        job.stdout = stdout
        session.add(job)
        session.commit()


def get_nodes_and_edges(db: str = "sqlite:///jobs.db") -> tuple[list, list]:
    engine = create_engine(db)
    with Session(engine) as session:
        jobs = session.exec(select(Job)).all()
        nodes = []
        edges = []
        for job in jobs:
            nodes.append(
                {
                    "id": str(job.id),
                    "label": job.name,
                    "status": job.status,
                    "queue": job.queue,
                    "lock": json.loads(job.lock) if job.lock else None,
                    "deps_lock": json.loads(job.deps_lock) if job.deps_lock else None,
                    "deps_hash": job.deps_hash,
                    "group": get_group(job.name),
                }
            )
            for parent in job.parents:
                edges.append({"source": str(parent.id), "target": str(job.id)})
        return nodes, edges


def get_stdout_stderr(
    job_name: str, experiment_id: int, db: str = "sqlite:///jobs.db"
) -> dict[str, str]:
    engine = create_engine(db)
    with Session(engine) as session:
        statement = (
            select(Job)
            .where(Job.experiment_id == experiment_id)
            .where(Job.name == job_name)
        )
        results = session.exec(statement)
        job = results.one()
        return {"stdout": job.stdout, "stderr": job.stderr}
