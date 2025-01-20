from typing import Optional, List
from sqlmodel import Field, Relationship, SQLModel, create_engine, Session, select
import networkx as nx
from sqlalchemy.orm import joinedload
import fnmatch

class JobDependency(SQLModel, table=True):
    parent_id: Optional[int] = Field(foreign_key="job.id", primary_key=True)
    child_id: Optional[int] = Field(foreign_key="job.id", primary_key=True)

class Job(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    cmd: str  # Command to execute
    status: str = "pending"  # pending, running, completed, failed
    queue: str = "default"  # Queue name(s)

    # Relationships
    parents: List["Job"] = Relationship(
        link_model=JobDependency,
        back_populates="children",
        sa_relationship_kwargs={"primaryjoin": "Job.id==JobDependency.child_id", "secondaryjoin": "Job.id==JobDependency.parent_id"}
    )
    children: List["Job"] = Relationship(
        link_model=JobDependency,
        back_populates="parents",
        sa_relationship_kwargs={"primaryjoin": "Job.id==JobDependency.parent_id", "secondaryjoin": "Job.id==JobDependency.child_id"}
    )

def save_graph_to_db(graph: nx.DiGraph, queues: dict[str, str]):
    engine = create_engine("sqlite:///jobs.db")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        for node in nx.topological_sort(graph):
            queue = "default"
            # use fnmatch to match the node name with the custom queues
            for pattern, q in queues.items():
                if fnmatch.fnmatch(node.name, pattern):
                    queue = q
                    break
            job = Job(cmd=node.cmd, name=node.name, queue=queue)
            session.add(job)
            # add dependencies
            for parent in graph.predecessors(node):
                parent_job = session.exec(select(Job).where(Job.cmd == parent.cmd)).one()
                session.add(JobDependency(parent_id=parent_job.id, child_id=job.id))
        
        session.commit()

def db_to_graph(db: str = "sqlite:///jobs.db") -> nx.DiGraph:
    engine = create_engine(db)
    with Session(engine) as session:
        jobs = session.exec(select(Job)).all()
        graph = nx.DiGraph()
        for job in jobs:
            graph.add_node(job.id, queue=job.queue, cmd=job.cmd, status=job.status)
            for parent in job.parents:
                graph.add_edge(parent.id, job.id)

    return graph


def get_job(db: str = "sqlite:///jobs.db", queues: list|None = None) -> dict | None:
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
            print(len(job.parents))
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

def complete_job(job_id: int, db: str = "sqlite:///jobs.db", status: str = "completed"):
    engine = create_engine(db)
    with Session(engine) as session:
        statement = select(Job).where(Job.id == job_id)
        results = session.exec(statement)
        job = results.one()
        job.status = status
        session.add(job)
        session.commit()
