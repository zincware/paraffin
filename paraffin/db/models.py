from datetime import datetime
from typing import List, Literal, Optional

from sqlmodel import Field, Relationship, SQLModel, String, UniqueConstraint


class Worker(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(max_length=100, index=True)
    machine: str = Field(max_length=100)
    status: Literal["running", "idle", "offline"] = Field(
        sa_type=String, default="idle", index=True
    )
    last_seen: datetime = Field(default_factory=datetime.now)
    cwd: str = Field(default="", max_length=255)  # Current working directory
    pid: int = Field(default=0)  # Process ID
    started_at: datetime = Field(default_factory=datetime.now)
    finished_at: Optional[datetime] = None

    # Relationships
    jobs: List["Job"] = Relationship(back_populates="worker")


class StageDependency(SQLModel, table=True):
    parent_id: int = Field(foreign_key="stage.id", primary_key=True)
    child_id: int = Field(foreign_key="stage.id", primary_key=True)

    # Unique constraint to prevent duplicate dependencies
    __table_args__ = (
        UniqueConstraint("parent_id", "child_id", name="unique_dependency"),
    )


class Experiment(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    base: str = Field()
    origin: str = Field(default="local")
    machine: str = Field(default="local")
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

    # Relationships
    stages: List["Stage"] = Relationship(back_populates="experiment")


class Job(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    stage_id: int = Field(foreign_key="stage.id")
    worker_id: int = Field(foreign_key="worker.id")
    stderr: str = Field(default="")
    stdout: str = Field(default="")
    started_at: datetime = Field(default_factory=datetime.now)
    finished_at: Optional[datetime] = None

    # Relationships
    stage: Optional["Stage"] = Relationship(back_populates="jobs")
    worker: Optional[Worker] = Relationship(back_populates="jobs")


class Stage(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(max_length=100)
    cmd: str = Field(max_length=255)  # Command to execute
    status: Literal["pending", "running", "completed", "cached", "failed"] = Field(
        sa_type=String, default="pending"
    )
    queue: str = Field(default="default", max_length=100)
    lockfile_content: str = Field(default="")  # JSON string of lockfile
    dependency_hash: str = Field(default="")  # Hash of the dependencies
    experiment_id: int = Field(foreign_key="experiment.id")
    capture_stderr: bool = Field(default=True)
    capture_stdout: bool = Field(default=True)
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    cache: bool = Field(default=False)  # Use the paraffin cache for this job
    force: bool = Field(default=False)  # Rerun the job even if cached
    max_workers: int = Field(default=1)  # Maximum number of workers for this job
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

    # Relationships
    experiment: Optional[Experiment] = Relationship(back_populates="stages")
    jobs: List[Job] = Relationship(back_populates="stage")
    parents: List["Stage"] = Relationship(
        link_model=StageDependency,
        back_populates="children",
        sa_relationship_kwargs={
            "primaryjoin": "Stage.id==StageDependency.child_id",
            "secondaryjoin": "Stage.id==StageDependency.parent_id",
        },
    )
    children: List["Stage"] = Relationship(
        link_model=StageDependency,
        back_populates="parents",
        sa_relationship_kwargs={
            "primaryjoin": "Stage.id==StageDependency.parent_id",
            "secondaryjoin": "Stage.id==StageDependency.child_id",
        },
    )

    def attach_job(self, worker: Worker) -> Job:
        self.status = "running"
        job = Job(stage_id=self.id, worker_id=worker.id)
        self.jobs.append(job)
        return job
