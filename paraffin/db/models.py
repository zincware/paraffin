from datetime import datetime
from typing import List, Optional

from sqlalchemy import Engine
from sqlmodel import (
    Field,
    Relationship,
    Session,
    SQLModel,
    String,
    UniqueConstraint,
    select,
    text,
)

from paraffin.backports import StrEnum
from paraffin.dvc import StageStatus


class ExperimentStatus(StrEnum):  # TODO: could be a bool
    ACTIVE = "active"
    INACTIVE = "inactive"


class WorkerStatus(StrEnum):
    """Worker status enum.

    Attributes
    ----------
    RUNNING : str
        The worker is currently running a job.
    IDLE : str
        The worker is idle and waiting for a job.
    OFFLINE : str
        The worker is offline and not available for jobs.
    """

    RUNNING = "running"
    IDLE = "idle"
    OFFLINE = "offline"


class Worker(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(max_length=100, index=True)
    machine: str = Field(max_length=100)
    status: WorkerStatus = Field(sa_type=String, default=WorkerStatus.IDLE, index=True)
    last_seen: datetime = Field(default_factory=datetime.now)
    cwd: str = Field(default="", max_length=255)
    pid: int = Field(default=0)
    started_at: datetime = Field(default_factory=datetime.now)
    finished_at: Optional[datetime] = None
    requires_dvc_lock: bool = Field(default=False)
    jobs: List["Job"] = Relationship(back_populates="worker")

    @classmethod
    def register(
        cls,
        name: str,
        machine: str,
        engine: Engine,
        cwd: str,
        pid: int,
        requires_dvc_lock: bool = False,
    ) -> "Worker":
        with Session(engine) as session:
            worker = cls(
                name=name,
                machine=machine,
                cwd=cwd,
                pid=pid,
                requires_dvc_lock=requires_dvc_lock,
            )
            session.add(worker)
            session.commit()
            session.refresh(worker)
            return worker

    def close(self, engine: Engine) -> None:
        with Session(engine) as session:
            worker = session.exec(select(Worker).where(Worker.id == self.id)).one()
            worker.status = WorkerStatus.OFFLINE
            worker.last_seen = datetime.now()
            worker.finished_at = datetime.now()
            session.add(worker)
            session.commit()


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
    status: ExperimentStatus = Field(
        sa_type=String, default=ExperimentStatus.ACTIVE
    )  # Status of the experiment
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
    status: StageStatus = Field(sa_type=String, default=StageStatus.RUNNING)

    # Relationships
    stage: Optional["Stage"] = Relationship(back_populates="jobs")
    worker: Optional[Worker] = Relationship(back_populates="jobs")

    def set_unfinished(self, engine: Engine) -> None:
        with Session(engine) as session:
            job = session.exec(select(Job).where(Job.id == self.id)).one()
            job.finished_at = datetime.now()
            job.status = StageStatus.UNFINISHED
            session.add(job)
            stage = job.stage
            if stage is None:
                return
            if stage.status != StageStatus.RUNNING:
                raise ValueError("stage is not running")
            for job in stage.jobs: # we only change the state if this is the last job
                if job.finished_at is None:
                    session.commit()
                    return
            stage.status = StageStatus.UNFINISHED
            session.add(stage)
            session.commit()
            session.refresh(stage)
            session.refresh(job)

    def set_failed(self, engine: Engine) -> None:
        with Session(engine) as session:
            job = session.exec(select(Job).where(Job.id == self.id)).one()
            job.finished_at = datetime.now()
            job.status = StageStatus.FAILED
            session.add(job)
            stage = job.stage
            if stage is None:
                return
            if stage.status != StageStatus.RUNNING:
                raise ValueError("stage is not running")
            for job in stage.jobs:
                # if a job fails, should we set the stage to failed for all?
                if job.finished_at is None:
                    session.commit()
                    return
            stage.status = StageStatus.FAILED
            session.add(stage)
            session.commit()
            session.refresh(stage)
            session.refresh(job)

    def set_finished(self, engine: Engine) -> None:
        with Session(engine) as session:
            job = session.exec(select(Job).where(Job.id == self.id)).one()
            job.finished_at = datetime.now()
            job.status = StageStatus.FINISHED
            session.add(job)
            stage = job.stage
            if stage is None:
                return
            # check if all jobs of the stage are finished, then set the stage to finished
            # TODO: do we want to look for worker heartbeats here?
            # TODO: should we check the status of the stage is running?
            if stage.status != StageStatus.RUNNING:
                raise ValueError("stage is not running")
            for job in stage.jobs:
                if job.finished_at is None:
                    session.commit()
                    return
            stage.status = StageStatus.FINISHED
            session.add(stage)
            session.commit()
            session.refresh(stage)
            session.refresh(job)


    @staticmethod
    def create(
        engine: Engine,
        worker: Worker,
        status: list[StageStatus],
        queues: list | None = None,
        experiment: int | None = None,
        stage_name: str | None = None,
    ) -> tuple["Stage", "Job"] | None:
        with Session(bind=engine) as session:
            worker = session.exec(select(Worker).where(Worker.id == worker.id)).one()
            stage = Stage.claim(session, status=status)
            # TODO: don't we need to rollback the SET status = '{StageStatus.RUNNING}' if _all_parents_completed is false?
            if stage and stage.check_completed_parents():
                job = stage.attach_job(worker)
                session.add(job)
                session.add(stage)
                session.commit()
                session.refresh(stage)
                session.refresh(job)
                return stage, job
            else:
                parallel_stage = Stage.claim_parallel(session)
                if parallel_stage and parallel_stage.check_completed_parents():
                    print(f"Claimed stage {parallel_stage.name} for parallel execution")
                    job = parallel_stage.attach_job(worker)
                    session.add(job)
                    session.add(parallel_stage)
                    session.commit()
                    session.refresh(parallel_stage)
                    session.refresh(job)
                    return parallel_stage, job

        return None

class Stage(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(max_length=100)
    cmd: str = Field(max_length=255)  # Command to execute
    status: StageStatus = Field(sa_type=String, default=StageStatus.PENDING)  # TODO: infer from the jobs? 
    queue: str = Field(default="default", max_length=100)
    lockfile_content: str = Field(default="")  # JSON string of lockfile
    dependency_hash: str = Field(default="")  # Hash of the dependencies
    experiment_id: int = Field(foreign_key="experiment.id")
    capture_stderr: bool = Field(default=True)
    capture_stdout: bool = Field(default=True)
    # started_at: Optional[datetime] = None # infer from the jobs
    # finished_at: Optional[datetime] = None  # infer from the jobs
    cache: bool = Field(default=False)  # Use the paraffin cache for this job
    force: bool = Field(default=False)  # Rerun the job even if cached
    max_workers: int = Field(default=1)  # Maximum number of workers for this job
    assigned_workers: int = Field(default=1)
    # Number of workers assigned to this job
    # we can infer this from the jobs table
    # but we need an atomic operation for assigning workers
    # and thus we need a table for this!
    # the default value is 1, because the first worker will be assigned
    # seperately and this field is not requried for that!
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    path: str = Field(default=".")  # Path to the dvc.yaml file

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
        self.status = StageStatus.RUNNING
        job = Job(stage_id=self.id, worker_id=worker.id)
        self.jobs.append(job)
        return job
    
    def update(
        self,
        engine: Engine,
        status: StageStatus,
        **kwargs: dict
    ) -> None:
        """
        Update the status of a job in the database.
        """
        with Session(engine) as session:
            self.status = status
            for key, value in kwargs.items():
                if hasattr(self, key):
                    setattr(self, key, value)
            session.add(self)
            session.commit()
            session.refresh(self)

    def check_completed_parents(self) -> bool:
        """
        Check if all parents of a job are completed.
        """
        return all(
            parent.status in [StageStatus.COMPLETED, StageStatus.FINISHED]
            for parent in self.parents
        )

    @staticmethod
    def claim(
        session: Session,
        status: list[StageStatus],
        commit: str = "test",
        origin: str = "test",
        machine: str = "test",
    ) -> Optional["Stage"]:
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

    @staticmethod
    def claim_parallel(
        session: Session,
    ) -> Optional["Stage"]:
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
