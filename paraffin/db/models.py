import json
import os
import subprocess
import threading
import time
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
    worker: Optional["Worker"] = Relationship(back_populates="jobs")

    def _update_status(self, engine: Engine, new_status: StageStatus) -> None:
        if self.finished_at is not None:
            raise ValueError("Job has already finished.")
        # This always closes the job
        with Session(engine) as session:
            # Get the current job
            job = session.exec(select(Job).where(Job.id == self.id)).one()
            job.finished_at = datetime.now()
            job.status = new_status
            session.add(job)

            # Get the associated stage (if any)
            stage = job.stage
            if stage is None:
                session.commit()
                return
            
            stage.assigned_workers -= 1
            if stage.assigned_workers < 0:
                raise ValueError(
                    f"Assigned workers for stage {stage.id} cannot be negative."
                )

            # If job failed, the whole stage fails
            if new_status == StageStatus.FAILED:
                stage.status = StageStatus.FAILED
                session.add(stage)

            if stage.status != StageStatus.FAILED:
                # Check if all jobs in the stage are finished
                all_jobs_finished = all(j.finished_at is not None for j in stage.jobs)
                if all_jobs_finished:
                    stage.status = new_status
                    session.add(stage)

            session.commit()

    def set_unfinished(self, engine: Engine) -> None:
        self._update_status(engine, StageStatus.UNFINISHED)

    def set_failed(self, engine: Engine) -> None:
        self._update_status(engine, StageStatus.FAILED)

    def set_finished(self, engine: Engine) -> None:
        self._update_status(engine, StageStatus.FINISHED)

    @staticmethod
    def create_for_commit(
        engine: Engine,
        worker: "Worker",
        queues: list | None = None,
        experiment: int | None = None,
        stage_name: str | None = None,
    ) -> tuple["Stage", "Job"] | None:
        """
        Create a job for a stage in the database.
        """
        with Session(bind=engine) as session:
            stage = Stage.claim_finished(session)
            if stage and stage.check_completed_parents():
                job = stage.attach_job(worker)
                session.add(job)
                session.add(stage)
                session.commit()
                session.refresh(stage)
                session.refresh(job)
                return stage, job
            else:
                return None

    def run(
        self,
        shutdown_event: threading.Event,
        worker: Worker,
        engine: Engine,
    ) -> bool:
        with Session(engine) as session:
            stage = session.get(Job, self.id).stage
            session.refresh(stage)
        cmd = json.loads(stage.cmd)
        print(f"({worker.id}) Running command: {cmd}")
        try:
            # subprocess.check_call(cmd, shell=True)
            proc = subprocess.Popen(
                cmd,
                shell=True,
                preexec_fn=os.setsid,
                universal_newlines=True,
                cwd=stage.path,
                env={"PARAFFIN_WORKER_ID": str(worker.id), **os.environ},
            )
            # Wait for the process to finish but also check for shutdown
            while proc.poll() is None and not shutdown_event.is_set():
                time.sleep(0.1)
            # If the shutdown event is set, terminate the process
            if shutdown_event.is_set():
                proc.terminate()
                proc.wait()
                return False
            # Check the return code
            if proc.returncode == 25:
                # The job was interrupted on purpose
                #  and should be marked as unfinished
                print(f"({worker.id}) Job was interrupted: {cmd}")
                self.set_unfinished(
                    engine=engine,
                )
                return False
            if proc.returncode != 0:
                raise subprocess.CalledProcessError(proc.returncode, cmd)
            self.set_finished(
                engine=engine,
            )
        except subprocess.CalledProcessError:
            print(f"({worker.id}) Command failed: {cmd}")
            self.set_failed(
                engine=engine,
            )

        return True


class Stage(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(max_length=100)
    cmd: str = Field(max_length=255)  # Command to execute
    # Can not infer from jobs, because all jobs can finish but the job did not finish yet.
    status: StageStatus = Field(sa_type=String, default=StageStatus.PENDING)
    queue: str = Field(default="default", max_length=100)
    lockfile_content: str = Field(default="")  # JSON string of lockfile
    dependency_hash: str = Field(default="")  # Hash of the dependencies
    experiment_id: int = Field(foreign_key="experiment.id")
    capture_stderr: bool = Field(default=True)
    capture_stdout: bool = Field(default=True)
    cache: bool = Field(default=False)  # Use the paraffin cache for this job
    force: bool = Field(default=False)  # Rerun the job even if cached
    max_workers: int = Field(default=1)  # Maximum number of workers for this job
    assigned_workers: int = Field(default=0)
    # Number of workers assigned to this job
    # we can infer this from the jobs table
    # but we need an atomic operation for assigning workers
    # and thus we need a table for this!
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    path: str = Field(default=".")  # Path to the dvc.yaml file

    # Relationships
    experiment: Optional["Experiment"] = Relationship(back_populates="stages")
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

    @property
    def started_at(self) -> Optional[datetime]:
        starts = [job.started_at for job in self.jobs if job.started_at]
        return min(starts) if starts else None

    @property
    def finished_at(self) -> Optional[datetime]:
        if self.status in {
            StageStatus.FAILED,
            StageStatus.COMPLETED,
            StageStatus.FINISHED,
        }:
            ends = [job.finished_at for job in self.jobs if job.finished_at]
            return max(ends) if ends else None
        return None

    def attach_job(self, worker: "Worker") -> Job:
        self.status = StageStatus.RUNNING
        job = Job(stage_id=self.id, worker_id=worker.id)
        self.jobs.append(job)
        return job

    def update(self, engine: Engine, status: StageStatus, **kwargs: dict) -> None:
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
    def claim_finished(
        session: Session,
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
                    WHERE s.status = '{StageStatus.FINISHED}'
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
    def claim(
        session: Session,
        worker_id: int,
        commit: str = "test",
        origin: str = "test",
        machine: str = "test",
    ) -> Optional["Job"]:
        status = [StageStatus.PENDING, StageStatus.UNKNOWN, StageStatus.UNFINISHED]
        result = session.exec(
            text(f"""
                UPDATE stage
                SET status = '{StageStatus.RUNNING}', assigned_workers = assigned_workers + 1
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
        if row is None:
            # Let's try to claim a stage that can be run in parallel
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
        if row is None:
            return None  # no stage available
        stage: "Stage" = session.exec(select(Stage).where(Stage.id == row[0])).one()
        worker: "Worker" = session.exec(
            select(Worker).where(Worker.id == worker_id)
        ).one()
        job = stage.attach_job(worker)
        session.add(job)
        session.add(stage)
        session.commit()
        session.refresh(stage)
        session.refresh(job)
        return job
