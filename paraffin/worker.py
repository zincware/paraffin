import json
import os
import socket
import subprocess
import threading
import time
from datetime import datetime
from typing import Optional

from sqlalchemy import Engine
from sqlmodel import Session

from paraffin.db.app import (
    Job,
    StageStatus,
    Worker,
)
from paraffin.db.models import Worker, Job, Stage


# TODO: make Job.run() a function
def run_job(
    job: Job,
    shutdown_event: threading.Event,
    worker: Worker,
    engine: Engine,
) -> bool:
    with Session(engine) as session:
        stage = session.get(Job, job.id).stage
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
            job.set_unfinished(
                engine=engine,
            )
            return False
        if proc.returncode != 0:
            raise subprocess.CalledProcessError(proc.returncode, cmd)
        job.set_finished(
            engine=engine,
        )
    except subprocess.CalledProcessError:
        print(f"({worker.id}) Command failed: {cmd}")
        job.set_failed(
            engine=engine,
        )

    return True


def run_worker(
    name: str, engine: Engine, shutdown_event: threading.Event, timeout: int
):
    active_job: Optional[Job] = None

    worker = Worker.register(
        name=name,
        machine=socket.gethostname(),
        engine=engine,
        cwd=os.getcwd(),
        pid=os.getpid(),
    )

    timer = None

    try:
        while not shutdown_event.is_set():
            with Session(engine) as session:
                job = Stage.claim(
                    session=session,
                    worker_id=worker.id,
                )
            if job is None and timer is None:
                timer = datetime.now()
            elif job is None and timer is not None:
                if (datetime.now() - timer).total_seconds() > timeout:
                    print(f"({worker.id}) No job found, shutting down.")
                    break
                print(f"({worker.id}) No job found, waiting for {timeout} seconds.")
                time.sleep(max([timeout / 5, 1]))
            elif job is not None:
                timer = None

                active_job = job

                result = run_job(
                    job=job,
                    shutdown_event=shutdown_event,
                    worker=worker,
                    engine=engine,
                )
                active_job = None
                if not result:
                    break
    finally:
        if active_job is not None:
            print(f"({worker.id}) Job {active_job.id} was interrupted.")
            active_job.set_unfinished(engine=engine)
        worker.close(engine=engine)
        print(f"({worker.id}) Worker closed.")
