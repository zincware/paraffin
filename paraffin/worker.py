import os
import socket
import threading
import time
from datetime import datetime
from typing import Optional

from sqlalchemy import Engine
from sqlmodel import Session

from paraffin.db.app import (
    Job,
    Worker,
)
from paraffin.db.models import Job, Stage, Worker


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

                result = job.run(
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
