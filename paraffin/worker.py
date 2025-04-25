import json
import os
import socket
import subprocess
import threading
import time
from datetime import datetime
from typing import Optional

from paraffin.db.app import (
    Job,
    StageStatus,
    close_worker,
    get_job,
    register_worker,
    update_job,
)


def run_worker(name: str, db: str, shutdown_event: threading.Event, timeout: int):
    active_job: Optional[Job] = None

    worker_id = register_worker(
        name=name,
        machine=socket.gethostname(),
        db_url=db,
        cwd=os.getcwd(),
        pid=os.getpid(),
    )

    timer = None

    try:
        while not shutdown_event.is_set():
            res = get_job(
                db_url=db,
                queues=None,
                worker_id=worker_id,
                experiment=None,
                stage_name=None,
                status=[StageStatus.PENDING, StageStatus.UNKNOWN],
            )
            if res is None and timer is None:
                timer = datetime.now()
            elif res is None and timer is not None:
                if (datetime.now() - timer).total_seconds() > timeout:
                    print(f"({worker_id}) No job found, shutting down.")
                    break
                print(f"({worker_id}) No job found, waiting for {timeout} seconds.")
                time.sleep(max([timeout / 5, 1]))
            elif res is not None:
                timer = None

                stage, job = res
                active_job = job

                cmd = json.loads(stage.cmd)
                print(f"({worker_id}) Running command: {cmd}")
                try:
                    # subprocess.check_call(cmd, shell=True)
                    proc = subprocess.Popen(
                        cmd,
                        shell=True,
                        preexec_fn=os.setsid,
                        universal_newlines=True,
                        cwd=stage.path,
                        env={"PARAFFIN_WORKER_ID": str(worker_id), **os.environ},
                    )
                    # Wait for the process to finish but also check for shutdown
                    while proc.poll() is None and not shutdown_event.is_set():
                        time.sleep(0.1)
                    # If the shutdown event is set, terminate the process
                    if shutdown_event.is_set():
                        proc.terminate()
                        proc.wait()
                        break
                    # Check the return code
                    if proc.returncode != 0:
                        raise subprocess.CalledProcessError(proc.returncode, cmd)
                    # TODO: only set to finished if the all jobs are finished
                    # TODO: set the job to finished
                    update_job(
                        db_url=db,
                        stage_id=job.stage_id,
                        status=StageStatus.FINISHED,
                    )
                except subprocess.CalledProcessError:
                    print(f"({worker_id}) Command failed: {cmd}")
                    update_job(
                        db_url=db,
                        stage_id=job.stage_id,
                        status=StageStatus.FAILED,
                    )
                active_job = None
    finally:
        if active_job is not None:
            print(f"({worker_id}) Job {active_job.id} was interrupted.")
            update_job(
                db_url=db,
                stage_id=active_job.stage_id,
                status=StageStatus.UNFINISHED,
            )
        close_worker(id=worker_id, db_url=db)
        print(f"({worker_id}) Worker closed.")
