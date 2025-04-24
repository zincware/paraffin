import json
import os
import socket
import subprocess
import threading
import time
from typing import Optional

from paraffin.db.app import (
    Job,
    StageStatus,
    close_worker,
    get_job,
    register_worker,
    update_job,
)


def run_worker(name: str, db: str, shutdown_event: threading.Event):
    active_job: Optional[Job] = None

    worker_id = register_worker(
        name=name,
        machine=socket.gethostname(),
        db_url=db,
        cwd=os.getcwd(),
        pid=os.getpid(),
    )

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
            if res is None:
                break

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
