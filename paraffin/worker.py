import json
import os
import socket
import subprocess

from paraffin.db.app import (
    StageStatus,
    close_worker,
    get_job,
    register_worker,
    update_job,
)


def run_worker(name: str, db: str):
    worker_id = register_worker(
        name=name,
        machine=socket.gethostname(),
        db_url=db,
        cwd=os.getcwd(),
        pid=os.getpid(),
    )

    try:
        while True:
            res = get_job(
                db_url=db,
                queues=None,
                worker_id=worker_id,
                experiment=None,
                stage_name=None,
                status=[StageStatus.QUEUED],
            )
            if res is None:
                break

            stage, job = res
            cmd = json.loads(stage.cmd)
            print(f"({worker_id}) Running command: {cmd}")
            try:
                subprocess.check_call(cmd, shell=True)
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
    finally:
        close_worker(id=worker_id, db_url=db)
        print(f"({worker_id}) Worker closed.")
