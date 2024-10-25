import logging
import pathlib
import subprocess
import time

from celery import Celery

log = logging.getLogger(__name__)
# set the andler.terminator = ""


def make_celery() -> Celery:
    data_folder = pathlib.Path(".paraffin", "data")
    control_folder = pathlib.Path(".paraffin", "control")
    results_db = pathlib.Path(".paraffin", "results.db")

    data_folder.mkdir(parents=True, exist_ok=True)
    control_folder.mkdir(parents=True, exist_ok=True)
    results_db.parent.mkdir(parents=True, exist_ok=True)

    app = Celery(
        __name__,
        broker_url="filesystem://",
        result_backend=f"db+sqlite:///{results_db.as_posix()}",
        broker_transport_options={
            "data_folder_in": data_folder.as_posix(),
            "data_folder_out": data_folder.as_posix(),
            "data_folder_processed": data_folder.as_posix(),
            "control_folder": control_folder.as_posix(),
        },
    )

    return app


app = make_celery()


@app.task(bind=True, default_retry_delay=5)  # retry in 5 seconds
def repro(self, *args, name: str):
    popen = subprocess.Popen(
        ["dvc", "repro", "--single-item", name],
        stdout=subprocess.PIPE,
        universal_newlines=True,
        stderr=subprocess.PIPE,
    )
    for stdout_line in iter(popen.stdout.readline, ""):
        # logging.info(stdout_line)
        print(stdout_line, end="")
    popen.stdout.close()

    for stderr_line in iter(popen.stderr.readline, ""):
        # logging.error(stderr_line)
        print(stderr_line, end="")
        if "ERROR: Unable to acquire lock" in stderr_line:
            log.error(f"Retrying {name} due to lock error")
            raise self.retry(max_retries=5)
        if (
            f"ERROR: failed to reproduce '{name}': Unable to acquire lock"
            in stderr_line
        ):
            # unable to commit lock, keep retrying
            for _ in range(5):
                try:
                    log.error(f"Commiting {name} again due to lock error")
                    subprocess.check_call(
                        ["dvc", "commit", name, "--force"],
                        stderr=subprocess.PIPE,
                        stdout=subprocess.PIPE,
                    )
                    break
                except subprocess.CalledProcessError:
                    time.sleep(1)
            else:
                raise RuntimeError(f"Unable to commit lock for {name}")
    popen.stderr.close()
    return True


# Shutdown task
@app.task(bind=True)
def shutdown_worker(self, *args, **kwargs):
    app.control.revoke(self.request.id)  # prevent this task from being executed again
    app.control.shutdown()
