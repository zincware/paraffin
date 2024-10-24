import pathlib
from celery import Celery
import subprocess
import logging

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
        broker_url='filesystem://',
        result_backend= f"db+sqlite:///{results_db.as_posix()}",
        broker_transport_options={
            "data_folder_in": data_folder.as_posix(),
            "data_folder_out": data_folder.as_posix(),
            "data_folder_processed": data_folder.as_posix(),
            "control_folder": control_folder.as_posix(),
        }
    )

    return app


app = make_celery()

@app.task(bind=True, default_retry_delay=5) # retry in 5 seconds
def repro(self, *args, name: str):
    try:
        lock_error = False

        popen = subprocess.Popen(
            ["dvc", "repro", "--single-item", name], stdout=subprocess.PIPE, universal_newlines=True, stderr=subprocess.PIPE
        )
        for stdout_line in iter(popen.stdout.readline, ""):
            # logging.info(stdout_line)
            print(stdout_line, end="")
        popen.stdout.close()

        for stderr_line in iter(popen.stderr.readline, ""):
            # logging.error(stderr_line)
            print(stderr_line, end="")
            if "ERROR: Unable to acquire lock" in stderr_line:
                lock_error = True
                
        popen.stderr.close()

    except subprocess.CalledProcessError as exc:
        if lock_error:
            raise self.retry(exc=exc, max_retries=5)
        else :
            # something else went wrong and we fail!
            raise exc
    return True
