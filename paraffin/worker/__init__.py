import logging
import pathlib
import random
import subprocess
import time

from celery import Celery

log = logging.getLogger(__name__)


def make_celery() -> Celery:
    paraffin_folder = pathlib.Path(".paraffin")
    data_folder = paraffin_folder / "data"
    control_folder = paraffin_folder / "data"
    results_db = paraffin_folder / "results.db"
    gitignore = paraffin_folder / ".gitignore"

    data_folder.mkdir(parents=True, exist_ok=True)
    control_folder.mkdir(parents=True, exist_ok=True)
    results_db.parent.mkdir(parents=True, exist_ok=True)

    if not gitignore.exists():
        gitignore.write_text("data\nresults.db\n")

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

    # app = Celery(
    #     __name__,
    #     broker="redis://localhost:6379/0",
    #     backend="redis://localhost:6379/0",
    # )

    return app


app = make_celery()


@app.task(bind=True, default_retry_delay=5)  # retry in 5 seconds
def repro(self, *args, name: str):
    """Celery task to reproduce a DVC pipeline stage.

    This task attempts to reproduce a specified DVC pipeline stage
    using the `dvc repro` command.
    If the command fails due to an "Unable to acquire lock" error,
    it retries the operation up to 5 times.
    If the error occurs after the stage has been executed, it attempts to
    commit the lock using the `dvc commit` command with a
    forced option to avoid loss of computational resources.

    Args:
        self (Task): The bound Celery task instance.
        *args: Additional arguments.
        name (str): The name of the DVC pipeline stage to reproduce.

    Raises:
        self.retry: If the "Unable to acquire lock" error occurs,
        the task is retried up to 5 times.
        RuntimeError: If unable to commit the lock after multiple attempts.

    Returns:
        bool: True if the operation is successful.
    """
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
                    log.error(f"Committing {name} again due to lock error")
                    subprocess.check_call(
                        ["dvc", "commit", name, "--force"],
                        stderr=subprocess.PIPE,
                        stdout=subprocess.PIPE,
                    )
                    break
                except subprocess.CalledProcessError:
                    time.sleep(1 + random.random())
            else:
                raise RuntimeError(f"Unable to commit lock for {name}")
    popen.stderr.close()
    return True
