import datetime
import logging
import os
import socket
import threading
import time
import typing as t
import webbrowser

import git
import typer
import uvicorn

from paraffin.db import (
    close_worker,
    complete_job,
    find_cached_job,
    get_job,
    register_worker,
    save_graph_to_db,
    update_worker,
)
from paraffin.stage import checkout, get_lock, repro
from paraffin.ui.app import app as webapp
from paraffin.utils import (
    detect_zntrack,
    get_custom_queue,
    get_stage_graph,
    update_gitignore,
)

log = logging.getLogger(__name__)

app = typer.Typer()


def spawn_worker(
    name: str,
    queues,
    experiment: str,
    stage_name: str,
    timeout: float,
    db: str,
    workers: dict,
):
    worker_id = register_worker(
        name=name,
        machine=socket.gethostname(),
        db_url=db,
        cwd=os.getcwd(),
        pid=os.getpid(),
    )
    workers[worker_id] = None
    log.info(f"Listening on queues: {queues}")

    last_seen = datetime.datetime.now()
    try:
        while True:
            job_obj = get_job(
                db_url=db,
                queues=queues,
                worker_id=worker_id,
                experiment=experiment,
                stage_name=stage_name,
            )

            if job_obj is None:
                remaining_seconds = (
                    timeout - (datetime.datetime.now() - last_seen).seconds
                )
                if remaining_seconds <= 0:
                    log.info("Timeout reached - exiting.")
                    break
                time.sleep(1)
                log.info(
                    "No more job found"
                    f" - sleeping until closing in {remaining_seconds} seconds"
                )
                continue

            stage, job = job_obj
            last_seen = datetime.datetime.now()

            update_worker(worker_id, status="running", db_url=db)
            workers[worker_id] = stage.id

            # This will search the DB and not rely on DVC run cache to determine if
            #  the job is cached so this can easily work across directories
            cached_job = None
            if stage.cache and detect_zntrack({"cmd": stage.cmd}) and not stage.force:
                stage_lock, dependency_hash = get_lock(stage.name)
                cached_job = find_cached_job(deps_cache=dependency_hash, db_url=db)
            if cached_job is not None:
                log.info(f"Job '{stage.name}' is cached and dvc.lock is available.")
                returncode, stdout, stderr = checkout(
                    stage_lock, cached_job.lockfile_content, stage.name
                )
                if returncode == 404:
                    # TODO: we need to ensure that all deps nodes are checked out!
                    #  this will be important when clone / push.
                    # TODO: this can be the cause for a lock issue!
                    log.warning(
                        f"Unable to checkout GIT tracked files for job '{stage.name}'"
                    )
                    log.info(f"Running job '{stage.name}'")
                    returncode, stdout, stderr = repro(
                        stage.name, force=stage.force
                    )  # TODO: this is not tested in CI,
                    #  because it did not raise an error
            else:
                log.info(f"Running job '{stage.name}'")
                # TODO: we need to ensure that all deps nodes are checked out!
                #  this will be important when clone / push.
                # TODO: this can be the cause for a lock issue!
                returncode, stdout, stderr = repro(stage.name, force=stage.force)
            if returncode != 0:
                complete_job(
                    stage_id=stage.id,  # TODO: should later be job.id
                    status="failed",
                    lock={},
                    stdout=stdout,
                    stderr=stderr,
                    db_url=db,
                    worker_id=worker_id,
                )
            else:
                stage_lock, _ = get_lock(stage.name)
                complete_job(
                    stage_id=stage.id,  # TODO: should later be job.id
                    status="completed",
                    lock=stage_lock,
                    stdout=stdout,
                    stderr=stderr,
                    db_url=db,
                    worker_id=worker_id,
                )
            job_obj = None
            update_worker(worker_id, status="idle", db_url=db)

    finally:
        if job_obj is not None:
            stage, job = job_obj
            complete_job(
                stage_id=stage.id,  # TODO: should later be job.id
                status="failed",
                lock={},
                stdout="",
                stderr="Worker exited.",
                db_url=db,
                worker_id=worker_id,
            )
        close_worker(id=worker_id, db_url=db)
        workers.pop(worker_id)


@app.command()
def ui(
    port: int = 8000,
    db: str = typer.Option(
        "sqlite:///paraffin.db", help="Database URL.", envvar="PARAFFIN_DB"
    ),
    all: bool = typer.Option(
        False, help="Show all experiments and not just from the current commit."
    ),
):
    """Start the Paraffin web UI."""
    if not all:
        try:
            repo = git.Repo(search_parent_directories=True)
            commit = repo.head.commit
            os.environ["PARAFFIN_COMMIT"] = commit.hexsha
        except git.InvalidGitRepositoryError:
            log.warning(
                "Unable to determine the current commit. Showing all experiments."
            )

    webbrowser.open(f"http://localhost:{port}")
    os.environ["PARAFFIN_DB"] = db
    uvicorn.run(webapp, host="0.0.0.0", port=port)


@app.command()
def worker(
    queues: str = typer.Option(
        "default",
        "--queues",
        "-q",
        envvar="PARAFFIN_QUEUES",
        help="Comma separated list of queues to listen on.",
    ),
    name: str = typer.Option(
        "default", "--name", "-n", help="Specify a custom name for this worker."
    ),
    stage: str | None = typer.Option(None, help="Job ID to run."),
    experiment: str | None = typer.Option(
        None, "--experiment", "-e", help="Experiment ID to run."
    ),
    timeout: int = typer.Option(
        0,
        "--timeout",
        "-t",
        help="Timeout in seconds before exiting"
        " the worker if no more jobs are in the queue.",
    ),
    db: str = typer.Option(
        "sqlite:///paraffin.db", help="Database URL.", envvar="PARAFFIN_DB"
    ),
    jobs: int = typer.Option(1, "--jobs", "-j", help="Number of jobs to run."),
    delay_between_workers: float = typer.Option(
        0.1, help="Delay between starting workers.", hidden=True
    ),
):
    """Start a paraffin worker to process the queued DVC stages."""
    queues = queues.split(",")
    logging.basicConfig(level=logging.INFO)
    threads = []

    workers = {}
    try:
        for i in range(jobs):
            t = threading.Thread(
                target=spawn_worker,
                args=(name, queues, experiment, stage, timeout, db, workers),
                daemon=True,
            )
            threads.append(t)
            t.start()
            time.sleep(delay_between_workers)

        for t in threads:
            t.join()
    finally:
        for worker_id, job_id in workers.items():
            if job_id is not None:
                complete_job(
                    stage_id=job_id,
                    status="failed",
                    lock={},
                    stdout="",
                    stderr="Worker exited.",
                    db_url=db,
                    worker_id=worker_id,
                )
            close_worker(id=worker_id, db_url=db)


@app.command()
def submit(
    names: t.Optional[list[str]] = typer.Argument(
        None, help="Stage names to run. If not specified, run all stages."
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output."),
    cache: bool = typer.Option(
        False,
        help="Use the paraffin cache in addition to the DVC cache"
        " to checkout cached jobs.",
    ),
    db: str = typer.Option(
        "sqlite:///paraffin.db", help="Database URL.", envvar="PARAFFIN_DB"
    ),
    force: bool = typer.Option(
        False,
        "--force",
        "-f",
        help="reproduce pipelines, regenerating its results, even if no changes"
        " were found. See https://dvc.org/doc/command-reference/repro#-f"
        " for more information.",
    ),
    single_item: bool = typer.Option(
        False,
        "--single-item",
        "-s",
        help="reproduce only a single stage by turning off the recursive search for"
        " changed dependencies. See https://dvc.org/doc/command-reference/repro#-s"
        " for more information.",
    ),
):
    """Run DVC stages in parallel."""
    if verbose:
        logging.basicConfig(level=logging.DEBUG)

    if single_item and names is None:
        typer.echo("Cannot use single item without specifying names")
        raise typer.Exit(1)

    # check if the repo has a commit
    repo = git.Repo(search_parent_directories=True)
    if not repo.head.is_valid():
        log.error(
            "Unable to create experiment inside a GIT repository without commits."
        )
        return
    else:
        commit = repo.head.commit
        try:
            origin = repo.remotes.origin.url
        except AttributeError:
            origin = "local"
            log.debug(f"Creating new experiment based on commit '{commit}'")

    log.debug("Getting stage graph")
    graph = get_stage_graph(names=names, force=force, single_item=single_item)

    custom_queues = get_custom_queue()
    update_gitignore(line="paraffin.db")
    save_graph_to_db(
        graph,
        queues=custom_queues,
        commit=commit.hexsha,
        origin=origin,
        machine=socket.gethostname(),
        cache=cache,
        db_url=db,
    )
