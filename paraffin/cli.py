import datetime
import logging
import os
import socket
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
    name: str = typer.Option("default", "--name", "-n", help="Worker name."),
    job: str | None = typer.Option(None, "--job", "-j", help="Job ID to run."),
    experiment: str | None = typer.Option(
        None, "--experiment", "-e", help="Experiment ID."
    ),
    timeout: int = typer.Option(
        0, "--timeout", "-t", help="Timeout in seconds before exiting."
    ),
    db: str = typer.Option(
        "sqlite:///paraffin.db", help="Database URL.", envvar="PARAFFIN_DB"
    ),
):
    """Start a paraffin worker."""
    queues = queues.split(",")
    # set the log level
    logging.basicConfig(level=logging.INFO)
    worker_id = register_worker(name=name, machine=socket.gethostname(), db_url=db)
    log.info(f"Listening on queues: {queues}")

    last_seen = datetime.datetime.now()
    try:
        while True:
            job_obj = get_job(
                db_url=db,
                queues=queues,
                worker=name,
                machine=socket.gethostname(),
                experiment=experiment,
                job_name=job,
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
            last_seen = datetime.datetime.now()

            update_worker(worker_id, status="running", db_url=db)

            # This will search the DB and not rely on DVC run cache to determine if
            #  the job is cached so this can easily work across directories
            cached_job = False
            if job_obj["cache"] and detect_zntrack(job_obj):
                stage_lock, deps_hash = get_lock(job_obj["name"])
                cached_job = find_cached_job(deps_cache=deps_hash, db_url=db)
            if cached_job:
                log.info(
                    f"Job '{job_obj['name']}' is cached and dvc.lock is available."
                )
                returncode, stdout, stderr = checkout(
                    stage_lock, cached_job["lock"], job_obj["name"]
                )
                if returncode == 404:
                    # TODO: we need to ensure that all deps nodes are checked out!
                    #  this will be important when clone / push.
                    # TODO: this can be the cause for a lock issue!
                    log.warning(
                        "Unable to checkout GIT tracked files"
                        f" for job '{job_obj['name']}'"
                    )
                    log.info(f"Running job '{job_obj['name']}'")
                    returncode, stdout, stderr = repro(job_obj["name"])
            else:
                log.info(f"Running job '{job_obj['name']}'")
                # TODO: we need to ensure that all deps nodes are checked out!
                #  this will be important when clone / push.
                # TODO: this can be the cause for a lock issue!
                returncode, stdout, stderr = repro(job_obj["name"])
            if returncode != 0:
                complete_job(
                    job_obj["id"],
                    status="failed",
                    lock={},
                    stdout=stdout,
                    stderr=stderr,
                    db_url=db,
                )
            else:
                stage_lock, _ = get_lock(job_obj["name"])
                complete_job(
                    job_obj["id"],
                    status="completed",
                    lock=stage_lock,
                    stdout=stdout,
                    stderr=stderr,
                    db_url=db,
                )
            job_obj = None
            update_worker(worker_id, status="idle", db_url=db)
    finally:
        if job_obj is not None:
            complete_job(
                job_obj["id"],
                status="failed",
                lock={},
                stdout="",
                stderr="Worker exited.",
                db_url=db,
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
):
    """Run DVC stages in parallel."""
    if verbose:
        logging.basicConfig(level=logging.DEBUG)

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
    graph = get_stage_graph(names=names)

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
