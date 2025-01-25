import logging
import socket
import subprocess
import typing as t
import webbrowser

import dvc.api
import git
import typer
import uvicorn
from dvc.stage.cache import _get_cache_hash
from dvc.stage.serialize import to_single_stage_lockfile

from paraffin.db import complete_job, find_cached_job, get_job, save_graph_to_db
from paraffin.ui.app import app as webapp
from paraffin.utils import get_custom_queue, get_stage_graph

log = logging.getLogger(__name__)

app = typer.Typer()


@app.command()
def ui(port: int = 8000):
    """Start the Paraffin web UI."""
    webbrowser.open(f"http://localhost:{port}")
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
):
    """Start a Celery worker."""
    queues = queues.split(",")
    # set the log level
    logging.basicConfig(level=logging.INFO)
    log.info(f"Listening on queues: {queues}")
    while True:
        job_obj = get_job(
            queues=queues,
            worker=name,
            machine=socket.gethostname(),
            experiment=experiment,
            job_name=job,
        )

        # now we want to compute another stage.save() to check if the stage is changed

        if job_obj is None:
            # TODO: timeout
            log.info("No more job found - exiting.")
            return

        fs = dvc.api.DVCFileSystem(url=None, rev=None)
        # This will search the DB and not rely on DVC run cache to determine if the job is cached
        #  so this can easily work across directories
        with fs.repo.lock:
            stage = fs.repo.stage.collect(job_obj["name"])[0]
            stage.save(allow_missing=True, run_cache=False)
            stage_lock = to_single_stage_lockfile(stage, with_files=True)
            reduced_lock = {
                k: v for k, v in stage_lock.items() if k in ["params", "deps", "cmd"]
            }
            deps_hash = _get_cache_hash(reduced_lock, key=True)
            cached_job = find_cached_job(deps_cache=deps_hash)
            if cached_job:
                log.info(f"Job '{job_obj['name']}' is cached and dvc.lock is available.")

        log.info(f"Running job '{job_obj['name']}'")
        # TODO: we need to ensure that all deps nodes are checked out!
        #  this will be important when clone / push.
        result = subprocess.run(
            f"dvc repro -s {job_obj['name']}", shell=True, capture_output=True
        )

        if result.returncode != 0:
            log.error(f"Failed to run job '{job_obj['name']}'")
            log.error(result.stderr.decode())
            complete_job(
                job_obj["id"],
                status="failed",
                lock={},
                stdout=result.stdout.decode(),
                stderr=result.stderr.decode(),
            )
        else:
            # get the stage_lock
            fs = dvc.api.DVCFileSystem(url=None, rev=None)
            with fs.repo.lock:
                stage = fs.repo.stage.collect(job_obj["name"])[0]
                stage.save()
            stage_lock = to_single_stage_lockfile(stage, with_files=True)
            complete_job(
                job_obj["id"],
                status="completed",
                lock=stage_lock,
                stdout=result.stdout.decode(),
                stderr=result.stderr.decode(),
            )


@app.command()
def submit(
    names: t.Optional[list[str]] = typer.Argument(
        None, help="Stage names to run. If not specified, run all stages."
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output."),
    check: bool = typer.Option(True, help="Check if stages are changed."),
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
    save_graph_to_db(
        graph,
        queues=custom_queues,
        commit=commit.hexsha,
        origin=origin,
        machine=socket.gethostname(),
    )
