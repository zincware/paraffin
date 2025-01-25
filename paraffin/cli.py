import logging
import socket
import subprocess
import typing as t
import webbrowser

import dvc.api
import git
import typer
import uvicorn
from dvc.stage.serialize import to_single_stage_lockfile

from paraffin.db import complete_job, get_job, save_graph_to_db, set_job_deps_lock
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
):
    """Start a Celery worker."""
    queues = queues.split(",")
    # set the log level
    logging.basicConfig(level=logging.INFO)
    log.info(f"Listening on queues: {queues}")
    while True:
        job = get_job(queues=queues)
        if job is None:
            # TODO: timeout
            log.info("No more job found - exiting.")
            return

        fs = dvc.api.DVCFileSystem(url=None, rev=None)
        with fs.repo.lock:
            stage = fs.repo.stage.collect(job["name"])[0]
            stage.save(allow_missing=True)
        stage_lock = to_single_stage_lockfile(stage, with_files=True)
        set_job_deps_lock(job["id"], stage_lock)

        result = subprocess.run(
            f"dvc repro -s {job['name']}", shell=True, capture_output=True
        )

        if result.returncode != 0:
            log.error(f"Failed to run job '{job['name']}'")
            log.error(result.stderr.decode())
            complete_job(
                job["id"],
                status="failed",
                lock={},
                stdout=result.stdout.decode(),
                stderr=result.stderr.decode(),
            )
        else:
            # get the stage_lock
            fs = dvc.api.DVCFileSystem(url=None, rev=None)
            with fs.repo.lock:
                stage = fs.repo.stage.collect(job["name"])[0]
                stage.save()
            stage_lock = to_single_stage_lockfile(stage, with_files=True)
            complete_job(
                job["id"],
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
