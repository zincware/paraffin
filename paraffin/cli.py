import logging
import subprocess
import typing as t
import webbrowser

import dvc.api
import typer
import uvicorn
from dvc.stage.serialize import to_single_stage_lockfile

from paraffin.db import complete_job, get_job, save_graph_to_db, set_job_deps_lock
from paraffin.ui.app import app as webapp
from paraffin.utils import get_changed_stages, get_custom_queue, get_stage_graph

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
    print(queues)
    while True:
        job = get_job(queues=queues)
        if job is None:
            # TODO: timeout
            print("No job found.")
            return

        fs = dvc.api.DVCFileSystem(url=None, rev=None)
        with fs.repo.lock:
            stage = fs.repo.stage.collect(job["name"])[0]
            stage.save(allow_missing=True)
        stage_lock = to_single_stage_lockfile(stage, with_files=True)
        set_job_deps_lock(job["id"], stage_lock)

        try:
            subprocess.check_call(f"dvc repro -s {job['name']}", shell=True)
        except Exception as e:
            log.error(f"Failed to run job: {e}")
            complete_job(job["id"], status="failed")
        else:
            # get the stage_lock
            fs = dvc.api.DVCFileSystem(url=None, rev=None)
            with fs.repo.lock:
                stage = fs.repo.stage.collect(job["name"])[0]
                stage.save()
            stage_lock = to_single_stage_lockfile(stage, with_files=True)
            complete_job(job["id"], status="completed", lock=stage_lock)


@app.command()
def submit(
    names: t.Optional[list[str]] = typer.Argument(
        None, help="Stage names to run. If not specified, run all stages."
    ),
    verbose: bool = typer.Option(False, help="Verbose output."),
    check: bool = typer.Option(True, help="Check if stages are changed."),
):
    """Run DVC stages in parallel."""
    if verbose:
        logging.basicConfig(level=logging.DEBUG)

    log.debug("Getting stage graph")
    graph = get_stage_graph(names=names, glob=True)
    if check:
        changed = get_changed_stages(graph)
    else:
        changed = [node.name for node in graph.nodes]
    print(f"Changed stages: {changed}")
    print(f"Submitting {graph}")
    custom_queues = get_custom_queue()
    save_graph_to_db(graph, queues=custom_queues, changed=changed)
