import logging
import subprocess
import typing as t

import typer
import uvicorn

from paraffin.db import complete_job, get_job, save_graph_to_db
from paraffin.ui.app import app as webapp
from paraffin.utils import (
    get_custom_queue,
    get_stage_graph,
)

log = logging.getLogger(__name__)

app = typer.Typer()


@app.command()
def ui():
    """Start the Paraffin web UI."""
    # run the web app using uvicorn
    uvicorn.run(webapp, host="0.0.0.0", port=8000)


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
        try:
            subprocess.check_call(f"dvc repro -s {job['name']}", shell=True)
        except Exception as e:
            log.error(f"Failed to run job: {e}")
            complete_job(job["id"], status="failed")
        else:
            complete_job(job["id"], status="completed")


@app.command()
def submit(
    names: t.Optional[list[str]] = typer.Argument(
        None, help="Stage names to run. If not specified, run all stages."
    ),
    verbose: bool = typer.Option(False, help="Verbose output."),
):
    """Run DVC stages in parallel."""
    if verbose:
        logging.basicConfig(level=logging.DEBUG)

    log.debug("Getting stage graph")
    graph = get_stage_graph(names=names, glob=True)
    print(f"Submitting {graph}")
    custom_queues = get_custom_queue()
    save_graph_to_db(graph, queues=custom_queues)
