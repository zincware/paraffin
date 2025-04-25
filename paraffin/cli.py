import os
import socket
import threading
import typing as t

import typer

from paraffin.db.app import (
    update_job,
)
from paraffin.db.models import Worker, Job

app = typer.Typer()


@app.command()
def ui(
    port: int = 8000,
    db: str = typer.Option(
        "sqlite:///paraffin.db", help="Database URL.", envvar="PARAFFIN_DB"
    ),
):
    """Start the Paraffin web UI."""
    import os
    import webbrowser

    import uvicorn

    from paraffin.ui.app import app as webapp

    webbrowser.open(f"http://localhost:{port}")
    os.environ["PARAFFIN_DB"] = db
    uvicorn.run(webapp, host="0.0.0.0", port=port)


@app.command()
def commit():
    """Commit all reproduced stages."""
    import json

    import dvc.api
    from dvc.stage.serialize import to_single_stage_lockfile
    from sqlmodel import create_engine

    from paraffin.dvc import StageStatus

    name = "paraffin"
    db = "sqlite:///paraffin.db"

    engine = create_engine(db)
    worker = Worker.register(
        name=name,
        machine=socket.gethostname(),
        engine=engine,
        cwd=os.getcwd(),
        pid=os.getpid(),
        requires_dvc_lock=True,
    )
    # TODO: make this a DVC worker and ensure only one worker is running at a time
    active_job = None
    fs = dvc.api.DVCFileSystem()
    while True:
        try:
            res = Job.create(
                engine=engine,
                queues=None,
                worker=worker,
                experiment=None,
                stage_name=None,
                status=[StageStatus.FINISHED],
            )
            if res is None:
                break

            stage, job = res
            active_job = job
            pipelinestage = list(
                fs.repo.stage.collect(stage.name)
            )  # TODO: does this work with path?
            if not pipelinestage:
                raise ValueError(f"Stage '{stage.name}' not found in DVC pipeline.")

            with pipelinestage[0].repo.lock:
                pipelinestage[0].save()
                pipelinestage[0].commit()
                pipelinestage[0].dump(update_pipeline=True, update_lock=True)

            update_job(
                engine=engine,
                stage_id=job.stage_id,
                status=StageStatus.COMPLETED,
                lockfile=json.dumps(
                    to_single_stage_lockfile(pipelinestage[0], with_files=True)
                ),
            )
            active_job = None
        finally:
            if active_job:
                update_job(
                    engine=engine,
                    stage_id=active_job.stage_id,
                    status=StageStatus.FINISHED,
                )
                active_job = None

    print("No job found.")
    worker.close(engine=engine)


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
):
    """Start a paraffin worker to process the queued DVC stages."""
    import signal

    from sqlmodel import create_engine

    from paraffin.worker import run_worker

    shutdown_event = threading.Event()

    def handle_shutdown(*args, **kwargs):
        shutdown_event.set()

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    engine = create_engine(db)
    # TODO: check!!

    threads = []
    for _ in range(jobs):
        t = threading.Thread(
            target=run_worker,
            args=(name, engine, shutdown_event, timeout),
            daemon=True,
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
    print("All workers done.")


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
    # TODO: cleanup
):
    """Run DVC stages in parallel."""
    # imports here for better performance
    from sqlmodel import (
        SQLModel,
        create_engine,
    )

    from paraffin.db.app import save_graph_to_db, update_existing_experiment_stages
    from paraffin.dvc import cleanup_stages, get_status, print_graph_description
    from paraffin.io import update_max_workers
    from paraffin.utils import handle_existing_stages

    # TODO: if there is an experiment, set the stages to outdated

    graph = get_status()

    engine = create_engine(db)
    SQLModel.metadata.create_all(engine)

    handle_existing_stages(graph=graph, engine=engine)
    update_existing_experiment_stages(engine=engine)
    cleanup_stages(graph=graph)
    # cleanup all stages that are `queued`
    update_max_workers(graph=graph)
    print_graph_description(graph)
    save_graph_to_db(graph=graph, engine=engine)


@app.command()
def status():
    from paraffin.dvc import get_status, print_graph_description
    # TODO: status will also perform checkouts!!

    graph = get_status()
    print_graph_description(graph)
