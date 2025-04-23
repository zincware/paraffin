import os
import socket

import typing as t
import subprocess
import json

import typer

app = typer.Typer()


@app.command()
def commit():
    """Commit all reproduced stages."""
    from paraffin.db.app import get_job, update_job, register_worker, close_worker
    from paraffin.dvc import StageStatus

    name = "paraffin"
    db = "sqlite:///paraffin.db"

    worker_id = register_worker(
        name=name,
        machine=socket.gethostname(),
        db_url=db,
        cwd=os.getcwd(),
        pid=os.getpid(),
    )
    # TODO: make this a DVC worker and ensure only one worker is running at a time

    import dvc.api

    while True:
        res = get_job(
            db_url=db,
            queues=None,
            worker_id=worker_id,
            experiment=None,
            stage_name=None,
            status=[StageStatus.REPRODUCED],
        )
        if res is None:
            break

        stage, job = res
        print(f"Updating lock file 'dvc.lock' for stage '{stage.name}'")
        subprocess.check_call(f"dvc commit --force --quiet {stage.name}", shell=True)

        update_job(
            db_url=db,
            stage_id=job.stage_id,
            status=StageStatus.FINISHED,
        )

    print("No job found.")
    close_worker(id=worker_id, db_url=db)


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
    from paraffin.db.app import get_job, register_worker, update_job, close_worker
    from paraffin.dvc import StageStatus

    worker_id = register_worker(
        name=name,
        machine=socket.gethostname(),
        db_url=db,
        cwd=os.getcwd(),
        pid=os.getpid(),
    )
    while True:
        res = get_job(
            db_url=db,
            queues=None,
            worker_id=worker_id,
            experiment=None,
            stage_name=None,
            status=[StageStatus.QUEUED],
        )
        if res is None:
            break
        
        stage, job = res
        cmd = json.loads(stage.cmd)
        print(f"({worker_id}) Running command: {cmd}")
        try:
            subprocess.check_call(cmd, shell=True)
            update_job(
                db_url=db,
                stage_id=job.stage_id,
                status=StageStatus.REPRODUCED,
            )
        except subprocess.CalledProcessError as e:
            print(f"({worker_id}) Command failed: {cmd}")
            update_job(
                db_url=db,
                stage_id=job.stage_id,
                status=StageStatus.FAILED,
            )

    print("No job found.")
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
    # TODO: cleanup
):
    """Run DVC stages in parallel."""
    # imports here for better performance
    from paraffin.dvc import get_status, print_graph_description
    from paraffin.db import save_graph_to_db

    graph = get_status()
    print_graph_description(graph) # TODO: read from database and not from dvc graph - this way the command can also be watdched
    save_graph_to_db(graph=graph, db_url=db)


@app.command()
def status():
    from paraffin.dvc import get_status, print_graph_description
    # TODO: status will also perform checkouts!!

    graph = get_status()
    print_graph_description(graph)