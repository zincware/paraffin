import os
import socket
import subprocess
import threading
import typing as t

import typer

from paraffin.db.app import (
    close_worker,
    get_job,
    register_worker,
    update_job,
)

app = typer.Typer()


@app.command()
def commit():
    """Commit all reproduced stages."""
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

    while True:
        res = get_job(
            db_url=db,
            queues=None,
            worker_id=worker_id,
            experiment=None,
            stage_name=None,
            status=[StageStatus.FINISHED],
        )
        if res is None:
            break

        stage, job = res
        print(f"Updating lock file 'dvc.lock' for stage '{stage.name}'")
        subprocess.check_call(f"dvc commit --force --quiet {stage.name}", shell=True)

        update_job(
            db_url=db,
            stage_id=job.stage_id,
            status=StageStatus.COMPLETED,
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
):
    """Start a paraffin worker to process the queued DVC stages."""
    from paraffin.worker import run_worker
    import signal

    shutdown_event = threading.Event()

    def handle_shutdown(*args, **kwargs):
        shutdown_event.set()

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    threads = []
    for _ in range(jobs):
        t = threading.Thread(
            target=run_worker,
            args=(name, db, shutdown_event),
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
    from paraffin.db.app import save_graph_to_db, query_existing_experiments
    from paraffin.dvc import get_status, print_graph_description, StageStatus, get_stage_from_graph
    import networkx as nx
    import dataclasses

    # TODO: if there is an experiment, set the stages to outdated

    graph = get_status()
    if stages := query_existing_experiments(db_url=db, status=StageStatus.FINISHED, graph=graph):
        print(
            f"Found {len(stages)} stages with finished status from previous runs that have not been commited."
        )
        # ask if the state should be transferred to the new experiment, e.g. the parameters and dependencies for this node did not change
        for stage in stages:
            print(f"Stage {stage.name} has status {stage.status}")
            print(
                f"Do you want to transfer the state of this stage to the new experiment? (y/n)"
            )
            answer = input().lower()
            # if no input was given, set to "y"
            if answer in ["", "y", "yes", "ja"]:
                answer = "y"
            node = get_stage_from_graph(graph, stage.name)
            if answer.lower() == "y":
                new_stage = dataclasses.replace(node, status=StageStatus.FINISHED)
                nx.relabel_nodes(
                    graph,
                    {node: new_stage},
                    copy=False
                )
            else:
                new_stage = dataclasses.replace(node, status=StageStatus.QUEUED)
                nx.relabel_nodes(
                    graph,
                    {node: new_stage},
                    copy=False
                )
    if stages := query_existing_experiments(db_url=db, status=StageStatus.RUNNING, graph=graph):
        print(
            f"Found {len(stages)} stages that are still running - please stop the workers and run again."
        )
        return
    if stages := query_existing_experiments(db_url=db, status=StageStatus.UNFINISHED, graph=graph):
        print(
            f"Found {len(stages)} stages that are still unfinished."
        )
        for stage in stages:
            print(f"Stage {stage.name} has status {stage.status}")
            print(
                f"Do you want to transfer the state of this stage to the new experiment? (y/n)"
            )
            answer = input().lower()
            # if no input was given, set to "y"
            if answer in ["", "y", "yes", "ja"]:
                answer = "y"
            node = get_stage_from_graph(graph, stage.name)
            if answer.lower() == "y":
                new_stage = dataclasses.replace(node, status=StageStatus.UNFINISHED)
                nx.relabel_nodes(
                    graph,
                    {node: new_stage},
                    copy=False
                )
            else:
                new_stage = dataclasses.replace(node, status=StageStatus.QUEUED)
                nx.relabel_nodes(
                    graph,
                    {node: new_stage},
                    copy=False
                )

    # cleanup all stages that are `queued`
    print_graph_description(
        graph
    )  
    save_graph_to_db(graph=graph, db_url=db)


@app.command()
def status():
    from paraffin.dvc import get_status, print_graph_description
    # TODO: status will also perform checkouts!!

    graph = get_status()
    print_graph_description(graph)
