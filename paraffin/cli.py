import logging
import os
import subprocess
import time
import typing as t

import git
import networkx as nx
import tqdm
import typer

from paraffin.submit import submit_node_graph
from paraffin.utils import (
    dag_to_levels,
    get_changed_stages,
    get_custom_queue,
    get_stage_graph,
    levels_to_mermaid,
)

log = logging.getLogger(__name__)

app = typer.Typer()


@app.command()
def worker(
    concurrency: int = typer.Option(
        1,
        "--concurrency",
        "-c",
        envvar="PARAFFIN_CONCURRENCY",
        help="Number of concurrent tasks to run.",
    ),
    queues: str = typer.Option(
        "celery",
        "--queues",
        "-q",
        envvar="PARAFFIN_QUEUES",
        help="Comma separated list of queues to listen on.",
    ),
    shutdown_timeout: int = typer.Option(
        10, help="Timeout in seconds to wait for worker to shutdown."
    ),
    working_directory: str = typer.Option(
        ".", help="Working directory.", envvar="PARAFFIN_WORKING_DIRECTORY"
    ),
    cleanup: bool = typer.Option(
        True,
        help="Cleanup working directory after task completion.",
        envvar="PARAFFIN_CLEANUP",
    ),
):
    """Start a Celery worker."""
    from paraffin.worker import app as celery_app

    env = os.environ.copy()
    env["PARAFFIN_WORKING_DIRECTORY"] = working_directory
    env["PARAFFIN_CLEANUP"] = str(cleanup)

    proc = subprocess.Popen(
        [
            "celery",
            "-A",
            "paraffin.worker",
            "worker",
            "--loglevel=info",
            f"--concurrency={concurrency}",
            "-Q",
            queues,
        ],
        env=env,
    )
    time.sleep(
        shutdown_timeout
    )  # wait for the worker to start. TODO: use regex on output

    def auto_shutdown(timeout: float):
        """
        Monitors worker activity and shuts down workers if idle for `timeout` seconds.
        """
        inspect = celery_app.control.inspect()

        while True:
            # Get active tasks
            active_tasks = inspect.active()
            if active_tasks is not None and any(active_tasks.values()):
                time.sleep(timeout)
                continue
            print("No active tasks. Shutting down worker.")
            break

        # Shutdown worker
        celery_app.control.broadcast("shutdown", destination=list(active_tasks.keys()))

    auto_shutdown(shutdown_timeout)
    proc.wait()


@app.command()
def submit(
    names: t.Optional[list[str]] = typer.Argument(
        None, help="Stage names to run. If not specified, run all stages."
    ),
    glob: bool = typer.Option(
        False, "--glob", "-g", help="Use glob pattern to match stage names."
    ),
    show_mermaid: bool = typer.Option(
        True, help="Visualize the parallel execution graph using Mermaid."
    ),
    skip_unchanged: bool = typer.Option(
        False, help="Do not re-evaluate unchanged stages."
    ),
    dry: bool = typer.Option(False, help="Dry run. Do not submit tasks."),
    commit: bool = typer.Option(
        False, help="Automatically commit changes and push to remotes."
    ),
    verbose: bool = typer.Option(False, help="Verbose output."),
    use_dvc: bool = typer.Option(
        True,
        help="Use DVC to manage pipeline stages. Do not change"
        " this unless you know what you are doing.",
    ),
):
    """Run DVC stages in parallel using Celery."""
    if skip_unchanged:
        raise NotImplementedError("Skipping unchanged stages is not yet implemented.")
    if not use_dvc and commit:
        raise ValueError("Cannot commit changes without using DVC.")
    if verbose:
        logging.basicConfig(level=logging.DEBUG)

    log.debug("Getting stage graph")
    graph = get_stage_graph(names=names, glob=glob)
    log.debug("Getting changed stages")
    changed_stages = get_changed_stages(graph)
    custom_queues = get_custom_queue()

    repo = git.Repo()  # TODO: consider allow submitting remote repos
    try:
        origin = str(repo.remotes.origin.url)
    except AttributeError:
        origin = None

    log.debug("Converting graph to levels")
    disconnected_subgraphs = list(nx.connected_components(graph.to_undirected()))
    disconnected_levels = []
    for subgraph in disconnected_subgraphs:
        disconnected_levels.append(
            dag_to_levels(
                graph=graph.subgraph(subgraph),
            )
        )
    # iterate disconnected subgraphs for better performance
    if not dry:
        log.debug("Submitting node graph")
        for levels in disconnected_levels:
            # TODO: why not have the commit, repo, branch and origin as arguments here!
            submit_node_graph(
                levels,
                custom_queues=custom_queues,
                changed_stages=changed_stages,
                branch=str(repo.active_branch),
                origin=origin,
                commit=commit,
                use_dvc=use_dvc,
            )
    if show_mermaid:
        log.debug("Visualizing graph")
        typer.echo(
            levels_to_mermaid(disconnected_levels, changed_stages=changed_stages)
        )

    typer.echo(f"Submitted all (n = {len(graph)})  tasks.")
    typer.echo(
        "Start your celery worker using `paraffin worker`"
        " and specify concurrency with `--concurrency`."
    )


@app.command()
def commit(
    names: t.Optional[list[str]] = typer.Argument(
        None, help="Stage names to run. If not specified, run all stages."
    ),
    check: bool = typer.Option(False),
    verbose: bool = typer.Option(False, help="Verbose output."),
):
    if verbose:
        logging.basicConfig(level=logging.DEBUG)
    log.debug("Getting stage graph")
    graph = get_stage_graph(names=names, glob=True)
    if check:
        log.debug("Getting changed stages")
        changed_stages = get_changed_stages(graph)
    else:
        changed_stages = [node.name for node in graph.nodes]

    disconnected_subgraphs = list(nx.connected_components(graph.to_undirected()))
    disconnected_levels = []
    for subgraph in disconnected_subgraphs:
        disconnected_levels.append(
            dag_to_levels(
                graph=graph.subgraph(subgraph),
            )
        )

    tbar = tqdm.tqdm(
        disconnected_levels, desc="Committing stages", total=len(changed_stages)
    )

    for levels in disconnected_levels:
        for nodes in levels.values():
            for node in nodes:
                if node.name in changed_stages:
                    tbar.set_postfix(current=node.name)
                    cmd = ["dvc", "commit", node.name, "--force"]
                    res = subprocess.run(cmd, capture_output=True)
                    if res.returncode != 0:
                        log.error(f"Failed to commit {node.name}")
                        log.error(res.stderr.decode())
                        raise RuntimeError(f"Failed to commit {node.name}")
                    tbar.update()
