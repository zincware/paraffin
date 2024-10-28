import typing as t

import networkx as nx
import typer

from paraffin.submit import submit_node_graph
from paraffin.utils import (
    dag_to_levels,
    get_custom_queue,
    get_stage_graph,
    levels_to_mermaid,
)

app = typer.Typer()


@app.command()
def main(
    names: t.Optional[list[str]] = typer.Argument(
        None, help="Stage names to run. If not specified, run all stages."
    ),
    concurrency: int = typer.Option(
        0,
        "--concurrency",
        "-c",
        envvar="PARAFFIN_CONCURRENCY",
        help="Number of stages to run in parallel. If not provided,"
        + " a celery worker has to be started manually.",
    ),
    glob: bool = typer.Option(
        False, "--glob", "-g", help="Use glob pattern to match stage names."
    ),
    shutdown_after_finished: bool = typer.Option(
        False,
        "--shutdown-after-finished",
        "-s",
        envvar="PARAFFIN_SHUTDOWN_AFTER_FINISHED",
        help="Shutdown the worker after all tasks are finished (experimental).",
    ),
    show_mermaid: bool = typer.Option(
        True, help="Visualize the parallel execution graph using Mermaid."
    ),
    dry: bool = typer.Option(False, help="Dry run. Do not submit tasks."),
):
    """Run DVC stages in parallel using Celery."""
    from paraffin.worker import app as celery_app

    graph = get_stage_graph(names=names, glob=glob)
    custom_queues = get_custom_queue()

    disconnected_subgraphs = list(nx.connected_components(graph.to_undirected()))
    disconnected_levels = [
        dag_to_levels(graph.subgraph(sg)) for sg in disconnected_subgraphs
    ]
    # iterate disconnected subgraphs for better performance
    if not dry:
        for levels in disconnected_levels:
            submit_node_graph(
                levels,
                shutdown_after_finished=shutdown_after_finished,
                custom_queues=custom_queues,
            )
    if show_mermaid:
        typer.echo(levels_to_mermaid(disconnected_levels))

    typer.echo(f"Submitted all (n = {len(graph)})  tasks.")
    if not dry:
        if concurrency > 0:
            celery_app.worker_main(
                argv=[
                    "worker",
                    "--loglevel=info",
                    f"--concurrency={concurrency}",
                    "--without-gossip",
                ]
            )
        else:
            typer.echo(
                "Start your celery worker using `celery -A paraffin.worker worker`"
                " and specify concurrency with `--concurrency`."
            )
