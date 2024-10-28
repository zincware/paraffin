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
    names: t.Optional[list[str]] = typer.Argument(None),
    concurrency: int = typer.Option(
        0, "--concurrency", "-c", envvar="PARAFFIN_CONCURRENCY"
    ),
    glob: bool = False,
    shutdown_after_finished: bool = typer.Option(
        False,
        "--shutdown-after-finished",
        "-s",
        envvar="PARAFFIN_SHUTDOWN_AFTER_FINISHED",
    ),
    show_mermaid: bool = typer.Option(True),
):
    from paraffin.worker import app as celery_app

    subgraph = get_stage_graph(names=names, glob=glob)
    custom_queues = get_custom_queue()

    # iterate disconnected subgraphs for better performance
    for sg in nx.connected_components(subgraph.to_undirected()):
        levels = dag_to_levels(subgraph.subgraph(sg))
        submit_node_graph(
            levels,
            shutdown_after_finished=shutdown_after_finished,
            custom_queues=custom_queues,
        )
        if show_mermaid:
            typer.echo(levels_to_mermaid(levels))

    typer.echo(f"Submitted all (n = {len(subgraph)})  tasks.")
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
