import fnmatch
import logging
import typing as t
from concurrent.futures import Future
from typing import List

import dvc.api
import dvc.cli
import dvc.repo
import dvc.stage
import networkx as nx
import tqdm
import typer


from paraffin.utils import get_stage_graph
from paraffin.submit import submit_node_graph

app = typer.Typer()


import networkx as nx


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
):
    from paraffin.worker import app as celery_app
    

    subgraph = get_stage_graph(names=names, glob=glob)
    submit_node_graph(subgraph, shutdown_after_finished=shutdown_after_finished)

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
            "Start your celery worker using `celery -A paraffin.worker worker` and specify concurrency with `--concurrency`."
        )
