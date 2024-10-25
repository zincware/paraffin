import contextlib
import fnmatch
import logging
import os
import pathlib
import subprocess
import threading
from concurrent.futures import Future, ProcessPoolExecutor
from typing import List, Optional
import typing as t
import tqdm

from celery import chain, group, chord

import dvc.cli
import dvc.repo
import dvc.stage
import dvc.api
import networkx as nx
import typer
from dvc.lock import LockError
from dvc.stage.cache import RunCacheNotFoundError

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)  # Ensure the logger itself is set to INFO or lower

# Attach a logging handler to print info to stdout
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)

# Set a format for the handler
formatter = logging.Formatter("%(asctime)s %(message)s")
handler.setFormatter(formatter)

log.addHandler(handler)

app = typer.Typer()

stages: List[dvc.stage.PipelineStage] = []
finished: set[str] = set()
submitted: dict[str, Future] = {}
graph: nx.DiGraph = nx.DiGraph()
positions: dict = {}

import networkx as nx


def get_subgraph_with_predecessors(G, X, reverse=False):
    # Initialize a set to store nodes that will be in the subgraph
    nodes_to_include = set(X)

    # For each node in X, find all its predecessors
    for node in X:
        predecessors = nx.ancestors(G, node)
        nodes_to_include.update(predecessors)

    # Create the subgraph with the selected nodes
    if reverse:
        return G.subgraph(nodes_to_include).reverse(copy=True)
    return G.subgraph(nodes_to_include).copy()


@app.command()
def main(
    names: t.Optional[list[str]] = None,
    concurrency: int = 0,
    glob: bool = False,
    shutdown_after_finished: bool = False,
):
    from paraffin.worker import repro, shutdown_worker
    from paraffin.worker import app as celery_app

    fs = dvc.api.DVCFileSystem(url=None, rev=None)
    graph = fs.repo.index.graph.reverse(copy=True)

    if names is not None:
        nodes = [x for x in graph.nodes if getattr(x, "name", None) in names]
    else:
        nodes = [x for x in graph.nodes if hasattr(x, "name")]
    subgraph = get_subgraph_with_predecessors(graph, nodes)

    task_dict = {}
    for node in subgraph.nodes:
        task_dict[node.name] = repro.s(name=node.name)

    endpoints = []
    chords = {}

    for node in tqdm.tqdm(nx.topological_sort(subgraph)):
        if len(list(subgraph.successors(node))) == 0:
            # if there are no successors, then add the node to the endpoints
            if node.name in chords:
                endpoints.append(chords[node.name])
            else:
                endpoints.append(task_dict[node.name])

        else:
            # for each successor, combine all predecessors into a chord
            for successor in subgraph.successors(node):
                if successor.name in chords:
                    continue
                deps = []
                for predecessor in subgraph.predecessors(successor):
                    if predecessor.name in chords:
                        deps.append(chords[predecessor.name])
                    else:
                        deps.append(task_dict[predecessor.name])
                chords[successor.name] = chord(deps, task_dict[successor.name])

    if shutdown_after_finished:
        chord(endpoints, shutdown_worker.s()).apply_async()
    else:
        group(endpoints).apply_async()

    typer.echo(f"Submitted all (n = {len(task_dict)})  tasks.")
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
