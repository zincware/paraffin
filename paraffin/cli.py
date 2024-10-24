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
def main(names: t.Optional[list[str]] = None):
    from paraffin.worker import repro

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
    
    for node in nx.topological_sort(subgraph):
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

    group(endpoints).apply_async()

# @app.command()
# def main(
#     max_workers: Optional[int] = typer.Option(
#         None, "--max-workers", "-n", help="Maximum number of workers to run in parallel"
#     ),
#     max_retries: int = typer.Option(
#         10, "--max-retries", "-r", help="Maximum number of retries to commit a stage"
#     ),
#     dashboard: bool = typer.Option(
#         False, "--dashboard", "-d", help="Enable the dashboard for monitoring"
#     ),
#     targets: List[str] = typer.Argument(None, help="List of DVC targets to run"),
#     glob: bool = typer.Option(
#         False, help="Allows targets containing shell-style wildcards"
#     ),
#     verbose: bool = typer.Option(
#         False, "--verbose", "-v", help="Enable verbose logging"
#     ),
# ):
#     """Run DVC stages in parallel."""
    


if __name__ == "__main__":
    app()
