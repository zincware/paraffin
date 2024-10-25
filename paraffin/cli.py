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
from celery import chord, group

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
    from paraffin.worker import repro, shutdown_worker

    fs = dvc.api.DVCFileSystem(url=None, rev=None)
    graph = fs.repo.index.graph.reverse(copy=True)

    nodes = [x for x in graph.nodes if hasattr(x, "name")]
    if names is not None:
        if glob:
            nodes = [
                x for x in nodes if any(fnmatch.fnmatch(x.name, name) for name in names)
            ]
        else:
            nodes = [x for x in nodes if x.name in names]

    subgraph = get_subgraph_with_predecessors(graph, nodes)

    # remove all nodes that do not have a name
    subgraph = nx.subgraph_view(subgraph, filter_node=lambda x: hasattr(x, "name"))

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
