import logging
import os
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from typing import List, Optional

import dvc.cli
import dvc.repo
import dvc.stage
import networkx as nx
import typer

log = logging.getLogger(__name__)

app = typer.Typer()


def run_stage(stage_name: str) -> str:
    # We use `single-item` for performance reasons
    # all previous stages are already computed
    dvc.cli.main(["repro", "--single-item", stage_name])
    return stage_name


def get_predecessor_subgraph(
    graph: nx.DiGraph, nodes: List[dvc.stage.PipelineStage]
) -> nx.DiGraph:
    # Create an empty set to hold all nodes to be included in the subgraph
    subgraph_nodes = set(nodes)

    # Iterate over each node and add all its ancestors (predecessors) to the set
    for node in nodes:
        subgraph_nodes.update(nx.ancestors(graph, node))

    # Create the subgraph with the collected nodes
    subgraph = graph.subgraph(subgraph_nodes).copy()

    return subgraph


@app.command()
def main(max_workers: Optional[int] = None, targets: List[str] = typer.Argument(None)):
    if max_workers is None:
        max_workers = os.cpu_count()
        typer.echo(f"Using {max_workers} workers")
    with dvc.repo.Repo() as repo:
        graph: nx.DiGraph = repo.index.graph
        # reverse the graph
        graph = graph.reverse()
        # construct a subgraph of the targets and their dependencies
        if targets:
            stages = [stage for stage in graph.nodes if stage.addressing in targets]
            graph = get_predecessor_subgraph(graph, stages)
        log.debug("Graph:", graph)
        stages: List[dvc.stage.PipelineStage] = list(
            reversed(list(nx.topological_sort(graph)))
        )

        finished: set[str] = set()
        submitted: set[Future] = set()
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            while len(finished) < len(stages):
                for stage in stages:
                    if stage.addressing in finished:
                        continue
                    if all(
                        pred.addressing in finished
                        for pred in graph.predecessors(stage)
                    ):
                        submitted.add(executor.submit(run_stage, stage.addressing))

                for future in as_completed(submitted):
                    finished.add(future.result())


if __name__ == "__main__":
    app()
