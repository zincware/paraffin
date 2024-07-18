import logging
import os
import subprocess
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from typing import List, Optional

import dvc.cli
import dvc.repo
import dvc.stage
import networkx as nx
import typer

log = logging.getLogger(__name__)

app = typer.Typer()


def run_stage(stage_name: str, max_retries: int) -> str:
    """Run the DVC repro command for a given stage and retry if an error occurs."""
    command = ["dvc", "repro", "--single-item", stage_name]
    for attempt in range(max_retries):
        log.debug(f"Attempting {stage_name}, attempt {attempt + 1} of {max_retries}...")
        process = subprocess.Popen(command, stderr=subprocess.PIPE, text=True)
        failed = False

        # Read stderr line by line in real time
        while True:
            stderr_line = process.stderr.readline()

            if stderr_line:
                log.critical(stderr_line.strip())
                failed = True

            # Check if the process has finished
            if process.poll() is not None:
                break

        # Check for errors
        if not failed:
            return stage_name

        log.critical(
            f"Retrying {stage_name} due to error. Attempt {attempt + 1}/{max_retries}."
        )

    raise RuntimeError(
        f"Failed to run stage {stage_name} after {max_retries} attempts."
    )


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
def main(
    max_workers: Optional[int] = None,
    targets: List[str] = typer.Argument(None),
    max_retries: int = 3,
):
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
                    # check if the stage is finished
                    if stage.already_cached():
                        finished.add(stage.addressing)
                        continue
                    if all(
                        pred.addressing in finished
                        for pred in graph.predecessors(stage)
                    ):
                        submitted.add(
                            executor.submit(run_stage, stage.addressing, max_retries)
                        )

                for future in as_completed(submitted):
                    finished.add(future.result())


if __name__ == "__main__":
    app()
