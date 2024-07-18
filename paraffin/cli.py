import logging
import os
import subprocess
import threading
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from typing import List, Optional

import dvc.cli
import dvc.repo
import dvc.stage
import networkx as nx
import typer


log = logging.getLogger(__name__)

app = typer.Typer()

stages: List[dvc.stage.PipelineStage] = []
finished: set[str] = set()
submitted: set[Future] = set()
graph: nx.DiGraph = nx.DiGraph()
positions: dict = {}


def run_stage(stage_name: str, max_retries: int) -> str:
    """Run the DVC repro command for a given stage and retry if an error occurs."""
    command = ["dvc", "repro", "--single-item", stage_name, "--quiet"]
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


def execute_graph(max_workers: int, targets: List[str], max_retries: int):
    with dvc.repo.Repo() as repo:
        # graph: nx.DiGraph = repo.index.graph
        # add to the existing graph
        global graph
        graph.add_nodes_from(repo.index.graph.nodes)
        positions.update(nx.spring_layout(graph))
        # reverse the graph
        graph = graph.reverse()
        # construct a subgraph of the targets and their dependencies
        if targets:
            selected_stages = [
                stage for stage in graph.nodes if stage.addressing in targets
            ]
            graph = get_predecessor_subgraph(graph, selected_stages)
        log.debug("Graph:", graph)
        stages.extend(list(reversed(list(nx.topological_sort(graph)))))

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
    print("Finished all stages!")


@app.command()
def main(
    max_workers: Optional[int] = None,
    targets: List[str] = typer.Argument(None),
    max_retries: int = 3,
    dashboard: bool = True,
):
    if max_workers is None:
        max_workers = os.cpu_count()
        typer.echo(f"Using {max_workers} workers")

    if not dashboard:
        execute_graph(max_workers, targets, max_retries)
    else:
        from .dashboard import app as dashboard_app

        execution_thread = threading.Thread(
            target=execute_graph, args=(max_workers, targets, max_retries)
        )
        execution_thread.start()
        dashboard_app.run_server()


if __name__ == "__main__":
    app()
