import logging
import os
import subprocess
import threading
from concurrent.futures import Future, ProcessPoolExecutor
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
submitted: dict[str, Future] = {}
graph: nx.DiGraph = nx.DiGraph()
positions: dict = {}


def get_tree_layout(graph):
    try:
        positions = nx.drawing.nx_agraph.graphviz_layout(graph, prog="dot")
    except ImportError:
        log.critical(
            "Graphviz is not available. Falling back to spring layout."
            "See https://pygraphviz.github.io/documentation/stable/install.html"
            "for installation instructions."
        )
        positions = nx.spring_layout(graph)
    return positions


def run_stage(stage_name: str, max_retries: int, quiet: bool, force: bool) -> str:
    """Run the DVC repro command for a given stage and retry if an error occurs."""
    command = ["dvc", "repro", "--single-item", stage_name]
    if quiet:
        command.append("--quiet")
    if force:
        command.append("--force")
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


def execute_graph(
    max_workers: int, targets: List[str], max_retries: int, quiet: bool, force: bool
):
    with dvc.repo.Repo() as repo:
        # graph: nx.DiGraph = repo.index.graph
        # add to the existing graph
        global graph
        graph.add_nodes_from(repo.index.graph.nodes)
        graph.add_edges_from(repo.index.graph.edges)

        positions.update(get_tree_layout(graph))

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
                    if stage.addressing in submitted:
                        continue
                    # check if the stage is finished
                    if stage.already_cached() and not force:
                        finished.add(stage.addressing)
                        continue
                    if all(
                        pred.addressing in finished
                        for pred in graph.predecessors(stage)
                    ):
                        submitted[stage.addressing] = executor.submit(
                            run_stage, stage.addressing, max_retries, quiet, force
                        )

                # iterare over the submitted stages and check if they are finished
                for stage in list(submitted):
                    future = submitted[stage]
                    if future.done():
                        finished.add(future.result())

    print("Finished running all stages.")


@app.command()
def main(
    max_workers: Optional[int] = typer.Option(
        None, "--max-workers", "-n", help="Maximum number of workers to run in parallel"
    ),
    max_retries: int = typer.Option(
        1, "--max-retries", "-r", help="Maximum number of retries for failed stages"
    ),
    dashboard: bool = typer.Option(
        False, "--dashboard", "-d", help="Enable the dashboard for monitoring"
    ),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress output"),
    force: bool = typer.Option(
        False, "--force", "-f", help="Force execution even if DVC stages are up to date"
    ),
    targets: List[str] = typer.Argument(None, help="List of DVC targets to run"),
):
    """Run DVC stages in parallel."""
    if max_workers is None:
        max_workers = os.cpu_count()
        typer.echo(f"Using {max_workers} workers")

    if not dashboard:
        execute_graph(max_workers, targets, max_retries, quiet, force)
    else:
        try:
            from .dashboard import app as dashboard_app
        except ImportError:
            typer.echo(
                "Dash is not installed. Please install it with `pip install dash` (see https://dash.plotly.com/installation)"
            )
            raise typer.Exit(1)

        execution_thread = threading.Thread(
            target=execute_graph, args=(max_workers, targets, max_retries, quiet, force)
        )
        execution_thread.start()
        dashboard_app.run_server()


if __name__ == "__main__":
    app()
