import fnmatch
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
from dvc.lock import LockError
from dvc.stage.cache import RunCacheNotFoundError

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
    with dvc.repo.Repo() as repo:
        stages = repo.stage.collect(stage_name)
        if len(stages) != 1:
            raise RuntimeError(f"Stage {stage_name} not found.")
        stage = stages[0]
        if stage.already_cached():
            print(f"Stage '{stage_name}' didn't change, skipping")
            return stage_name
        # https://github.com/iterative/dvc/blob/main/dvc/stage/run.py#L166
        try:
            with repo.lock:
                stage.repo.stage_cache.restore(stage)
        except (RunCacheNotFoundError, FileNotFoundError):
            print(f"Running stage '{stage_name}':")
            print(f"> {stage.cmd}")
            subprocess.check_call(stage.cmd, shell=True)
            for _ in range(max_retries):
                try:
                    with repo.lock:
                        stage.save()
                        stage.commit()
                        stage.dump(update_pipeline=False)
                    break
                except LockError:
                    print(f"Failed to commit stage '{stage_name}', retrying...")
                    continue
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


def execute_graph(
    max_workers: int,
    targets: List[str],
    max_retries: int,
    quiet: bool,
    force: bool,
    glob: bool = False,
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
            if not glob:
                selected_stages = [
                    stage for stage in graph.nodes if stage.addressing in targets
                ]
            else:
                selected_stages = [
                    stage
                    for stage in graph.nodes
                    if any(
                        fnmatch.fnmatch(stage.addressing, target) for target in targets
                    )
                ]
                log.debug(f"Selected stages: {selected_stages} from {targets}")

            graph = get_predecessor_subgraph(graph, selected_stages)
        log.debug(f"Graph: {graph}")
        stages.extend(list(reversed(list(nx.topological_sort(graph)))))

        print(f"Running {len(stages)} stages using {max_workers} workers.")

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            while len(finished) < len(stages):
                for stage in stages:
                    if stage.addressing in finished:
                        continue
                    if stage.addressing in submitted:
                        continue
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
    glob: bool = typer.Option(
        False, help="Allows targets containing shell-style wildcards"
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose logging"
    ),
):
    """Run DVC stages in parallel."""
    if verbose:
        logging.basicConfig(level=logging.DEBUG)

    if max_workers is None:
        max_workers = os.cpu_count()

    if not dashboard:
        execute_graph(max_workers, targets, max_retries, quiet, force, glob)
    else:
        try:
            from .dashboard import app as dashboard_app
        except ImportError:
            typer.echo(
                "Dash is not installed. Please install it with `pip install dash` (see https://dash.plotly.com/installation)"
            )
            raise typer.Exit(1)

        execution_thread = threading.Thread(
            target=execute_graph,
            args=(max_workers, targets, max_retries, quiet, force, glob),
        )
        execution_thread.start()
        dashboard_app.run_server()


if __name__ == "__main__":
    app()
