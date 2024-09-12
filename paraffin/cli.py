import contextlib
import fnmatch
import logging
import os
import pathlib
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


def update_gitignore():
    if not pathlib.Path(".gitignore").exists():
        with open(".gitignore", "w") as f:
            f.write(".parrafin_*\n")
        return
    with open(".gitignore", "r") as f:
        if ".parrafin_*" in f.read():
            return
    with open(".gitignore", "a") as f:
        f.write(".parrafin_*\n")


def get_paraffin_stage_file(addressing: str) -> pathlib.Path:
    return pathlib.Path(f".parrafin_{addressing}")


def get_tree_layout(graph):
    try:
        positions = nx.drawing.nx_agraph.graphviz_layout(graph, prog="dot")
    except ImportError:
        log.critical(
            "Graphviz is not available. Falling back to spring layout."
            "See https://pygraphviz.github.io/documentation/stable/install.html"
            " for installation instructions."
        )
        positions = nx.spring_layout(graph)
    return positions


def run_stage(stage_name: str, max_retries: int) -> bool:
    # get the stage from the DVC repo
    with dvc.repo.Repo() as repo:
        for _ in range(max_retries):
            with contextlib.suppress(LockError):
                with repo.lock:
                    stages = list(repo.stage.collect(stage_name))
                    if len(stages) != 1:
                        raise RuntimeError(f"Stage {stage_name} not found.")
                    stage = stages[0]
                    if stage.already_cached():
                        print(f"Stage '{stage_name}' didn't change, skipping")
                        return True
                    # try to restore the stage from the cache
                    # https://github.com/iterative/dvc/blob/main/dvc/stage/run.py#L166
                    with contextlib.suppress(RunCacheNotFoundError, FileNotFoundError):
                        stage.repo.stage_cache.restore(stage)
                        return True
                break
                # no LockError was raised and no return was
                # executed ->  the stage was not found in the cache

    print(f"Running stage '{stage_name}':")
    print(f"> {stage.cmd}")
    subprocess.check_call(stage.cmd, shell=True)

    for _ in range(max_retries):
        with contextlib.suppress(LockError):
            with repo.lock:
                stage.save()
                stage.commit()
                stage.dump(update_pipeline=False)
            return True
    else:
        raise RuntimeError(
            f"Failed to commit stage '{stage_name}' after {max_retries} retries."
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
    max_workers: int,
    targets: List[str],
    max_retries: int,
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
        try:
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                while len(finished) < len(stages):
                    # TODO: consider using proper workers / queues like celery with file system broker ?

                    for stage in stages:
                        # shuffling the stages might lead to better performance with multiple workers
                        if (
                            len(submitted) >= max_workers
                        ):  # do not queue more jobs than workers
                            break
                        if stage.addressing in finished:
                            continue
                        # if stage.addressing in submitted:
                        if get_paraffin_stage_file(stage.addressing).exists():
                            continue
                        # if a run finished in another paraffin process, if will be added
                        #  but automatically loaded from the DVC cache and added to the finished set
                        if not all(
                            pred.addressing in finished
                            for pred in graph.predecessors(stage)
                        ):
                            continue
                        get_paraffin_stage_file(stage.addressing).touch()
                        submitted[stage.addressing] = executor.submit(
                            run_stage, stage.addressing, max_retries
                        )

                    # iterare over the submitted stages and check if they are finished
                    for stage_addressing in list(submitted.keys()):
                        future = submitted[stage_addressing]
                        if future.done():
                            # check if an exception was raised
                            _ = future.result()
                            finished.add(stage_addressing)
                            del submitted[stage_addressing]
                            get_paraffin_stage_file(stage_addressing).unlink()
        finally:
            for stage_addressing in list(submitted.keys()):
                get_paraffin_stage_file(stage_addressing).unlink(missing_ok=True)

    print("Finished running all stages.")


@app.command()
def main(
    max_workers: Optional[int] = typer.Option(
        None, "--max-workers", "-n", help="Maximum number of workers to run in parallel"
    ),
    max_retries: int = typer.Option(
        10, "--max-retries", "-r", help="Maximum number of retries to commit a stage"
    ),
    dashboard: bool = typer.Option(
        False, "--dashboard", "-d", help="Enable the dashboard for monitoring"
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
    update_gitignore()

    if verbose:
        logging.basicConfig(level=logging.DEBUG)

    if max_workers is None:
        max_workers = os.cpu_count()

    if not dashboard:
        execute_graph(max_workers, targets, max_retries, glob)
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
            args=(max_workers, targets, max_retries, glob),
        )
        execution_thread.start()
        dashboard_app.run_server()


if __name__ == "__main__":
    app()
