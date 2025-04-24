import json
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

import dvc.api
import networkx as nx
from dvc.stage import PipelineStage
from dvc.stage.cache import RunCacheNotFoundError
from dvc.stage.serialize import to_single_stage_lockfile
from tqdm import tqdm


class StageStatus(StrEnum):
    """Stage status enum.

    Attributes
    ----------
    PENDING : str
        The stage is in the dvc.yaml but has not been run yet.
    COMPLETED : str
        The stage has been run and the output files are up to date.
        The stage is cached and the dvc.lock file is up to date.
    RUNNING : str
        The stage is currently running on a worker.
    UNFINISHED : str
        The stage is not running but has not been finished yet.
        A worker should pick it up and continue running it
        from the last checkpoint.
    FAILED : str
        The stage has failed and will not be run again.
    FINISHED : str
        The stage has been reproduced and the output files are up to date.
        The stage is not yet cached and the dvc.lock file is not up to date.
    UNKNOWN : str
        The stage has not been run yet.
        One or more dependencies have not been run yet as well.
        Therefore, the state can not be determined, because if all
        dependencies yield cached outputs, the stage might be
        in the run cache.
        Currently, this is the same as PENDING and hashed dependency
        will not be accounted for.
    """

    PENDING = "pending"
    COMPLETED = "completed"
    FINISHED = "finished"
    RUNNING = "running"
    UNFINISHED = "unfinished"
    FAILED = "failed"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class StageDC:
    addressing: str
    status: StageStatus
    cmd: str | None
    path: str
    lockfile: str | None


def get_stage_from_graph(graph: nx.DiGraph, stage: str) -> StageDC:
    for node in graph.nodes:
        if node.addressing == stage:
            return node
    raise ValueError(f"Stage {stage} not found in graph")


def _create_stage_dc(stage, status: StageStatus) -> StageDC:
    is_pipeline = isinstance(stage, PipelineStage)
    return StageDC(
        addressing=stage.addressing,
        status=status,
        cmd=json.dumps(stage.cmd) if is_pipeline else None,
        path=Path(stage.path_in_repo).parent.as_posix(),
        lockfile=json.dumps(to_single_stage_lockfile(stage, with_files=True))
        if is_pipeline
        else None,
    )


def _restore_and_classify(stage, run_cache: bool) -> StageDC:
    if run_cache:
        try:
            with stage.repo.lock:
                stage.repo.stage_cache.restore(stage, dry=False)
                stage.save()
                stage.dump()
                return _create_stage_dc(stage, StageStatus.COMPLETED)
        except (RunCacheNotFoundError, FileNotFoundError):
            return _create_stage_dc(stage, StageStatus.PENDING)
    else:
        return _create_stage_dc(stage, StageStatus.PENDING)


def get_status(run_cache: bool = True, **kwargs) -> nx.DiGraph:
    fs = dvc.api.DVCFileSystem(**kwargs)
    repo = fs.repo
    graph = repo.index.graph.reverse(copy=True)
    status = repo.status()

    results = {}
    for stage in tqdm(
        nx.topological_sort(graph),
        total=len(graph),
        desc="Checking stage status",
        unit="stage",
    ):
        if stage.addressing in status:
            results[stage] = _restore_and_classify(stage, run_cache)
        else:
            # Handle stages not in DVC status
            deps = list(graph.predecessors(stage))
            # convert to for loop to avoid long convoluted list comprehension
            if any(
                results.get(
                    dep, StageDC("", StageStatus.UNKNOWN, None, "", None)
                ).status
                != StageStatus.COMPLETED
                for dep in deps
            ):
                results[stage] = _create_stage_dc(stage, StageStatus.UNKNOWN)
            else:
                results[stage] = _create_stage_dc(stage, StageStatus.COMPLETED)

    assert len(results) == len(graph), (
        f"Expected {len(graph)} results, got {len(results)}"
    )
    return nx.relabel_nodes(graph, results, copy=True)


def print_graph_description(graph: nx.DiGraph):
    # TODO: read from database and not from dvc graph - this way the command can also be watdched
    from rich import box
    from rich.console import Console
    from rich.table import Table

    console = Console()
    table = Table(title="DVC Pipeline Stage Status", box=box.SIMPLE_HEAVY)
    table.add_column("Stage", justify="left", style="cyan", no_wrap=True)
    table.add_column("Status", justify="center", style="bold")

    status_icons = {
        StageStatus.COMPLETED: "[green]✅ Finished[/green]",
        StageStatus.FINISHED: "[green]✅ Finished[/green]",
        StageStatus.PENDING: "[yellow]🕐 Pending[/yellow]",
        StageStatus.RUNNING: "[blue]🔄 Running[/blue]",
        StageStatus.UNFINISHED: "[orange1]⏳ Unfinished[/orange1]",
        StageStatus.FAILED: "[red]❌ Failed[/red]",
        StageStatus.UNKNOWN: "[red]❓ Unknown[/red]",
    }

    for stage in graph.nodes:
        desc = status_icons.get(stage.status, f"[red]❓ Unknown ({stage.status})[/red]")
        table.add_row(stage.addressing, desc)

    console.print(table)


def cleanup_stages(graph: nx.DiGraph) -> None:
    import dvc.api

    fs = dvc.api.DVCFileSystem()
    # collect all stages that are queued
    stage_addressings = [
        stage.addressing
        for stage in graph.nodes
        if stage.status in [StageStatus.PENDING, StageStatus.UNKNOWN]
    ]

    stages = sum(
        (fs.repo.stage.collect(with_deps=False, target=ad) for ad in stage_addressings),
        [],
    )
    assert len(stages) == len(stage_addressings)
    for stage in tqdm(
        stages,
        total=len(stages),
        desc="Cleaning up stages",
        unit="stage",
    ):
        with stage.repo.lock:
            stage.remove_outs()
