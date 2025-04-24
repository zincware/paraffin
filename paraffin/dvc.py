import json
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

import dvc.api
import networkx as nx
from dvc.stage import PipelineStage
from dvc.stage.cache import RunCacheNotFoundError
from tqdm import tqdm


class StageStatus(StrEnum):
    """Stage status enum.

    Attributes
    ----------
    QUEUED : str
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
        Currently, this is the same as QUEUED and hashed dependency
        will not be accounted for.
    """

    # TODO: what about cached, to we always want to checkout all files?

    QUEUED = "queued"
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
    path: str = "."


def get_stage_from_graph(graph: nx.DiGraph, stage: str) -> StageDC:
    for node in graph.nodes:
        if node.addressing == stage:
            return node
    raise ValueError(f"Stage {stage} not found in graph")


def get_status(run_cache: bool = True, **kwargs) -> nx.DiGraph:
    # with run_cache false, we will not check for nodes that can be restored
    #  this is faster but can lead to computational overhead!

    fs = dvc.api.DVCFileSystem(**kwargs)
    repo = fs.repo

    graph = repo.index.graph.reverse(copy=True)
    status = repo.status()
    # TODO! check for downstream stages, they might not have the correct status from DVC status!!

    results = {}

    for stage in tqdm(
        nx.topological_sort(graph),
        total=len(graph.nodes),
        desc="Checking stage status",
        unit="stage",
    ):
        # TODO: only valid for pipeline stages
        if stage.addressing in status:
            if run_cache:
                with stage.repo.lock:
                    try:
                        # disable logging
                        # import logging
                        # logging.getLogger("dvc").setLevel(logging.CRITICAL)
                        # dry must be false, otherwise we will get wrong results!
                        stage.repo.stage_cache.restore(stage, dry=False)
                        # FYI, there is also stage.commit() which is like Stage.save(),
                        # but also saves file to the cache (i.e. commit).
                        # Stage.dump() is what saves the stage to dvc.yaml and
                        # dvc.lock file. (dump has update_pipeline=True|False and
                        # update_lock=True|False arguments to save to only
                        # one or to both of the files).

                        stage.save()
                        stage.dump()
                        results[stage] = StageDC(
                            addressing=stage.addressing,
                            status=StageStatus.COMPLETED,
                            cmd=json.dumps(stage.cmd)
                            if isinstance(stage, PipelineStage)
                            else None,
                            path=Path(stage.path_in_repo).parent.as_posix(),
                        )
                    except (RunCacheNotFoundError, FileNotFoundError):
                        results[stage] = StageDC(
                            addressing=stage.addressing,
                            status=StageStatus.QUEUED,
                            cmd=json.dumps(stage.cmd)
                            if isinstance(stage, PipelineStage)
                            else None,
                            path=Path(stage.path_in_repo).parent.as_posix(),
                        )
            else:
                results[stage] = StageDC(
                    addressing=stage.addressing,
                    status=StageStatus.QUEUED,
                    cmd=json.dumps(stage.cmd)
                    if isinstance(stage, PipelineStage)
                    else None,
                    path=Path(stage.path_in_repo).parent.as_posix(),
                )
        else:
            print(f"{stage.addressing} - {list(graph.predecessors(stage))}")
            if any(
                results[stage].status != StageStatus.COMPLETED
                for stage in graph.predecessors(stage)
            ):
                results[stage] = StageDC(
                    addressing=stage.addressing,
                    status=StageStatus.UNKNOWN,
                    cmd=json.dumps(stage.cmd)
                    if isinstance(stage, PipelineStage)
                    else None,
                    path=Path(stage.path_in_repo).parent.as_posix(),
                )
            else:
                results[stage] = StageDC(
                    addressing=stage.addressing,
                    status=StageStatus.COMPLETED,
                    cmd=json.dumps(stage.cmd)
                    if isinstance(stage, PipelineStage)
                    else None,
                    path=Path(stage.path_in_repo).parent.as_posix(),
                )

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

    for node in graph.nodes:
        stage: StageDC = node
        status = stage.status

        if status == StageStatus.COMPLETED:
            table.add_row(stage.addressing, "[green]✅ Finished[/green]")
        elif status == StageStatus.QUEUED:
            table.add_row(stage.addressing, "[yellow]🕐 Queued[/yellow]")
        elif status == StageStatus.RUNNING:
            table.add_row(stage.addressing, "[blue]🔄 Running[/blue]")
        elif status == StageStatus.UNFINISHED:
            table.add_row(stage.addressing, "[orange]⏳ Unfinished[/orange]")
        elif status == StageStatus.FAILED:
            table.add_row(stage.addressing, "[red]❌ Failed[/red]")
        elif status == StageStatus.FINISHED:
            table.add_row(stage.addressing, "[green]✅ Finished[/green]")
        else:
            table.add_row(stage.addressing, f"[red]❓ Unknown ({status})[/red]")

    console.print(table)
