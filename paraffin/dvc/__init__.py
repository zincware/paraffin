from dataclasses import dataclass
from enum import StrEnum

import dvc.api
import networkx as nx
from dvc.repo.reproduce import plan_repro
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
    """

    # TODO: what about cached, to we always want to checkout all files?

    QUEUED = "queued"
    COMPLETED = "completed"
    FINISHED = "finished"
    RUNNING = "running"
    UNFINISHED = "unfinished"
    FAILED = "failed"


@dataclass(frozen=True)
class StageDC:
    addressing: str
    status: StageStatus
    cmd: str | dict | None


def get_status(run_cache: bool = True, **kwargs) -> nx.DiGraph:
    # with run_cache false, we will not check for nodes that can be restored
    #  this is faster but can lead to computational overhead!

    fs = dvc.api.DVCFileSystem(**kwargs)
    repo = fs.repo

    graph = repo.index.graph
    steps = plan_repro(graph, stages=None)
    status = repo.status()
    # TODO! check for downstream stages, they might not have the correct status from DVC status!!

    results = {}

    for stage in tqdm(steps, desc="Checking stage status", unit="stage"):
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
                            cmd=stage.cmd if isinstance(stage, PipelineStage) else None,
                        )
                    except (RunCacheNotFoundError, FileNotFoundError):
                        results[stage] = StageDC(
                            addressing=stage.addressing,
                            status=StageStatus.QUEUED,
                            cmd=stage.cmd if isinstance(stage, PipelineStage) else None,
                        )
            else:
                results[stage] = StageDC(
                    addressing=stage.addressing,
                    status=StageStatus.QUEUED,
                    cmd=stage.cmd if isinstance(stage, PipelineStage) else None,
                )
        else:
            if any(stage.addressing in status for stage in nx.ancestors(graph, stage)):
                results[stage] = StageDC(
                    addressing=stage.addressing,
                    status=StageStatus.QUEUED,  # TODO: we need to check the run chache here as well!
                    cmd=stage.cmd if isinstance(stage, PipelineStage) else None,
                )
            else:
                results[stage] = StageDC(
                    addressing=stage.addressing,
                    status=StageStatus.COMPLETED,
                    cmd=stage.cmd if isinstance(stage, PipelineStage) else None,
                )

    assert len(results) == len(steps), (
        f"Expected {len(steps)} results, got {len(results)}"
    )
    return nx.relabel_nodes(graph, results, copy=True).reverse(copy=True)


def print_graph_description(graph: nx.DiGraph):
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
        else:
            table.add_row(stage.addressing, f"[red]❓ Unknown ({status})[/red]")

    console.print(table)
