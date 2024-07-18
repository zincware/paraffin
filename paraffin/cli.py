import dvc.stage
import networkx as nx
import dvc.repo
import dvc.cli
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List
import typer
import logging

log = logging.getLogger(__name__)
# set log level to debug
logging.basicConfig(level=logging.DEBUG)

app = typer.Typer()


def run_stage(stage_name: str):
    log.debug(f"Running stage {stage_name}")
    dvc.cli.main(["repro", stage_name, "--quiet"])
    log.debug(f"Finished stage {stage_name}")


@app.command()
def main(max_workers: int = 4):
    with dvc.repo.Repo() as repo:
        graph: nx.DiGraph = repo.index.graph
        stages: List[dvc.stage.PipelineStage] = list(nx.topological_sort(graph))
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(run_stage, stage.addressing) for stage in stages]
            for future in as_completed(futures):
                future.result()


if __name__ == "__main__":
    app()
