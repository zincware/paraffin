import dataclasses

import networkx as nx

from paraffin.db.app import StageStatus, query_existing_experiments
from paraffin.dvc import get_stage_from_graph


def prompt_transfer(stage_name: str, current_status: StageStatus) -> bool:
    print(f"Stage {stage_name} has status {current_status}")
    print(
        "Do you want to transfer the state of this stage to the new experiment? (Y/n)"
    )
    answer = input().lower().strip()
    return answer in ["", "y", "yes", "ja"]


def handle_existing_stages(graph, db):
    # Handle finished and unfinished stages
    for status in [StageStatus.FINISHED, StageStatus.UNFINISHED]:
        stages = query_existing_experiments(db_url=db, status=status, graph=graph)
        if not stages:
            continue

        print(f"Found {len(stages)} stages with {status} status from previous runs ")
        for stage in stages:
            should_transfer = prompt_transfer(stage.name, stage.status)
            new_status = stage.status if should_transfer else StageStatus.PENDING
            node = get_stage_from_graph(graph, stage.name)
            new_stage = dataclasses.replace(node, status=new_status)
            nx.relabel_nodes(graph, {node: new_stage}, copy=False)

    # Handle running stages
    running_stages = query_existing_experiments(
        db_url=db, status=StageStatus.RUNNING, graph=graph
    )
    if running_stages:
        print(
            f"Found {len(running_stages)} stages that are still running - "
            f"please stop the workers and run again."
        )
        return False

    return True
