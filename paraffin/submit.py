import fnmatch
import typing as t

import networkx as nx
from celery import chain, group

from paraffin.worker import repro, shutdown_worker
from paraffin.utils import dag_to_levels, levels_to_mermaid


def submit_node_graph(
    subgraph: nx.DiGraph,
    shutdown_after_finished: bool = False,
    custom_queues: t.Optional[dict] = None,
):  # noqa C901

    levels = dag_to_levels(subgraph)

    per_level_groups = []
    for nodes in levels.values():
        group_tasks = []
        for node in nodes:
            if matched_pattern := next(
                (
                    pattern
                    for pattern in custom_queues
                    if fnmatch.fnmatch(node.name, pattern)
                ),
                None,
            ):
                group_tasks.append(repro.s(name=node.name).set(queue=custom_queues[matched_pattern]))
            else:
                group_tasks.append(repro.s(name=node.name))
        per_level_groups.append(group(group_tasks))
    
    workflow = chain(per_level_groups)

    if shutdown_after_finished:
        chain(workflow, shutdown_worker.s()).apply_async()
    else:
        workflow.apply_async()

    print(levels_to_mermaid(levels))
