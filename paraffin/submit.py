import fnmatch
import typing as t

from celery import chain, group

from paraffin.abc import HirachicalStages
from paraffin.worker import repro


def submit_node_graph(
    levels: HirachicalStages,
    custom_queues: t.Optional[t.Dict[str, str]] = None,
):
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
                group_tasks.append(
                    repro.s(**node.to_dict()).set(queue=custom_queues[matched_pattern])
                )
            else:
                group_tasks.append(repro.s(**node.to_dict()))
        per_level_groups.append(group(group_tasks))

    workflow = chain(per_level_groups)
    workflow.apply_async()
