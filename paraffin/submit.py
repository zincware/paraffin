import fnmatch

from celery import chain, group

from paraffin.abc import HirachicalStages
from paraffin.worker import repro, skipped_repro


def submit_node_graph(
    levels: HirachicalStages,
    custom_queues: dict[str, str],
    changed_stages: list[str],
    branch: str,
    origin: str | None,
    commit: bool,
    use_dvc: bool,
):
    per_level_groups = []
    for nodes in levels.values():
        group_tasks = []
        for node in nodes:
            if changed_stages and node.name not in changed_stages:
                group_tasks.append(skipped_repro.s())
            elif matched_pattern := next(
                (
                    pattern
                    for pattern in custom_queues
                    if fnmatch.fnmatch(node.name, pattern)
                ),
                None,
            ):
                group_tasks.append(
                    repro.s(
                        name=node.name,
                        cmd=node.cmd,
                        branch=branch,
                        commit=commit,
                        origin=origin,
                        use_dvc=use_dvc,
                    ).set(queue=custom_queues[matched_pattern])
                )
            else:
                group_tasks.append(
                    repro.s(
                        name=node.name,
                        cmd=node.cmd,
                        branch=branch,
                        commit=commit,
                        origin=origin,
                        use_dvc=use_dvc,
                    )
                )
        per_level_groups.append(group(group_tasks))

    workflow = chain(per_level_groups)
    workflow.apply_async()
