import networkx as nx
from celery import chord, group

from paraffin.worker import repro, shutdown_worker


def submit_node_graph(subgraph: nx.DiGraph, shutdown_after_finished: bool = False):
    task_dict = {}
    for node in subgraph.nodes:
        task_dict[node.name] = repro.s(name=node.name)

    endpoints = []
    chords = {}

    for node in nx.topological_sort(subgraph):
        if len(list(subgraph.successors(node))) == 0:
            # if there are no successors, then add the node to the endpoints
            if node.name in chords:
                endpoints.append(chords[node.name])
            else:
                endpoints.append(task_dict[node.name])

        else:
            # for each successor, combine all predecessors into a chord
            for successor in subgraph.successors(node):
                if successor.name in chords:
                    continue
                deps = []
                for predecessor in subgraph.predecessors(successor):
                    if predecessor.name in chords:
                        deps.append(chords[predecessor.name])
                    else:
                        deps.append(task_dict[predecessor.name])
                chords[successor.name] = chord(deps, task_dict[successor.name])

    if shutdown_after_finished:
        chord(endpoints, shutdown_worker.s()).apply_async()
    else:
        group(endpoints).apply_async()
