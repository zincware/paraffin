import fnmatch
import logging
import pathlib
from collections import defaultdict

import dvc.api
import networkx as nx
import yaml

log = logging.getLogger(__name__)


def get_subgraph_with_predecessors(graph, nodes, reverse=False):
    """
    Generate a subgraph containing the specified nodes and all their predecessors.

    Parameters
    ----------
    graph: networkx.DiGraph
        The original graph from which the subgraph is to be extracted.
    nodes: Iterable
        An iterable of nodes to be included in the subgraph along with
        their predecessors.
    reverse: bool, optional
        If True, the resulting subgraph will be reversed. Default is False.

    Returns
    -------
    networkx.Graph
        A subgraph containing the specified nodes and all their predecessors.
    """
    # Initialize a set to store nodes that will be in the subgraph
    nodes_to_include = set(nodes)

    # For each node in X, find all its predecessors
    for node in nodes:
        predecessors = nx.ancestors(graph, node)
        nodes_to_include.update(predecessors)

    # Create the subgraph with the selected nodes
    if reverse:
        return graph.subgraph(nodes_to_include).reverse(copy=True)
    return graph.subgraph(nodes_to_include).copy()


def get_stage_graph(names, glob=False) -> nx.DiGraph:
    """
    Generates a subgraph of stages from a DVC repository based on provided names.

    Attributes
    ----------
    names: list
        A list of stage names to filter the graph nodes.
    glob: bool, optional
        If True, uses glob pattern matching for names. Defaults to False.

    Returns
    -------
    networkx.DiGraph:
        A subgraph containing the specified stages and their predecessors.
    """
    fs = dvc.api.DVCFileSystem(url=None, rev=None)
    graph = fs.repo.index.graph.reverse(copy=True)
    nodes = [x for x in graph.nodes if hasattr(x, "name")]
    if names is not None and len(names) > 0:
        if glob:
            nodes = [
                x for x in nodes if any(fnmatch.fnmatch(x.name, name) for name in names)
            ]
        else:
            nodes = [x for x in nodes if x.name in names]

    subgraph = get_subgraph_with_predecessors(graph, nodes)

    # remove all nodes that do not have a name
    subgraph = nx.subgraph_view(subgraph, filter_node=lambda x: hasattr(x, "name"))

    return subgraph


def get_changed_stages(subgraph) -> list:
    fs = dvc.api.DVCFileSystem(url=None, rev=None)
    repo = fs.repo
    names = [x.name for x in subgraph.nodes]
    log.debug(f"Checking status for stages: {names}")
    changed = list(repo.status(targets=names))
    graph = fs.repo.index.graph.reverse(copy=True)
    # find all downstream stages and add them to the changed list
    # Issue with changed stages is, if any upstream stage was changed
    # then we need to run ALL downstream stages, because
    # dvc status does not know / tell us because the immediate
    # upstream stage was unchanged at the point of checking.

    for name in changed:
        stage = next(x for x in graph.nodes if hasattr(x, "name") and x.name == name)
        for node in nx.descendants(graph, stage):
            changed.append(node.name)
    # TODO: split into definitely changed and maybe changed stages
    return changed


def get_custom_queue():
    try:
        with pathlib.Path("paraffin.yaml").open() as f:
            config = yaml.safe_load(f)

            return config.get("queue", {})

    except FileNotFoundError:
        return {}


def build_elk_hierarchy(graph: nx.DiGraph, node_width=100, node_height=50):
    """
    Export a networkx.DiGraph to a JSON structure compatible with ELK.js,
    including support for hierarchical subgraphs.

    Args:
        graph (nx.DiGraph): The directed graph to export.
        node_width (int): Default width for nodes.
        node_height (int): Default height for nodes.

    Returns:
        dict: JSON-compatible dictionary for ELK.js.
    """

    # Helper function to recursively build subgraph structure
    def build_subgraph_hierarchy(subgraph_nodes, path):
        """Recursively build subgraph children for a given path."""
        result = []
        children_by_group = defaultdict(list)

        for node in subgraph_nodes:
            group_path = tuple(graph.nodes[node].get("group", []))
            if group_path[: len(path)] == path:  # Node belongs in this subgraph
                if len(group_path) == len(path):  # Node is directly in this group
                    result.append(graph.nodes[node] | {"id": graph.nodes[node]["name"]})
                else:  # Node belongs in a subgroup
                    sub_group = group_path[len(path)]
                    children_by_group[sub_group].append(node)

        # Add subgroups recursively
        for sub_group, nodes in children_by_group.items():
            result.append(
                {
                    "id": "/".join(path + (sub_group,)),
                    "children": build_subgraph_hierarchy(nodes, path + (sub_group,)),
                }
            )

        return result

    # Collect nodes in the root group (group = [])
    root_nodes = [
        node for node in graph.nodes if not graph.nodes[node].get("group", [])
    ]

    elk_graph = {
        "id": "root",
        "children": build_subgraph_hierarchy(graph.nodes, ()),
        "edges": [
            {
                "id": f"{graph.nodes[source]['name']}-{graph.nodes[target]['name']}",
                "sources": [graph.nodes[source]["name"]],
                "targets": [graph.nodes[target]["name"]],
            }
            for source, target in graph.edges
        ],
    }

    return elk_graph
