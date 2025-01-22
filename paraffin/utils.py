import fnmatch
import logging
import pathlib

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
