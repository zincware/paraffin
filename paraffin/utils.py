import fnmatch
import pathlib

import dvc.api
import networkx as nx
import yaml

from paraffin.abc import HirachicalStages


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


def get_stage_graph(names, glob=False):
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


def get_custom_queue():
    try:
        with pathlib.Path("paraffin.yaml").open() as f:
            config = yaml.safe_load(f)

            return config.get("queue", {})

    except FileNotFoundError:
        return {}


def dag_to_levels(graph) -> HirachicalStages:
    """Converts a directed acyclic graph (DAG) into hierarchical levels.

    This function takes a directed acyclic graph (DAG) and organizes its nodes
    into hierarchical levels based on their distance from the root nodes.
    A root node is defined as a node with no predecessors.

    Arguments
    ---------
    graph: newtorkx.DiGraph
        A directed acyclic graph represented using NetworkX.

    Returns
    -------
    HirachicalStages
        A dictionary where the keys are levels (integers)
        and the values are lists of nodes at that level.

    Example:
        >>> import networkx as nx
        >>> G = nx.DiGraph()
        >>> G.add_edges_from([(1, 2), (1, 3), (3, 4)])
        >>> dag_to_levels(G)
        {0: [1], 1: [2, 3], 2: [4]}
    """
    nodes = []
    levels = {}
    for start_node in graph.nodes():
        if len(list(graph.predecessors(start_node))) == 0:
            if start_node not in nodes:
                for node in nx.bfs_tree(graph, start_node):
                    if node not in nodes:
                        nodes.append(node)
                        level = nx.shortest_path_length(graph, start_node, node)
                        try:
                            levels[level].append(node)
                        except KeyError:
                            levels[level] = [node]
                    else:
                        # this part has already been added
                        break
    return levels


def levels_to_mermaid(all_levels: list[HirachicalStages]) -> str:
    # Initialize Mermaid syntax
    mermaid_syntax = "flowchart TD\n"

    for idx, levels in enumerate(all_levels):
        # Add each level as a subgraph
        for level, nodes in levels.items():
            mermaid_syntax += f"\tsubgraph Level{idx}:{level + 1}\n"
            for node in nodes:
                mermaid_syntax += f"\t\t{node.name}\n"
            mermaid_syntax += "\tend\n"

        # Add connections between levels
        for i in range(len(levels) - 1):
            mermaid_syntax += f"\tLevel{idx}:{i + 1} --> Level{idx}:{i + 2}\n"

    return mermaid_syntax
