import fnmatch

import dvc.api
import networkx as nx


def get_subgraph_with_predecessors(G, X, reverse=False):
    # Initialize a set to store nodes that will be in the subgraph
    nodes_to_include = set(X)

    # For each node in X, find all its predecessors
    for node in X:
        predecessors = nx.ancestors(G, node)
        nodes_to_include.update(predecessors)

    # Create the subgraph with the selected nodes
    if reverse:
        return G.subgraph(nodes_to_include).reverse(copy=True)
    return G.subgraph(nodes_to_include).copy()


def get_stage_graph(names, glob=False):
    fs = dvc.api.DVCFileSystem(url=None, rev=None)
    graph = fs.repo.index.graph.reverse(copy=True)

    nodes = [x for x in graph.nodes if hasattr(x, "name")]
    if names is not None:
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
