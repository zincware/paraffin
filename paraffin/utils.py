import fnmatch
import json
import logging
import pathlib
from collections import defaultdict

import dvc.api
import networkx as nx
import yaml
from dvc.repo.status import _local_status

log = logging.getLogger(__name__)


def get_subgraph_with_predecessors(graph, nodes) -> nx.DiGraph:
    """
    Generate a subgraph containing the specified nodes and all their predecessors.

    Parameters
    ----------
    graph: networkx.DiGraph
        The original graph from which the subgraph is to be extracted.
    nodes: Iterable
        An iterable of nodes to be included in the subgraph along with
        their predecessors.

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

    return graph.subgraph(nodes_to_include).copy()


def get_stage_graph(names: list | None, force: bool, single_item: bool) -> nx.DiGraph:
    """
    Generates a subgraph of stages from a DVC repository based on provided names.

    Attributes
    ----------
    names: list|None
        A list of stage names to filter the graph nodes.
    force: bool
        Force rerun the selected stages
    single_item: bool
        only reproduce the names without upstream dependencies

    Returns
    -------
    networkx.DiGraph:
        A subgraph containing the specified stages and their predecessors.
    """
    from paraffin.stage import PipelineStageDC  # avoid circular import

    fs = dvc.api.DVCFileSystem(url=None, rev=None)
    graph = fs.repo.index.graph.reverse(copy=True)
    nodes = [x for x in graph.nodes if hasattr(x, "name")]
    if names is not None and len(names) > 0:
        nodes = [
            x for x in nodes if any(fnmatch.fnmatch(x.name, name) for name in names)
        ]

    if single_item:
        # If single_item is True, only include the specified
        # nodes without their predecessors
        subgraph = graph.subgraph(nodes)
    else:
        # Otherwise, include the specified nodes and their predecessors
        subgraph = get_subgraph_with_predecessors(graph, nodes)

    # remove all nodes that do not have a name
    subgraph = nx.subgraph_view(subgraph, filter_node=lambda x: hasattr(x, "name"))

    mapping = {}
    with fs.repo.lock:
        status = _local_status(fs.repo, check_updates=True, with_deps=True)
        for node in nx.topological_sort(subgraph):
            for pred in nx.ancestors(graph, node):
                if pred in mapping:
                    if mapping[pred].changed:
                        status[node.name] = status.get(node.name, []) + [
                            "changed by upstream"
                        ]
                        break

            mapping[node] = PipelineStageDC(
                stage=node,
                status=json.dumps(status.get(node.name, [])),
                force=force,
            )

    return nx.relabel_nodes(subgraph, mapping, copy=True)


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
    # root_nodes = [
    #     node for node in graph.nodes if not graph.nodes[node].get("group", [])
    # ]

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


def get_group(name: str) -> tuple[list[str], str]:
    """Extract the group from the job name."""
    parts = name.split("_")
    # check if parts[-1] is a number
    if parts[-1].isdigit():
        return parts[:-2], "_".join(parts[-2:])
    return parts[:-1], parts[-1]


def update_gitignore(line: str):
    """Add a line to the .gitignore file."""
    gitignore = pathlib.Path(".gitignore")
    if not gitignore.exists():
        gitignore.touch()

    with gitignore.open("r") as f:
        lines = f.readlines()

    if line not in lines:
        lines.append(line)

    with gitignore.open("w") as f:
        f.writelines(lines)


def replace_node_working_dir(
    path: str | pathlib.Path, ref_nwd: str | pathlib.Path, inp_nwd: str | pathlib.Path
) -> pathlib.Path:
    """
    Replace the reference node working directory (ref_nwd) in the given path
    with the input node working directory (inp_nwd), ignoring common prefixes.

    Parameters
    ----------
    path : str | Path
        The original path containing the ref_nwd.
    ref_nwd : str | Path
        The reference node working directory to be replaced.
    inp_nwd : str | Path
        The input node working directory to replace ref_nwd with.

    Returns
    -------
    Path
        The updated path with ref_nwd replaced by inp_nwd.
    """
    # Convert inputs to Path objects
    original_path = pathlib.Path(path)
    ref_nwd_path = pathlib.Path(ref_nwd)
    inp_nwd_path = pathlib.Path(inp_nwd)

    # Convert paths to relative strings for better matching
    ref_nwd_parts = ref_nwd_path.parts
    inp_nwd_parts = inp_nwd_path.parts
    path_parts = original_path.parts

    # Find where `ref_nwd` starts in `path`
    try:
        ref_index = path_parts.index(ref_nwd_parts[0])
        # Ensure the full `ref_nwd` matches
        if path_parts[ref_index : ref_index + len(ref_nwd_parts)] == ref_nwd_parts:
            # Replace the `ref_nwd` part with `inp_nwd`
            new_parts = (
                path_parts[:ref_index]
                + inp_nwd_parts
                + path_parts[ref_index + len(ref_nwd_parts) :]
            )
            return pathlib.Path(*new_parts)
    except ValueError:
        pass

    # If `ref_nwd` is not found, raise an error
    raise ValueError(f"Reference nwd '{ref_nwd}' not found in '{path}'.")


def detect_zntrack(lock: dict) -> bool:
    """Detect if the lock is a ZnTrack lock."""
    return "zntrack" in lock.get("cmd", "")
