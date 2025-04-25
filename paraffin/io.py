"""Handle config file IO for paraffin."""

import dataclasses
from fnmatch import fnmatch
from pathlib import Path

import networkx as nx
import yaml

from paraffin.dvc import StageDC


def update_max_workers(graph: nx.DiGraph, file: Path = Path("paraffin.yaml")) -> None:
    """Update the max_workers field in the config file.

    The config file is a YAML file with the following structure.
    It supports full stage names and wildcards.
    ```yaml
    max_workers:
        stage_name: max_workers
        stage_*: max_workers
    ```

    """
    if not file.exists():
        return
    with open(file, "r") as f:
        config = yaml.safe_load(f)

    mapping = {}
    for node in graph.nodes:
        node: StageDC
        for key, value in config.get("max_workers", {}).items():
            if fnmatch(node.addressing, key):
                print(
                    f"Updating {node.addressing} max_workers from {node.max_workers} to {value}"
                )
                new_node = dataclasses.replace(node, max_workers=value)
                mapping[node] = new_node

    nx.relabel_nodes(
        graph,
        mapping,
        copy=False,
    )
