import networkx as nx

from paraffin.utils import dag_to_levels


def test_dag_to_levels_1():
    """
    ```mermaid
    flowchart TD
        A --> C
        B --> C
    ```
    """
    G = nx.DiGraph()
    G.add_edges_from([("A", "C"), ("B", "C")])
    levels = dag_to_levels(G, branch="main", origin=None, commit=False)
    assert len(levels) == 2

    assert len(levels[0]) == 2
    assert len(levels[1]) == 1
    assert levels[0][0].stage == "A"
    assert levels[0][1].stage == "B"
    assert levels[1][0].stage == "C"


def test_dag_to_levels_2():
    """
    ```mermaid
    flowchart TD
        A --> B --> C
    ```
    """
    G = nx.DiGraph()
    G.add_edges_from([("A", "B"), ("B", "C")])
    levels = dag_to_levels(G, branch="main", origin=None, commit=False)
    assert len(levels) == 3

    assert len(levels[0]) == 1
    assert len(levels[1]) == 1
    assert len(levels[2]) == 1
    assert levels[0][0].stage == "A"
    assert levels[1][0].stage == "B"
    assert levels[2][0].stage == "C"


def test_dag_to_levels_3():
    """
    ```mermaid
    flowchart TD
        A --> B --> C
        A --> C
    ```
    """
    G = nx.DiGraph()
    G.add_edges_from([("A", "B"), ("B", "C"), ("A", "C")])
    levels = dag_to_levels(G, branch="main", origin=None, commit=False)
    assert len(levels) == 3

    assert len(levels[0]) == 1
    assert len(levels[1]) == 1
    assert len(levels[2]) == 1
    assert levels[0][0].stage == "A"
    assert levels[1][0].stage == "B"
    assert levels[2][0].stage == "C"


def test_dag_to_levles_4():
    """
    ```mermaid
    flowchart TD
        A --> D
        B --> D
        B --> E
        C --> E
    ```
    """
    G = nx.DiGraph()
    G.add_edges_from([("A", "D"), ("B", "D"), ("B", "E"), ("C", "E")])
    levels = dag_to_levels(G, branch="main", origin=None, commit=False)
    assert len(levels) == 2

    assert len(levels[0]) == 3
    assert len(levels[1]) == 2
    assert levels[0][0].stage == "A"
    assert levels[0][1].stage == "B"
    assert levels[0][2].stage == "C"
    assert levels[1][0].stage == "D"
    assert levels[1][1].stage == "E"
