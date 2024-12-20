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
    digraph = nx.DiGraph()
    digraph.add_edges_from([("A", "C"), ("B", "C")])
    levels = dag_to_levels(digraph)

    assert levels == {0: ["A", "B"], 1: ["C"]}


def test_dag_to_levels_2():
    """
    ```mermaid
    flowchart TD
        A --> B --> C
    ```
    """
    digraph = nx.DiGraph()
    digraph.add_edges_from([("A", "B"), ("B", "C")])
    levels = dag_to_levels(digraph)

    assert levels == {0: ["A"], 1: ["B"], 2: ["C"]}


def test_dag_to_levels_3():
    """
    ```mermaid
    flowchart TD
        A --> B --> C
        A --> C
    ```
    """
    digraph = nx.DiGraph()
    digraph.add_edges_from([("A", "B"), ("B", "C"), ("A", "C")])
    levels = dag_to_levels(digraph)

    assert levels == {0: ["A"], 1: ["B"], 2: ["C"]}


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
    digraph = nx.DiGraph()
    digraph.add_edges_from([("A", "D"), ("B", "D"), ("B", "E"), ("C", "E")])
    levels = dag_to_levels(digraph)

    assert levels == {0: ["A", "B", "C"], 1: ["D", "E"]}

def test_dag_to_levels_5():
    """
    ```mermaid
    flowchart TD
        A --> D
        B --> D
        B --> E
        C --> E

        D --> F
        E --> F

        B --> G
        E --> G

        F --> H
        C --> H

        G --> I
    ```
    """
    digraph = nx.DiGraph()
    digraph.add_edges_from([
        ("A", "D"), ("B", "D"), ("B", "E"), ("C", "E"),
        ("D", "F"), ("E", "F"),
        ("B", "G"), ("E", "G"),
        ("F", "H"), ("C", "H"),
        ("G", "I")
    ])
    levels = dag_to_levels(digraph)

    assert levels == {0: ["A", "B", "C"], 1: ["D", "E"], 2: ["F", "G"], 3: ["H", "I"]}
