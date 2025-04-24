import networkx as nx

from paraffin.db.app import export_db_to_graph, save_graph_to_db
from paraffin.dvc import get_status


def test_db_graph_conversion(proj01):
    db_path = "sqlite:///paraffin.db"
    status_graph: nx.DiGraph = get_status()

    save_graph_to_db(
        db_url=db_path,
        graph=status_graph,
    )

    db_graph: nx.DiGraph = export_db_to_graph(db_url=db_path)

    # You can directly compare nodes and edges
    assert len(status_graph) == 14
    assert len(status_graph.edges) == 12

    assert set(status_graph.nodes) == set(db_graph.nodes)
    assert set(status_graph.edges) == set(db_graph.edges)
