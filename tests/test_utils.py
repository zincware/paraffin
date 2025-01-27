from paraffin.utils import get_group, replace_node_working_dir


def test_get_group():
    assert get_group("Node") == ([], "Node")
    assert get_group("Node_1") == ([], "Node_1")
    assert get_group("grp_Node") == (["grp"], "Node")
    assert get_group("grp_Node_1") == (["grp"], "Node_1")
    assert get_group("grp_a_Node") == (["grp", "a"], "Node")
    assert get_group("grp_a_Node_1") == (["grp", "a"], "Node_1")


def test_replace_node_working_dir():
    # 1
    inp_nwd = "nodes/grp/MyNode"
    ref_nwd = "nodes/MyNode_1"
    ref_path = "nodes/MyNode_1/node-meta.json"

    assert (
        replace_node_working_dir(ref_path, ref_nwd, inp_nwd).as_posix()
        == "nodes/grp/MyNode/node-meta.json"
    )

    # 2
    inp_nwd = "nodes/MyNode"
    ref_nwd = "nodes/MyNode"
    ref_path = "nodes/MyNode/node-meta.json"

    assert (
        replace_node_working_dir(ref_path, ref_nwd, inp_nwd).as_posix()
        == "nodes/MyNode/node-meta.json"
    )

    # 3
    inp_nwd = "nodes/MyNode_1"
    ref_nwd = "nodes/MyNode"
    ref_path = "nodes/MyNode/node-meta.json"

    assert (
        replace_node_working_dir(ref_path, ref_nwd, inp_nwd).as_posix()
        == "nodes/MyNode_1/node-meta.json"
    )

    # 4
    inp_nwd = "grp/MyNode"
    ref_nwd = "MyNode_1"
    ref_path = "nodes/MyNode_1/node-meta.json"

    assert (
        replace_node_working_dir(ref_path, ref_nwd, inp_nwd).as_posix()
        == "nodes/grp/MyNode/node-meta.json"
    )
