from paraffin.db import get_group


def test_get_group():
    assert get_group("Node") == []
    assert get_group("Node_1") == []
    assert get_group("grp_Node") == ["grp"]
    assert get_group("grp_Node_1") == ["grp"]
    assert get_group("grp_a_Node") == ["grp", "a"]
    assert get_group("grp_a_Node_1") == ["grp", "a"]
