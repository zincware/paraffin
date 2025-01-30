import pytest
from dvc.stage.cache import _get_cache_hash

from paraffin.lock import clean_lock, transform_lock


@pytest.fixture()
def lock01() -> tuple[dict, dict]:
    raw = {
        "cmd": "zntrack run zntrack.examples.AddNodeNumbers --name A_AddNodeNumbers",
        "params": {"params.yaml": {"A_AddNodeNumbers": {"params": 644999}}},
        "deps": [
            {
                "path": "nodes/A/1/ParamsToOuts/outs.json",
                "hash": "md5",
                "md5": "fb6d880180fbf208fab297f75d32c5ce",
            },
            {
                "path": "nodes/A/1/ParamsToOuts_1/outs.json",
                "hash": "md5",
                "md5": "fb6d880180fbf208fab297f75d32c5ce",
            },
        ],
        "outs": [
            {
                "path": "nodes/A/AddNodeNumbers/node-meta.json",
                "hash": "md5",
                "md5": "461013724d26fce139a6586d6635b42c",
            },
            {
                "path": "nodes/A/AddNodeNumbers/sum.json",
                "hash": "md5",
                "md5": "fb6d880180fbf208fab297f75d32c5ce",
            },
        ],
    }

    exp = {
        "cmd": "zntrack run zntrack.examples.AddNodeNumbers --name <node-name>",
        "params": {"params.yaml": {"<node-name>": {"params": 644999}}},
        "deps": [
            {
                "hash": "md5",
                "md5": "fb6d880180fbf208fab297f75d32c5ce",
            },
            {
                "hash": "md5",
                "md5": "fb6d880180fbf208fab297f75d32c5ce",
            },
        ],
    }

    return raw, exp


@pytest.fixture()
def lock02() -> tuple[dict, dict]:
    raw = {
        "cmd": "zntrack run package.MyNode --name MyNode",
        "params": {
            "cp2k.yaml": {"basis": "DZVP-MOLOPT-SR-GTH"},
            "params.yaml": {"MyNode": {"params": 644999}},
        },
        "outs": [
            {
                "path": "nodes/MyNode/node-meta.json",
                "hash": "md5",
                "md5": "461013724d0429e139a6586d6635b42c",
            },
        ],
    }

    exp = {
        "cmd": "zntrack run package.MyNode --name <node-name>",
        "params": {
            "cp2k.yaml": {"basis": "DZVP-MOLOPT-SR-GTH"},
            "params.yaml": {"<node-name>": {"params": 644999}},
        },
    }

    return raw, exp


@pytest.fixture()
def lock03() -> tuple[dict, dict]:
    raw = {
        "cmd": "zntrack run package.MyNode --name MyNode",
        "outs": [
            {
                "path": "nodes/MyNode/node-meta.json",
                "hash": "md5",
                "md5": "461013724d0429e139a6586d6635b42c",
            },
        ],
    }

    exp = {
        "cmd": "zntrack run package.MyNode --name <node-name>",
    }

    return raw, exp


@pytest.fixture()
def lock04() -> tuple[dict, dict]:
    raw = {
        "cmd": ["zntrack run package.MyNode --name MyNode"],
        "outs": [
            {
                "path": "nodes/MyNode/node-meta.json",
                "hash": "md5",
                "md5": "461013724d0429e139a6586d6635b42c",
            },
        ],
    }

    exp = {
        "cmd": ["zntrack run package.MyNode --name <node-name>"],
    }

    return raw, exp


@pytest.fixture()
def lock05() -> tuple[dict, dict]:
    raw = {
        "cmd": "zntrack run package.MyNode --name MyNode",
        "deps": [
            {
                "path": "data",
                "hash": "md5",
                "files": [
                    {
                        "relpath": "data.csv",
                        "md5": "421109828f8547af8727ca039ebd3d13",
                        "size": 23,
                    }
                ],
            },
            {
                "path": "nodes/MyNode/outs.json",
                "hash": "md5",
                "md5": "fb6d880180fbf208fab297f75d32c5ce",
            },
        ],
    }

    exp = {
        "cmd": "zntrack run package.MyNode --name <node-name>",
        "deps": [
            {
                "hash": "md5",
                "files": [
                    {
                        "md5": "421109828f8547af8727ca039ebd3d13",
                        "relpath": "data.csv",
                        # relpath should be fine, because it won't contain the node name
                        "size": 23,
                    }
                ],
            },
            {
                "hash": "md5",
                "md5": "fb6d880180fbf208fab297f75d32c5ce",
            },
        ],
    }

    return raw, exp


@pytest.fixture()
def lock_a_b() -> tuple[dict, dict]:
    """Two locks that should yield the same hash"""

    a = {
        "cmd": "zntrack run package.MyNode --name MyNode_1",
        "params": {"params.yaml": {"MyNode_1": {"params": 644999}}},
        "deps": [
            {
                "path": "nodes/SomeNodeA/ParamsToOuts_1/outs.json",
                "hash": "md5",
                "md5": "fb6d880180fbf208fab297f75d32c5ce",
            },
        ],
        "outs": [
            {
                "path": "nodes/MyNode_2/node-meta.json",
                "hash": "md5",
                "md5": "461013724d0429e139a6586d6635b42c",
            },
        ],
    }

    b = {
        "cmd": "zntrack run package.MyNode --name MyNode_2",
        "params": {"params.yaml": {"MyNode_2": {"params": 644999}}},
        "deps": [
            {
                "path": "nodes/SomeNodeB/ParamsToOuts_2/outs.json",
                "hash": "md5",
                "md5": "fb6d880180fbf208fab297f75d32c5ce",
            },
        ],
        "outs": [
            {
                "path": "nodes/MyNode_1/node-meta.json",
                "hash": "md5",
                "md5": "461013724d0429e139a6586d6635b42c",
            },
        ],
    }

    return a, b


@pytest.fixture()
def lock_input_ref_output() -> tuple[dict, dict, dict]:
    """
    Returns
    -------
    tuple[dict, dict, dict]
        Input hash only contains the lock of a not-yet executed stage but
        with the deps and params.
        Ref hash contains the lock of a stage that has been executed and
        is being loaded from the database.
        Output hash is the expected new lock from the input that should
        be written to "dvc.lock" to load the correct data.
    """
    inp = {
        "cmd": "zntrack run package.MyNode --name grp_MyNode",
        "params": {"params.yaml": {"grp_MyNode": {"params": 644999}}},
        "deps": [
            {
                "path": "nodes/ParamsToOuts/outs.json",
                "hash": "md5",
                "md5": "fb6d880180fbf208fab297f75d32c5ce",
            },
        ],
    }

    ref = {
        "cmd": "zntrack run package.MyNode --name MyNode_1",
        "params": {"params.yaml": {"MyNode_1": {"params": 644999}}},
        "deps": [
            {
                "path": "nodes/SomeNodeA/ParamsToOuts_1/outs.json",
                "hash": "md5",
                "md5": "fb6d880180fbf208fab297f75d32c5ce",
            },
        ],
        "outs": [
            {
                "path": "nodes/MyNode_1/node-meta.json",
                "hash": "md5",
                "md5": "461013724d26fce139a6586d6635b42c",
            },
            {
                "path": "nodes/MyNode_1/sum.json",
                "hash": "md5",
                "md5": "fb6d880180fbf208fab297f75d32c5ce",
            },
        ],
    }

    out = {
        "cmd": "zntrack run package.MyNode --name grp_MyNode",
        "params": {"params.yaml": {"grp_MyNode": {"params": 644999}}},
        "deps": [
            {
                "path": "nodes/ParamsToOuts/outs.json",
                "hash": "md5",
                "md5": "fb6d880180fbf208fab297f75d32c5ce",
            },
        ],
        "outs": [
            {
                "path": "nodes/grp/MyNode/node-meta.json",
                "hash": "md5",
                "md5": "461013724d26fce139a6586d6635b42c",
            },
            {
                "path": "nodes/grp/MyNode/sum.json",
                "hash": "md5",
                "md5": "fb6d880180fbf208fab297f75d32c5ce",
            },
        ],
    }

    return inp, ref, out


def test_cache_hash(lock01):
    assert (
        "ad8ecfc775a1d13b602acbfa3cf563f6616ca9d76ee25e8cf45fac390742743e"
        == _get_cache_hash(lock01[0], key=False)
    )
    assert (
        "10894a16d56e5f45557a481820ce0e188d7570ce900c31c3ddbbb0d325901bf5"
        == _get_cache_hash(lock01[1], key=False)
    )


def test_clean_lock01(lock01):
    raw, expected = lock01
    assert clean_lock(raw) == expected


def test_clean_lock02(lock02):
    raw, expected = lock02
    assert clean_lock(raw) == expected


def test_clean_lock03(lock03):
    raw, expected = lock03
    assert clean_lock(raw) == expected


def test_clean_lock04(lock04):
    raw, expected = lock04
    assert clean_lock(raw) == expected


def test_clean_lock05(lock05):
    raw, expected = lock05
    assert clean_lock(raw) == expected


def test_clean_lockAB(lock_a_b):
    a, b = lock_a_b
    clean_a = clean_lock(a)
    clean_b = clean_lock(b)

    assert clean_a == clean_b
    assert _get_cache_hash(clean_a, key=False) == _get_cache_hash(clean_b, key=False)


def test_transform_lock01(lock_input_ref_output):
    inp, ref, out = lock_input_ref_output
    assert transform_lock(inp, ref) == out
