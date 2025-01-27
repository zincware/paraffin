import pytest

from dvc.stage.cache import _get_cache_hash
from paraffin.lock import clean_lock


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
        "outs": [
            {
                "hash": "md5",
                "md5": "461013724d26fce139a6586d6635b42c",
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
        "outs": [
            {
                "hash": "md5",
                "md5": "461013724d0429e139a6586d6635b42c",
            },
        ],
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
        "outs": [
            {
                "hash": "md5",
                "md5": "461013724d0429e139a6586d6635b42c",
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


def test_cache_hash(lock01):
    assert (
        "ad8ecfc775a1d13b602acbfa3cf563f6616ca9d76ee25e8cf45fac390742743e"
        == _get_cache_hash(lock01[0], key=False)
    )
    assert (
        "d6ebbacc4cb4482e7ee87d4ed0399d469128bfbb49abab72b65951f687e7c3a6"
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


def test_clean_lockAB(lock_a_b):
    a, b = lock_a_b
    clean_a = clean_lock(a)
    clean_b = clean_lock(b)

    assert clean_a == clean_b
    assert _get_cache_hash(clean_a, key=False) == _get_cache_hash(clean_b, key=False)
