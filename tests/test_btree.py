import random

import pytest

from tinykv.btree import BPlusTree
from tinykv.pager import Pager


@pytest.fixture
def tree(tmp_path):
    pager = Pager(str(tmp_path / "t.kv"))
    yield BPlusTree(pager)
    pager.close()


def test_put_get_single(tree):
    tree.put(b"a", b"1")
    assert tree.get(b"a") == b"1"
    assert tree.get(b"missing") is None


def test_update_in_place(tree):
    tree.put(b"k", b"v1")
    tree.put(b"k", b"v2")
    assert tree.get(b"k") == b"v2"
    assert list(tree.items()) == [(b"k", b"v2")]


def test_sorted_scan_after_shuffled_insert(tree):
    keys = [f"key{i:04d}".encode() for i in range(500)]
    shuffled = keys[:]
    random.Random(1).shuffle(shuffled)
    for k in shuffled:
        tree.put(k, k[::-1])
    tree.check_invariants()
    assert [k for k, _ in tree.items()] == keys


def test_range_query(tree):
    for i in range(100):
        tree.put(f"{i:03d}".encode(), b"x")
    got = [k for k, _ in tree.items(start=b"010", end=b"020")]
    assert got == [f"{i:03d}".encode() for i in range(10, 20)]


def test_delete_keeps_tree_balanced(tree):
    keys = [f"k{i:05d}".encode() for i in range(800)]
    for k in keys:
        tree.put(k, b"value-" + k)
    random.Random(2).shuffle(keys)
    for k in keys[:600]:
        assert tree.delete(k) is True
    tree.check_invariants()
    remaining = set(keys[600:])
    assert {k for k, _ in tree.items()} == remaining
    for k in remaining:
        assert tree.get(k) == b"value-" + k


def test_delete_all_empties_tree(tree):
    for i in range(200):
        tree.put(str(i).encode(), b"v")
    for i in range(200):
        tree.delete(str(i).encode())
    assert tree.root_pid == 0
    assert list(tree.items()) == []


def test_randomized_model_check(tmp_path):
    pager = Pager(str(tmp_path / "m.kv"))
    tree = BPlusTree(pager)
    model: dict[bytes, bytes] = {}
    rng = random.Random(1234)
    for step in range(4000):
        k = f"{rng.randint(0, 300):03d}".encode()
        if rng.random() < 0.6:
            v = str(rng.randint(0, 10**6)).encode()
            tree.put(k, v)
            model[k] = v
        else:
            expect = k in model
            assert tree.delete(k) == expect
            model.pop(k, None)
        if step % 250 == 0:
            tree.check_invariants()
            assert dict(tree.items()) == model
    tree.check_invariants()
    assert dict(tree.items()) == model
    pager.close()


def test_large_values_force_splits(tree):
    big = b"z" * 900
    for i in range(60):
        tree.put(f"k{i:03d}".encode(), big)
    tree.check_invariants()
    assert tree.get(b"k042") == big
    assert tree.pager.meta.page_count > 5  # actually spread across pages


def test_key_length_limit(tree):
    with pytest.raises(ValueError):
        tree.put(b"x" * 10_000, b"v")
