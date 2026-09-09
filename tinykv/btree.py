"""A disk-backed B+ tree.

Design notes
------------
* Every node is exactly one page.  Internal nodes hold ``n`` separator keys and
  ``n + 1`` child page ids; leaf nodes hold the actual key/value pairs and a
  ``next`` pointer so a full scan is a single walk along the bottom of the tree.
* Nodes are variable length.  Instead of a fixed "order" we split whenever a
  node's serialized form would not fit in a page, and merge/borrow whenever a
  node drops below half full.  That keeps the tree balanced with arbitrary
  key/value sizes.
* All keys and values are ``bytes``.  The higher layers deal with ``str``.

The tree is a pure index over a :class:`~tinykv.pager.Pager`; durability and
atomicity are the job of the WAL one layer up.
"""

from __future__ import annotations

import struct
from typing import Iterator, Optional

from .pager import Pager

LEAF = 1
INTERNAL = 2

_HEADER = struct.Struct(">BHI")  # node_type, num_keys, next_leaf (leaf only)
_U16 = struct.Struct(">H")
_U32 = struct.Struct(">I")

# Cap individual keys so a leaf can always hold at least a few of them and an
# internal split always makes progress.
MAX_KEY = 512


class Node:
    __slots__ = ("pid", "is_leaf", "keys", "values", "children", "next_leaf", "_page_size")

    def __init__(self, pid: int, is_leaf: bool, page_size: int):
        self.pid = pid
        self.is_leaf = is_leaf
        self.keys: list[bytes] = []
        self.values: list[bytes] = []      # leaf only, parallel to keys
        self.children: list[int] = []      # internal only, len == len(keys) + 1
        self.next_leaf = 0                  # leaf only
        self._page_size = page_size

    # -- serialization ----------------------------------------------
    def serialize(self) -> bytes:
        out = bytearray()
        if self.is_leaf:
            out += _HEADER.pack(LEAF, len(self.keys), self.next_leaf)
            for k, v in zip(self.keys, self.values):
                out += _U16.pack(len(k)) + k + _U32.pack(len(v)) + v
        else:
            out += _HEADER.pack(INTERNAL, len(self.keys), 0)
            out += _U32.pack(self.children[0])
            for k, child in zip(self.keys, self.children[1:]):
                out += _U16.pack(len(k)) + k + _U32.pack(child)
        if len(out) > self._page_size:
            raise ValueError("node overflow")
        return bytes(out)

    def byte_size(self) -> int:
        n = _HEADER.size
        if self.is_leaf:
            for k, v in zip(self.keys, self.values):
                n += 2 + len(k) + 4 + len(v)
        else:
            n += 4
            for k in self.keys:
                n += 2 + len(k) + 4
        return n

    @classmethod
    def deserialize(cls, pid: int, raw: bytes, page_size: int) -> "Node":
        node_type, num, next_leaf = _HEADER.unpack_from(raw, 0)
        off = _HEADER.size
        node = cls(pid, node_type == LEAF, page_size)
        if node.is_leaf:
            node.next_leaf = next_leaf
            for _ in range(num):
                (klen,) = _U16.unpack_from(raw, off); off += 2
                k = raw[off:off + klen]; off += klen
                (vlen,) = _U32.unpack_from(raw, off); off += 4
                v = raw[off:off + vlen]; off += vlen
                node.keys.append(k)
                node.values.append(v)
        else:
            (c0,) = _U32.unpack_from(raw, off); off += 4
            node.children.append(c0)
            for _ in range(num):
                (klen,) = _U16.unpack_from(raw, off); off += 2
                k = raw[off:off + klen]; off += klen
                (child,) = _U32.unpack_from(raw, off); off += 4
                node.keys.append(k)
                node.children.append(child)
        return node

    # -- helpers ---------------------------------------------------
    def is_underfull(self) -> bool:
        # Root is allowed to be as small as it likes; caller checks that.
        return self.byte_size() < self._page_size // 4

    def can_absorb(self, other: "Node") -> bool:
        extra = _HEADER.size if self.is_leaf else _HEADER.size + 4
        return self.byte_size() + other.byte_size() - extra <= self._page_size


class BPlusTree:
    def __init__(self, pager: Pager):
        self.pager = pager
        self._cache: dict[int, Node] = {}

    # -- node cache / io ------------------------------------------
    @property
    def root_pid(self) -> int:
        return self.pager.meta.root_pid

    def _load(self, pid: int) -> Node:
        node = self._cache.get(pid)
        if node is None:
            node = Node.deserialize(pid, self.pager.read_page(pid), self.pager.page_size)
            self._cache[pid] = node
        return node

    def _store(self, node: Node) -> None:
        self.pager.write_page(node.pid, node.serialize())
        self._cache[node.pid] = node

    def _new_node(self, is_leaf: bool) -> Node:
        pid = self.pager.allocate_page()
        node = Node(pid, is_leaf, self.pager.page_size)
        self._cache[pid] = node
        return node

    def _discard(self, pid: int) -> None:
        self._cache.pop(pid, None)
        self.pager.free_page(pid)

    def close_cache(self) -> None:
        self._cache.clear()

    # -- search --------------------------------------------------
    def _find_leaf(self, key: bytes) -> list[Node]:
        """Return the root-to-leaf path of nodes for ``key``."""
        path = [self._load(self.root_pid)]
        node = path[-1]
        while not node.is_leaf:
            i = _upper_bound(node.keys, key)
            node = self._load(node.children[i])
            path.append(node)
        return path

    def get(self, key: bytes) -> Optional[bytes]:
        if not self.root_pid:
            return None
        leaf = self._find_leaf(key)[-1]
        i = _lower_bound(leaf.keys, key)
        if i < len(leaf.keys) and leaf.keys[i] == key:
            return leaf.values[i]
        return None

    # -- insert -------------------------------------------------
    def put(self, key: bytes, value: bytes) -> None:
        if len(key) == 0 or len(key) > MAX_KEY:
            raise ValueError(f"key length must be 1..{MAX_KEY}")
        if _HEADER.size + 6 + len(key) + len(value) > self.pager.page_size:
            raise ValueError("key+value too large for a page")

        if not self.root_pid:
            root = self._new_node(is_leaf=True)
            root.keys.append(key)
            root.values.append(value)
            self._store(root)
            self.pager.meta.root_pid = root.pid
            return

        path = self._find_leaf(key)
        leaf = path[-1]
        i = _lower_bound(leaf.keys, key)
        if i < len(leaf.keys) and leaf.keys[i] == key:
            leaf.values[i] = value
        else:
            leaf.keys.insert(i, key)
            leaf.values.insert(i, value)

        if leaf.byte_size() <= self.pager.page_size:
            self._store(leaf)
            return
        self._split(path)

    def _split(self, path: list[Node]) -> None:
        node = path[-1]
        parent = path[-2] if len(path) >= 2 else None

        if node.is_leaf:
            mid = len(node.keys) // 2
            right = self._new_node(is_leaf=True)
            right.keys = node.keys[mid:]
            right.values = node.values[mid:]
            node.keys = node.keys[:mid]
            node.values = node.values[:mid]
            right.next_leaf = node.next_leaf
            node.next_leaf = right.pid
            sep = right.keys[0]
        else:
            mid = len(node.keys) // 2
            sep = node.keys[mid]
            right = self._new_node(is_leaf=False)
            right.keys = node.keys[mid + 1:]
            right.children = node.children[mid + 1:]
            node.keys = node.keys[:mid]
            node.children = node.children[:mid + 1]

        self._store(node)
        self._store(right)

        if parent is None:
            new_root = self._new_node(is_leaf=False)
            new_root.keys = [sep]
            new_root.children = [node.pid, right.pid]
            self._store(new_root)
            self.pager.meta.root_pid = new_root.pid
            return

        j = _upper_bound(parent.keys, sep)
        parent.keys.insert(j, sep)
        parent.children.insert(j + 1, right.pid)
        if parent.byte_size() <= self.pager.page_size:
            self._store(parent)
        else:
            self._split(path[:-1])

    # -- delete -----------------------------------------------
    def delete(self, key: bytes) -> bool:
        if not self.root_pid:
            return False
        path = self._find_leaf(key)
        leaf = path[-1]
        i = _lower_bound(leaf.keys, key)
        if i >= len(leaf.keys) or leaf.keys[i] != key:
            return False
        leaf.keys.pop(i)
        leaf.values.pop(i)
        self._store(leaf)
        self._rebalance(path)
        return True

    def _rebalance(self, path: list[Node]) -> None:
        node = path[-1]
        # Root handling: collapse an internal root down to its only child, or
        # drop the tree entirely once the last leaf empties.
        if len(path) == 1:
            if not node.is_leaf and len(node.children) == 1:
                self.pager.meta.root_pid = node.children[0]
                self._discard(node.pid)
            elif node.is_leaf and not node.keys:
                self.pager.meta.root_pid = 0
                self._discard(node.pid)
            return

        if not node.is_underfull():
            return

        parent = path[-2]
        idx = parent.children.index(node.pid)
        left = self._load(parent.children[idx - 1]) if idx > 0 else None
        right = self._load(parent.children[idx + 1]) if idx + 1 < len(parent.children) else None

        # Merge-preferred: merging keeps nodes fuller and is always correct when
        # the combined node fits a page.  Borrowing is only needed when it does
        # not — which means the sibling is large enough to give one entry away
        # and stay healthy.
        if left is not None and left.can_absorb(node):
            self._merge(parent, idx - 1, left, node)
        elif right is not None and node.can_absorb(right):
            self._merge(parent, idx, node, right)
        elif left is not None and len(left.keys) > 1:
            self._borrow_from_left(parent, idx, left, node)
            return
        elif right is not None and len(right.keys) > 1:
            self._borrow_from_right(parent, idx, node, right)
            return
        else:
            # Sibling has a single entry and cannot be merged: force the merge,
            # temporarily allowing a slightly over-min node (still <= page).
            if left is not None:
                self._merge(parent, idx - 1, left, node)
            elif right is not None:
                self._merge(parent, idx, node, right)
            else:
                return

        self._rebalance(path[:-1])

    def _borrow_from_left(self, parent: Node, idx: int, left: Node, node: Node) -> None:
        if node.is_leaf:
            node.keys.insert(0, left.keys.pop())
            node.values.insert(0, left.values.pop())
            parent.keys[idx - 1] = node.keys[0]
        else:
            node.keys.insert(0, parent.keys[idx - 1])
            node.children.insert(0, left.children.pop())
            parent.keys[idx - 1] = left.keys.pop()
        self._store(left)
        self._store(node)
        self._store(parent)

    def _borrow_from_right(self, parent: Node, idx: int, node: Node, right: Node) -> None:
        if node.is_leaf:
            node.keys.append(right.keys.pop(0))
            node.values.append(right.values.pop(0))
            parent.keys[idx] = right.keys[0]
        else:
            node.keys.append(parent.keys[idx])
            node.children.append(right.children.pop(0))
            parent.keys[idx] = right.keys.pop(0)
        self._store(right)
        self._store(node)
        self._store(parent)

    def _merge(self, parent: Node, sep_idx: int, left: Node, right: Node) -> None:
        if left.is_leaf:
            left.keys += right.keys
            left.values += right.values
            left.next_leaf = right.next_leaf
        else:
            left.keys.append(parent.keys[sep_idx])
            left.keys += right.keys
            left.children += right.children
        parent.keys.pop(sep_idx)
        parent.children.pop(sep_idx + 1)
        self._discard(right.pid)
        self._store(left)

    # -- iteration -------------------------------------------
    def items(self, start: Optional[bytes] = None, end: Optional[bytes] = None) -> Iterator[tuple[bytes, bytes]]:
        """Yield ``(key, value)`` in key order, optionally within ``[start, end)``."""
        if not self.root_pid:
            return
        if start is None:
            node = self._load(self.root_pid)
            while not node.is_leaf:
                node = self._load(node.children[0])
            leaf, i = node, 0
        else:
            leaf = self._find_leaf(start)[-1]
            i = _lower_bound(leaf.keys, start)
        while leaf is not None:
            while i < len(leaf.keys):
                k = leaf.keys[i]
                if end is not None and k >= end:
                    return
                yield k, leaf.values[i]
                i += 1
            leaf = self._load(leaf.next_leaf) if leaf.next_leaf else None
            i = 0

    # -- invariant check (used by tests) -------------------
    def check_invariants(self) -> None:
        if not self.root_pid:
            return
        seen_keys: list[bytes] = []

        def walk(pid: int, lo: Optional[bytes], hi: Optional[bytes], depth: int) -> int:
            node = self._load(pid)
            for k in node.keys:
                assert lo is None or k >= lo, "key below lower bound"
                assert hi is None or k < hi, "key above upper bound"
            assert node.keys == sorted(node.keys), "keys not sorted in node"
            if node.is_leaf:
                seen_keys.extend(node.keys)
                return depth
            assert len(node.children) == len(node.keys) + 1
            depths = set()
            bounds = [lo] + list(node.keys) + [hi]
            for c in range(len(node.children)):
                depths.add(walk(node.children[c], bounds[c], bounds[c + 1], depth + 1))
            assert len(depths) == 1, "leaves at differing depths"
            return depths.pop()

        walk(self.root_pid, None, None, 0)
        assert seen_keys == sorted(seen_keys), "leaf scan not globally ordered"
        assert len(seen_keys) == len(set(seen_keys)), "duplicate keys"


# -- ordered-list helpers (bisect works on parallel lists too) --------
def _lower_bound(keys: list[bytes], key: bytes) -> int:
    lo, hi = 0, len(keys)
    while lo < hi:
        mid = (lo + hi) // 2
        if keys[mid] < key:
            lo = mid + 1
        else:
            hi = mid
    return lo


def _upper_bound(keys: list[bytes], key: bytes) -> int:
    lo, hi = 0, len(keys)
    while lo < hi:
        mid = (lo + hi) // 2
        if keys[mid] <= key:
            lo = mid + 1
        else:
            hi = mid
    return lo
