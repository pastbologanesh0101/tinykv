"""The public database object: ``open`` a directory, then ``set``/``get``/
``delete``/``scan``.

Durability model
----------------
1. ``set``/``delete`` append a record to the WAL and ``fsync`` it.
2. The change is applied to the B+ tree (pages written through the pager).
3. On a clean ``close`` — or every ``checkpoint_every`` writes — the data file
   and meta page are ``fsync``-ed and the WAL is truncated.

A crash between (1) and (3) is recovered on the next ``open`` by replaying the
WAL from ``meta.checkpoint_lsn``.
"""

from __future__ import annotations

import os
from typing import Iterator, Optional

from .btree import BPlusTree
from .pager import Pager
from .wal import DELETE, PUT, WAL

_DATA = "data.kv"
_WAL = "data.wal"


class TinyKV:
    def __init__(self, directory: str, checkpoint_every: int = 256):
        os.makedirs(directory, exist_ok=True)
        self.dir = directory
        self.checkpoint_every = checkpoint_every
        self._writes_since_ckpt = 0

        self.pager = Pager(os.path.join(directory, _DATA))
        self.tree = BPlusTree(self.pager)
        self.wal = WAL(os.path.join(directory, _WAL))
        self._recover()

    # -- context manager -------------------------------------
    def __enter__(self) -> "TinyKV":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # -- recovery -------------------------------------------
    def _recover(self) -> None:
        applied = 0
        for rec in self.wal.replay(self.pager.meta.checkpoint_lsn):
            if rec.op == PUT:
                self.tree.put(rec.key, rec.value)
            elif rec.op == DELETE:
                self.tree.delete(rec.key)
            applied += 1
        if applied:
            self._checkpoint()

    # -- writes -------------------------------------------
    def set(self, key, value) -> None:
        k, v = _b(key), _b(value)
        self.wal.append(PUT, k, v)
        self.wal.sync()
        self.tree.put(k, v)
        self._after_write()

    def delete(self, key) -> bool:
        k = _b(key)
        self.wal.append(DELETE, k, b"")
        self.wal.sync()
        removed = self.tree.delete(k)
        self._after_write()
        return removed

    def _after_write(self) -> None:
        self._writes_since_ckpt += 1
        if self._writes_since_ckpt >= self.checkpoint_every:
            self._checkpoint()

    def _checkpoint(self) -> None:
        self.pager.meta.checkpoint_lsn = self.wal.last_lsn
        self.pager.flush_meta()
        self.pager.sync()
        self.wal.truncate()
        self.wal.sync()
        self._writes_since_ckpt = 0

    # -- reads --------------------------------------------
    def get(self, key, default=None):
        v = self.tree.get(_b(key))
        return default if v is None else v.decode()

    def get_bytes(self, key) -> Optional[bytes]:
        return self.tree.get(_b(key))

    def __contains__(self, key) -> bool:
        return self.tree.get(_b(key)) is not None

    def scan(self, prefix: str = "", limit: Optional[int] = None) -> Iterator[tuple[str, str]]:
        p = prefix.encode()
        end = _prefix_upper_bound(p)
        n = 0
        for k, v in self.tree.items(start=p or None, end=end):
            yield k.decode(), v.decode()
            n += 1
            if limit is not None and n >= limit:
                return

    def __len__(self) -> int:
        return sum(1 for _ in self.tree.items())

    # -- lifecycle --------------------------------------
    def close(self) -> None:
        self._checkpoint()
        self.wal.close()
        self.pager.close()

    def stats(self) -> dict:
        return {
            "keys": len(self),
            "pages": self.pager.meta.page_count,
            "page_size": self.pager.page_size,
            "root_pid": self.pager.meta.root_pid,
            "checkpoint_lsn": self.pager.meta.checkpoint_lsn,
            "wal_lsn": self.wal.last_lsn,
            "file_bytes": os.path.getsize(os.path.join(self.dir, _DATA)),
        }


def _b(x) -> bytes:
    if isinstance(x, bytes):
        return x
    if isinstance(x, str):
        return x.encode()
    raise TypeError(f"expected str or bytes, got {type(x).__name__}")


def _prefix_upper_bound(prefix: bytes) -> Optional[bytes]:
    """Smallest key that is strictly greater than every key starting with ``prefix``."""
    if not prefix:
        return None
    b = bytearray(prefix)
    while b and b[-1] == 0xFF:
        b.pop()
    if not b:
        return None
    b[-1] += 1
    return bytes(b)
