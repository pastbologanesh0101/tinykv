"""Fixed-size page file — the lowest layer of the storage engine.

The database file is a flat array of ``page_size``-byte pages addressed by an
integer page id.  Page 0 is reserved for the meta page.  Freed pages are kept
on an intrusive singly linked free list so their space can be reused instead of
growing the file forever.

Nothing in here knows about B+ trees; it only moves bytes between memory and
disk and hands out page ids.
"""

from __future__ import annotations

import os
import struct
from dataclasses import dataclass

PAGE_SIZE = 4096
MAGIC = b"TINYKV01"

# Meta page layout (big-endian):
#   magic          8s
#   page_size      I
#   page_count     I   total pages that exist in the file
#   root_pid       I   root page of the B+ tree (0 until the first insert)
#   free_head      I   first page on the free list, or 0
#   checkpoint_lsn Q   highest WAL lsn whose effects are known to be on disk
_META = struct.Struct(">8sIIIIQ")


@dataclass
class Meta:
    page_size: int = PAGE_SIZE
    page_count: int = 1  # the meta page itself
    root_pid: int = 0
    free_head: int = 0
    checkpoint_lsn: int = 0


class Pager:
    def __init__(self, path: str, page_size: int = PAGE_SIZE):
        self.path = path
        self.page_size = page_size
        new = not os.path.exists(path) or os.path.getsize(path) == 0
        # ``r+b`` needs the file to exist; create it first if necessary.
        if new:
            open(path, "wb").close()
        self._f = open(path, "r+b")
        if new:
            self.meta = Meta(page_size=page_size)
            self._write_meta()
            self.sync()
        else:
            self.meta = self._read_meta()
            if self.meta.page_size != page_size:
                raise ValueError(
                    f"file page size {self.meta.page_size} != requested {page_size}"
                )

    # -- meta -------------------------------------------------------------
    def _read_meta(self) -> Meta:
        self._f.seek(0)
        raw = self._f.read(_META.size)
        magic, ps, pc, root, free, ckpt = _META.unpack(raw)
        if magic != MAGIC:
            raise ValueError("not a TinyKV file (bad magic)")
        return Meta(page_size=ps, page_count=pc, root_pid=root,
                    free_head=free, checkpoint_lsn=ckpt)

    def _write_meta(self) -> None:
        buf = bytearray(self.page_size)
        _META.pack_into(buf, 0, MAGIC, self.meta.page_size, self.meta.page_count,
                        self.meta.root_pid, self.meta.free_head,
                        self.meta.checkpoint_lsn)
        self._f.seek(0)
        self._f.write(buf)

    def flush_meta(self) -> None:
        self._write_meta()

    # -- raw page io ----------------------------------------------------
    def read_page(self, pid: int) -> bytes:
        if pid <= 0 or pid >= self.meta.page_count:
            raise IndexError(f"page {pid} out of range (count={self.meta.page_count})")
        self._f.seek(pid * self.page_size)
        data = self._f.read(self.page_size)
        if len(data) != self.page_size:
            raise IOError(f"short read on page {pid}")
        return data

    def write_page(self, pid: int, data: bytes) -> None:
        if len(data) > self.page_size:
            raise ValueError("page payload larger than page size")
        if pid <= 0 or pid >= self.meta.page_count:
            raise IndexError(f"page {pid} out of range")
        buf = data.ljust(self.page_size, b"\x00")
        self._f.seek(pid * self.page_size)
        self._f.write(buf)

    # -- allocation ---------------------------------------------------
    def allocate_page(self) -> int:
        """Return a fresh, zeroed page id, reusing the free list when possible."""
        if self.meta.free_head:
            pid = self.meta.free_head
            nxt = struct.unpack(">I", self.read_page(pid)[:4])[0]
            self.meta.free_head = nxt
            self.write_page(pid, b"\x00" * self.page_size)
            return pid
        pid = self.meta.page_count
        self.meta.page_count += 1
        # Physically grow the file so read_page/write_page bounds hold.
        self._f.seek((pid + 1) * self.page_size - 1)
        self._f.write(b"\x00")
        return pid

    def free_page(self, pid: int) -> None:
        buf = bytearray(self.page_size)
        struct.pack_into(">I", buf, 0, self.meta.free_head)
        self.write_page(pid, bytes(buf))
        self.meta.free_head = pid

    # -- lifecycle ---------------------------------------------------
    def sync(self) -> None:
        self._f.flush()
        os.fsync(self._f.fileno())

    def close(self) -> None:
        try:
            self.flush_meta()
            self.sync()
        finally:
            self._f.close()
