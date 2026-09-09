"""Write-ahead log for crash recovery.

Every mutation is appended here and ``fsync``-ed *before* the B+ tree pages are
touched.  If the process dies while pages are half-written, reopening the DB
replays every record whose ``lsn`` is newer than the last checkpoint, which
rebuilds a consistent index.

Record framing (big-endian)::

    lsn      Q
    op       B     1 = put, 2 = delete
    key_len  H
    val_len  I
    key      bytes
    val      bytes
    crc32    I     over everything above

A torn write at the tail fails the CRC check; replay stops there and the
partial record is truncated away.
"""

from __future__ import annotations

import os
import struct
import zlib
from typing import Iterator, NamedTuple

PUT = 1
DELETE = 2

_HDR = struct.Struct(">QBHI")
_CRC = struct.Struct(">I")


class Record(NamedTuple):
    lsn: int
    op: int
    key: bytes
    value: bytes


class WAL:
    def __init__(self, path: str):
        self.path = path
        if not os.path.exists(path):
            open(path, "wb").close()
        self._f = open(path, "r+b")
        self._f.seek(0, os.SEEK_END)
        self.last_lsn = self._scan_last_lsn()

    def _scan_last_lsn(self) -> int:
        last = 0
        for rec in self.replay(0):
            last = rec.lsn
        return last

    # -- append -------------------------------------------------
    def append(self, op: int, key: bytes, value: bytes) -> int:
        self.last_lsn += 1
        body = _HDR.pack(self.last_lsn, op, len(key), len(value)) + key + value
        frame = body + _CRC.pack(zlib.crc32(body) & 0xFFFFFFFF)
        self._f.seek(0, os.SEEK_END)
        self._f.write(frame)
        return self.last_lsn

    def sync(self) -> None:
        self._f.flush()
        os.fsync(self._f.fileno())

    # -- replay -----------------------------------------------
    def replay(self, after_lsn: int) -> Iterator[Record]:
        """Yield valid records with ``lsn > after_lsn`` in write order.

        Truncates the file at the first malformed/torn record.
        """
        self._f.seek(0)
        data = self._f.read()
        off = 0
        good_end = 0
        while off + _HDR.size + _CRC.size <= len(data):
            lsn, op, klen, vlen = _HDR.unpack_from(data, off)
            end = off + _HDR.size + klen + vlen + _CRC.size
            if end > len(data):
                break
            body = data[off:end - _CRC.size]
            (crc,) = _CRC.unpack_from(data, end - _CRC.size)
            if zlib.crc32(body) & 0xFFFFFFFF != crc:
                break
            key = body[_HDR.size:_HDR.size + klen]
            val = body[_HDR.size + klen:_HDR.size + klen + vlen]
            good_end = end
            off = end
            if lsn > after_lsn:
                yield Record(lsn, op, key, val)
        if good_end < len(data):
            # Drop a torn tail so future appends stay contiguous.
            self._f.truncate(good_end)

    # -- checkpoint -----------------------------------------
    def truncate(self) -> None:
        """Discard the log after its effects are durably in the data file.

        ``last_lsn`` deliberately keeps counting up so lsns stay globally
        monotonic across checkpoints.
        """
        self._f.seek(0)
        self._f.truncate(0)

    def close(self) -> None:
        try:
            self.sync()
        finally:
            self._f.close()
