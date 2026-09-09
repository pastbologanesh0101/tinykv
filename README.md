# TinyKV

A persistent key/value store built from scratch in pure Python, with a real
**B+ tree storage engine**, a **paged file format**, and a **write-ahead log**
for crash recovery. No third-party dependencies.

It exists to make the internals of an embedded database (SQLite, LMDB, BoltDB)
concrete: how records are laid out on disk, why a B+ tree and not a hash map,
how a database survives `kill -9` in the middle of a write.

```
┌──────────────────────────────────────────────┐
│  TinyKV        set / get / delete / scan      │   public API + REPL
├──────────────────────────────────────────────┤
│  WAL           append-only, fsync, CRC32      │   durability & atomicity
├──────────────────────────────────────────────┤
│  BPlusTree     nodes = pages, ordered scan    │   the index
├──────────────────────────────────────────────┤
│  Pager         4 KiB pages, free list, meta   │   bytes <-> disk
└──────────────────────────────────────────────┘
```

## Quick start

```bash
git clone https://github.com/pastbologanesh0101/tinykv
cd tinykv
pip install -e ".[dev]"

pytest -q                       # 24 tests
python examples/bench.py        # throughput numbers
python -m tinykv mydb/          # interactive REPL
```

```
tinykv> set user:1 alice
tinykv> set user:2 bob
tinykv> get user:1
alice
tinykv> scan user:
user:1 = alice
user:2 = bob
tinykv> stats
keys             2
pages            2
...
```

Library use:

```python
from tinykv import TinyKV

with TinyKV("mydb/") as db:
    db.set("user:1", "alice")
    db.set("user:2", "bob")
    print(db.get("user:1"))                 # -> "alice"
    for k, v in db.scan("user:"):           # ordered range scan by prefix
        print(k, v)
# closing checkpoints the data file and truncates the WAL
```

## Concepts demonstrated

| Area | Where | What it shows |
|---|---|---|
| **Storage engine** | `tinykv/pager.py` | Fixed-size pages, a meta/superblock page, an intrusive free list so deleted space is reused instead of leaking. |
| **B+ trees** | `tinykv/btree.py` | Leaf/internal split, borrow-from-sibling and merge on delete, leaves chained for `O(1)`-per-step ordered scans, height stays balanced. Splitting is driven by **serialized byte size**, so keys and values can be any length. |
| **Indexing** | `tinykv/btree.py` | Point lookup, prefix scan and `[start, end)` range scan all fall out of the same ordered structure — the reason real databases pick a B+ tree over a hash index. |
| **Durability (WAL)** | `tinykv/wal.py` | Write-ahead logging: append + `fsync` the intent before mutating the tree. CRC32 per record detects torn tail writes; replay stops at the first bad record. |
| **Crash recovery** | `tinykv/db.py` | On open, replay every WAL record after `meta.checkpoint_lsn`, then checkpoint. Covered by a test that kills the DB mid-write and reopens it. |
| **Serialization** | `pager.py`, `btree.py`, `wal.py` | Hand-rolled binary framing with `struct`, big-endian, length-prefixed fields. |

## How it works

### Pages (`Pager`)

The data file is an array of 4 KiB pages. Page 0 is the meta page: magic bytes,
page size, page count, B+ tree root page id, free-list head, and the checkpoint
LSN. `allocate_page()` pops from the free list if it can, otherwise grows the
file; `free_page()` pushes the page back onto the list (the freed page stores
the previous head — an intrusive linked list).

### The B+ tree (`BPlusTree`)

Each node is exactly one page.

* **Internal node**: `n` separator keys, `n + 1` child page ids.
* **Leaf node**: the key/value pairs plus a `next` pointer to the following
  leaf.

There is no fixed "order". A node splits when its serialized form would exceed
a page and merges/borrows when it drops below a quarter full. Delete uses a
**merge-preferred** strategy: merge whenever the combined node fits a page,
otherwise borrow one entry from a sibling (which must therefore be large enough
to stay healthy). `check_invariants()` verifies sortedness, key-range bounds,
equal leaf depth and a globally ordered leaf walk — the randomized tests call
it after thousands of mixed operations.

### Durability (`WAL` + checkpoints)

```
set(k, v):
    wal.append(PUT, k, v); wal.fsync()      # 1. log the intent, durably
    btree.put(k, v)                          # 2. update the index
    every N writes -> checkpoint()           # 3. fsync data file, truncate WAL
```

If the process dies between (1) and (3), the data file may hold half-written
pages. Reopening replays the WAL from the last checkpoint LSN and rebuilds a
consistent tree. LSNs are globally monotonic so a truncated WAL never confuses
recovery.

## Benchmarks

Pure Python, single thread, `fsync` on every write, Apple M-series laptop:

| Operation | Throughput |
|---|---|
| insert (durable, fsync per op) | ~30k ops/s |
| point get | ~850k ops/s |
| ordered full scan | ~6M pairs/s |
| delete | ~65k ops/s |

Insert is `fsync`-bound; batching writes or a group-commit WAL would raise it
substantially.

## Limitations (deliberate, for a teaching implementation)

- **Single process, single thread.** No locking or MVCC; one `TinyKV` handle at
  a time.
- **No transactions across multiple keys.** Each `set`/`delete` is atomic on its
  own.
- **Node cache is unbounded.** Loaded nodes stay in memory for the life of the
  handle — fine for millions of keys, not for arbitrarily large datasets.
- **Value size < one page.** ~4 KiB minus overhead. No overflow pages.
- **`str`/`bytes` keys and values only.**

## Project layout

```
tinykv/
  pager.py     paged file + free list + meta page
  btree.py     disk-backed B+ tree (split / merge / scan / invariant check)
  wal.py       write-ahead log with CRC32 framing
  db.py        TinyKV: ties it together, recovery, checkpointing
  cli.py       REPL
tests/         pager / btree / wal / db, incl. randomized model checks
examples/
  bench.py     throughput harness
```

## License

MIT
