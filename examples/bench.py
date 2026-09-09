"""Rough throughput numbers for the storage engine.

    python examples/bench.py            # default 50k keys
    python examples/bench.py 200000
"""

import random
import shutil
import sys
import time

from tinykv import TinyKV

N = int(sys.argv[1]) if len(sys.argv) > 1 else 50_000
DIR = "/tmp/tinykv-bench"
shutil.rmtree(DIR, ignore_errors=True)


def timed(label, fn):
    t0 = time.perf_counter()
    fn()
    dt = time.perf_counter() - t0
    print(f"{label:28} {dt:7.3f}s   {N / dt:>12,.0f} ops/s")


keys = [f"key:{i:09d}" for i in range(N)]
random.Random(0).shuffle(keys)

db = TinyKV(DIR)
timed("sequential-ish insert", lambda: [db.set(k, k * 2) for k in keys])
timed("point get (hit)", lambda: [db.get(k) for k in keys])
lookups = [f"key:{random.randint(0, N * 2):09d}" for _ in range(N)]
timed("point get (random)", lambda: [db.get(k) for k in lookups])


def full_scan():
    c = 0
    for _ in db.scan():
        c += 1
    assert c == N


timed("full ordered scan", full_scan)
timed("delete half", lambda: [db.delete(k) for k in keys[: N // 2]])
print("stats:", db.stats())
db.close()
shutil.rmtree(DIR, ignore_errors=True)
