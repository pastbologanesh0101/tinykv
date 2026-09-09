import random

import pytest

from tinykv.db import TinyKV


def test_basic_crud(tmp_path):
    with TinyKV(str(tmp_path / "db")) as db:
        db.set("a", "apple")
        db.set("b", "banana")
        assert db.get("a") == "apple"
        assert "b" in db
        assert db.delete("a") is True
        assert db.get("a") is None
        assert db.delete("a") is False
        assert len(db) == 1


def test_scan_by_prefix(tmp_path):
    with TinyKV(str(tmp_path / "db")) as db:
        db.set("user:1", "alice")
        db.set("user:2", "bob")
        db.set("post:1", "hello")
        assert list(db.scan("user:")) == [("user:1", "alice"), ("user:2", "bob")]
        assert list(db.scan("user:", limit=1)) == [("user:1", "alice")]
        assert list(db.scan()) == [
            ("post:1", "hello"),
            ("user:1", "alice"),
            ("user:2", "bob"),
        ]


def test_persistence_across_reopen(tmp_path):
    d = str(tmp_path / "db")
    with TinyKV(d) as db:
        for i in range(1000):
            db.set(f"k{i:04d}", f"v{i}")
    with TinyKV(d) as db:
        assert len(db) == 1000
        assert db.get("k0500") == "v500"
        db.delete("k0500")
    with TinyKV(d) as db:
        assert db.get("k0500") is None
        assert len(db) == 999


def test_crash_recovery_from_wal(tmp_path):
    """Simulate a crash: mutate, never call close(), reopen — WAL must replay."""
    d = str(tmp_path / "db")
    db = TinyKV(d, checkpoint_every=10_000)  # keep everything in the WAL
    for i in range(300):
        db.set(f"key{i:03d}", f"value{i}")
    db.delete("key100")
    # no db.close() — pretend the process was killed here.
    db.pager._f.close()
    db.wal._f.close()

    with TinyKV(d) as db2:
        assert len(db2) == 299
        assert db2.get("key000") == "value0"
        assert db2.get("key299") == "value299"
        assert db2.get("key100") is None


def test_checkpoint_truncates_wal(tmp_path):
    import os

    d = str(tmp_path / "db")
    with TinyKV(d, checkpoint_every=50) as db:
        for i in range(200):
            db.set(f"k{i}", "v" * 20)
        wal_size = os.path.getsize(os.path.join(d, "data.wal"))
        assert wal_size < 50 * 40  # far smaller than all 200 records
        assert db.pager.meta.checkpoint_lsn > 0


def test_model_check_with_reopens(tmp_path):
    d = str(tmp_path / "db")
    model: dict[str, str] = {}
    rng = random.Random(99)
    for epoch in range(6):
        db = TinyKV(d, checkpoint_every=37)
        for _ in range(400):
            k = f"k{rng.randint(0, 200):03d}"
            if rng.random() < 0.65:
                v = str(rng.randint(0, 10**6))
                db.set(k, v)
                model[k] = v
            else:
                db.delete(k)
                model.pop(k, None)
        assert dict(db.scan()) == model
        db.close()
    with TinyKV(d) as db:
        assert dict(db.scan()) == model


def test_rejects_bad_types(tmp_path):
    with TinyKV(str(tmp_path / "db")) as db:
        with pytest.raises(TypeError):
            db.set(123, "x")
