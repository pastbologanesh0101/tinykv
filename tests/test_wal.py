from tinykv.wal import DELETE, PUT, WAL


def test_append_and_replay(tmp_path):
    w = WAL(str(tmp_path / "a.wal"))
    w.append(PUT, b"k1", b"v1")
    w.append(PUT, b"k2", b"v2")
    w.append(DELETE, b"k1", b"")
    w.sync()
    w.close()

    w2 = WAL(str(tmp_path / "a.wal"))
    recs = list(w2.replay(0))
    assert [(r.op, r.key, r.value) for r in recs] == [
        (PUT, b"k1", b"v1"),
        (PUT, b"k2", b"v2"),
        (DELETE, b"k1", b""),
    ]
    assert w2.last_lsn == 3


def test_replay_after_lsn(tmp_path):
    w = WAL(str(tmp_path / "b.wal"))
    for i in range(10):
        w.append(PUT, f"k{i}".encode(), b"v")
    w.sync()
    recs = list(w.replay(7))
    assert [r.lsn for r in recs] == [8, 9, 10]


def test_torn_tail_is_truncated(tmp_path):
    path = str(tmp_path / "c.wal")
    w = WAL(path)
    w.append(PUT, b"good", b"1")
    w.append(PUT, b"alsogood", b"2")
    w.sync()
    w.close()

    # Simulate a crash mid-write: append garbage bytes.
    with open(path, "ab") as f:
        f.write(b"\x00\x01\x02\x03 half a record")

    w2 = WAL(path)
    recs = list(w2.replay(0))
    assert [r.key for r in recs] == [b"good", b"alsogood"]
    # The torn bytes are gone, so a fresh append lands cleanly.
    w2.append(PUT, b"after", b"3")
    w2.sync()
    w2.close()
    assert [r.key for r in WAL(path).replay(0)] == [b"good", b"alsogood", b"after"]


def test_bitflip_stops_replay(tmp_path):
    path = str(tmp_path / "d.wal")
    w = WAL(path)
    w.append(PUT, b"a", b"1")
    w.append(PUT, b"b", b"2")
    w.append(PUT, b"c", b"3")
    w.sync()
    w.close()

    data = bytearray(open(path, "rb").read())
    data[len(data) // 2] ^= 0xFF  # corrupt somewhere in the middle record
    open(path, "wb").write(data)

    recs = list(WAL(path).replay(0))
    assert [r.key for r in recs] == [b"a"]  # replay stops at first bad CRC


def test_truncate_keeps_lsn_monotonic(tmp_path):
    path = str(tmp_path / "e.wal")
    w = WAL(path)
    w.append(PUT, b"a", b"1")
    w.append(PUT, b"b", b"2")
    assert w.last_lsn == 2
    w.truncate()
    assert w.last_lsn == 2
    lsn = w.append(PUT, b"c", b"3")
    assert lsn == 3
