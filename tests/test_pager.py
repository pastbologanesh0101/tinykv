from tinykv.pager import Pager


def test_alloc_write_read_roundtrip(tmp_path):
    p = Pager(str(tmp_path / "x.kv"))
    a = p.allocate_page()
    b = p.allocate_page()
    assert a != b
    p.write_page(a, b"hello")
    p.write_page(b, b"world")
    assert p.read_page(a).rstrip(b"\x00") == b"hello"
    assert p.read_page(b).rstrip(b"\x00") == b"world"
    p.close()


def test_free_list_reuse(tmp_path):
    p = Pager(str(tmp_path / "x.kv"))
    a = p.allocate_page()
    b = p.allocate_page()
    p.free_page(a)
    c = p.allocate_page()
    assert c == a  # most recently freed page comes back first
    assert p.meta.page_count == 3  # b + reused a, no growth
    p.close()


def test_meta_persists_across_reopen(tmp_path):
    path = str(tmp_path / "x.kv")
    p = Pager(path)
    pid = p.allocate_page()
    p.meta.root_pid = pid
    p.meta.checkpoint_lsn = 42
    p.close()

    p2 = Pager(path)
    assert p2.meta.root_pid == pid
    assert p2.meta.checkpoint_lsn == 42
    assert p2.meta.page_count == 2
    p2.close()
