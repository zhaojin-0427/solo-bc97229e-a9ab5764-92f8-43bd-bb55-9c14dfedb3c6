"""归档功能测试:

1. 已完成增量批次归档为 gzip NDJSON(按 seq、保留全部字段),登记与删除在线日志同事务
2. 日志仍供未完成全量回放时拒绝归档并说明;装载完成后可归档
3. 幂等:同批重复/并发归档复用同一结果
4. 写盘失败/提交前崩溃:在线日志完整保留,可重试;启动清理只删孤儿临时文件
5. 浏览器接口:归档页、候选列表、下载
"""
import gzip
import io
import json
import os
import sys
import threading

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import create_app               # noqa: E402
from app import archive, db, loader      # noqa: E402


@pytest.fixture()
def app(tmp_path):
    return create_app({
        "TESTING": True,
        "DATA_DIR": str(tmp_path / "data"),
        "SAMPLES_DIR": os.path.join(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__))), "samples"),
        "CHUNK_SIZE": 3,
        "CHUNK_DELAY": 0.0,
    })


@pytest.fixture()
def client(app):
    return app.test_client()


def csv_upload(client, rows, kind="full"):
    header = (["report_no", "item", "result", "revision"] if kind == "full"
              else ["report_no", "item", "action", "result", "revision"])
    buf = io.StringIO()
    buf.write(",".join(header) + "\n")
    for r in rows:
        buf.write(",".join(str(c) for c in r) + "\n")
    data = {"file": (io.BytesIO(buf.getvalue().encode("utf-8")), "up.csv")}
    return client.post(f"/upload/{kind}", data=data,
                       content_type="multipart/form-data")


def finish_pending(app):
    loader.run_pending(app.config["DATA_DIR"], app.config["CHUNK_SIZE"])


def online_logs(app, batch_id):
    conn = db.connect(app.config["DATA_DIR"])
    try:
        rows = conn.execute(
            "SELECT seq, batch_id, report_no, item, action, result, revision,"
            " effect, created_at FROM increments WHERE batch_id=? ORDER BY seq",
            (batch_id,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def archives_rows(app):
    conn = db.connect(app.config["DATA_DIR"])
    try:
        return conn.execute("SELECT * FROM archives ORDER BY id").fetchall()
    finally:
        conn.close()


def read_archive_file(path):
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def make_inc_batch(client, rows=("R1",)):
    """上传一个增量批次并返回批次 id。"""
    inc_rows = [(rn, "铅", "correct", "0.5", 2) for rn in rows]
    r = csv_upload(client, inc_rows, kind="inc")
    assert r.status_code == 200
    return r.get_json()["batch"]["id"]


# ---------------------------------------------------------------- 基本归档

def test_archive_exports_gzip_ndjson_and_removes_online_logs(app, client):
    csv_upload(client, [("R1", "铅", "0.1", 1), ("R2", "镉", "0.2", 1)])
    finish_pending(app)
    bid = make_inc_batch(client)
    csv_upload(client, [("R2", "镉", "withdraw", "", 1)], kind="inc")
    before = online_logs(app, bid)
    assert len(before) == 1

    r = client.post(f"/api/archive/{bid}")
    assert r.status_code == 200
    payload = r.get_json()
    assert payload["ok"] and payload["archive"]["created"] is True
    arch = payload["archive"]
    assert arch["row_count"] == 1
    assert arch["first_seq"] == before[0]["seq"] == arch["last_seq"]

    # 归档文件存在且内容 = 原日志全部字段,按 seq 升序
    path = os.path.join(app.config["DATA_DIR"], "archives",
                        f"inc_batch_{bid}.ndjson.gz")
    assert os.path.exists(path)
    assert os.path.getsize(path) == arch["file_bytes"]
    import hashlib
    with open(path, "rb") as f:
        assert hashlib.sha256(f.read()).hexdigest() == arch["sha256"]
    lines = read_archive_file(path)
    assert lines == before                      # 字段与顺序完全一致

    # 在线日志已删除;批次摘要与正式结果保留
    assert online_logs(app, bid) == []
    conn = db.connect(app.config["DATA_DIR"])
    b = conn.execute("SELECT status FROM batches WHERE id=?", (bid,)).fetchone()
    n_results = conn.execute("SELECT COUNT(*) c FROM results").fetchone()["c"]
    conn.close()
    assert b["status"] == "done" and n_results == 2


def test_archive_empty_batch_allowed(app, client):
    """全部行都是错误行的增量批次(0 条日志)也可归档,内容为空。"""
    buf = "report_no,item,action,result,revision\nR1,铅,correct,0.1,0\n"  # 修订号非法
    client.post("/upload/inc",
                data={"file": (io.BytesIO(buf.encode()), "bad.csv")},
                content_type="multipart/form-data")
    bid = client.get("/api/batches").get_json()["batches"][0]["id"]
    r = client.post(f"/api/archive/{bid}")
    assert r.status_code == 200
    arch = r.get_json()["archive"]
    assert arch["row_count"] == 0 and arch["first_seq"] is None
    path = os.path.join(app.config["DATA_DIR"], "archives",
                        f"inc_batch_{bid}.ndjson.gz")
    assert read_archive_file(path) == []


# ---------------------------------------------------------------- 回放保护

def test_archive_refused_when_logs_needed_for_replay(app, client):
    csv_upload(client, [("R1", "铅", "0.1", 1)])
    finish_pending(app)
    # 新全量开始装载(多块,先不装完)
    csv_upload(client, [(f"N{i}", "铅", "1", 1) for i in range(9)])
    loader.run_pending(app.config["DATA_DIR"], 3, max_chunks=1)
    # 装载期间确认增量 -> seq 大于该装载的 start_seq
    bid = make_inc_batch(client)

    r = client.post(f"/api/archive/{bid}")
    assert r.status_code == 409
    msg = r.get_json()["error"]
    assert "回放" in msg and "拒绝归档" in msg
    assert len(online_logs(app, bid)) == 1      # 在线日志未被删除
    assert archives_rows(app) == []             # 无登记

    # 全量装载完成(切换已回放该批日志)后允许归档
    finish_pending(app)
    r = client.post(f"/api/archive/{bid}")
    assert r.status_code == 200
    assert online_logs(app, bid) == []


def test_archive_allowed_when_load_started_after_increment(app, client):
    """装载开始前确认的增量(应已含于全量导出)不参与回放,可正常归档。"""
    csv_upload(client, [("R1", "铅", "0.1", 1)])
    finish_pending(app)
    bid = make_inc_batch(client)                # 先确认增量
    csv_upload(client, [(f"N{i}", "铅", "1", 1) for i in range(9)])  # 后开始装载
    loader.run_pending(app.config["DATA_DIR"], 3, max_chunks=1)
    r = client.post(f"/api/archive/{bid}")
    assert r.status_code == 200
    finish_pending(app)                         # 回放不受影响
    assert client.get("/api/results").get_json()["count"] == 9


def test_archive_rejects_invalid_targets(app, client):
    csv_upload(client, [("R1", "铅", "0.1", 1)])
    finish_pending(app)
    full_bid = client.get("/api/batches").get_json()["batches"][0]["id"]
    r = client.post(f"/api/archive/{full_bid}")
    assert r.status_code == 409 and "全量批次" in r.get_json()["error"]
    r = client.post("/api/archive/9999")
    assert r.status_code == 409 and "不存在" in r.get_json()["error"]
    # 未完成的增量批次
    conn = db.connect(app.config["DATA_DIR"])
    conn.execute("INSERT INTO batches (kind, filename, orig_name, status)"
                 " VALUES ('inc', '/nonexistent.csv', 'x.csv', 'processing')")
    pending_bid = conn.execute("SELECT MAX(id) i FROM batches").fetchone()["i"]
    conn.close()
    r = client.post(f"/api/archive/{pending_bid}")
    assert r.status_code == 409 and "已完成" in r.get_json()["error"]


# ---------------------------------------------------------------- 幂等与并发

def test_archive_idempotent_reuse(app, client):
    csv_upload(client, [("R1", "铅", "0.1", 1)])
    finish_pending(app)
    bid = make_inc_batch(client)
    r1 = client.post(f"/api/archive/{bid}").get_json()["archive"]
    r2 = client.post(f"/api/archive/{bid}").get_json()["archive"]
    assert r1["id"] == r2["id"] and r1["sha256"] == r2["sha256"]
    assert r2["created"] is False               # 复用同一结果
    assert len(archives_rows(app)) == 1


def test_concurrent_archive_same_batch_single_result(app, client):
    csv_upload(client, [("R1", "铅", "0.1", 1)])
    finish_pending(app)
    bid = make_inc_batch(client)
    data_dir = app.config["DATA_DIR"]
    results, errors = [], []

    def work():
        try:
            results.append(archive.archive_batch(data_dir, bid))
        except Exception as exc:                # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=work) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert sum(1 for _, created in results if created) == 1
    assert len({rec["id"] for rec, _ in results}) == 1
    assert len(archives_rows(app)) == 1
    assert online_logs(app, bid) == []


# ---------------------------------------------------------------- 失败与恢复

def test_write_failure_keeps_online_logs_and_retry_succeeds(app, client, monkeypatch):
    csv_upload(client, [("R1", "铅", "0.1", 1)])
    finish_pending(app)
    bid = make_inc_batch(client)

    def boom(*_a, **_k):
        raise OSError("disk full")
    monkeypatch.setattr(archive, "_write_archive_file", boom)
    with pytest.raises(OSError):
        archive.archive_batch(app.config["DATA_DIR"], bid)
    assert len(online_logs(app, bid)) == 1      # 在线日志完整
    assert archives_rows(app) == []             # 无登记
    # 无残留文件(临时文件已自清)
    assert os.listdir(os.path.join(app.config["DATA_DIR"], "archives")) == []

    monkeypatch.undo()
    rec, created = archive.archive_batch(app.config["DATA_DIR"], bid)
    assert created and rec["row_count"] == 1
    assert online_logs(app, bid) == []


def test_crash_before_commit_keeps_online_logs_and_retry_succeeds(app, client, monkeypatch):
    """文件写完后、登记事务提交前进程崩溃:日志仍在在线表,重试成功。"""
    csv_upload(client, [("R1", "铅", "0.1", 1)])
    finish_pending(app)
    bid = make_inc_batch(client)

    calls = {"n": 0}
    real_blockers = archive.replay_blockers

    def flaky(conn, batch_id):
        calls["n"] += 1
        if calls["n"] == 2:                     # 事务内复查时“断电”
            raise RuntimeError("power loss before commit")
        return real_blockers(conn, batch_id)
    monkeypatch.setattr(archive, "replay_blockers", flaky)
    with pytest.raises(RuntimeError):
        archive.archive_batch(app.config["DATA_DIR"], bid)
    assert len(online_logs(app, bid)) == 1      # 事务回滚,日志仍在
    assert archives_rows(app) == []

    monkeypatch.undo()
    rec, created = archive.archive_batch(app.config["DATA_DIR"], bid)
    assert created
    assert online_logs(app, bid) == []
    lines = read_archive_file(rec["filename"])
    assert len(lines) == 1 and lines[0]["batch_id"] == bid


def test_startup_cleanup_removes_only_orphan_temp_files(app, client, tmp_path):
    csv_upload(client, [("R1", "铅", "0.1", 1)])
    finish_pending(app)
    bid = make_inc_batch(client)
    rec, _ = archive.archive_batch(app.config["DATA_DIR"], bid)
    with open(rec["filename"], "rb") as f:
        registered_bytes = f.read()

    adir = os.path.join(app.config["DATA_DIR"], "archives")
    orphan = os.path.join(adir, "inc_batch_999.ndjson.gz.tmp-deadbeef")
    with open(orphan, "wb") as f:
        f.write(b"partial garbage")

    # 模拟进程重启:新建应用触发启动清理
    app2 = create_app({"TESTING": True, "DATA_DIR": app.config["DATA_DIR"],
                       "SAMPLES_DIR": app.config["SAMPLES_DIR"],
                       "CHUNK_SIZE": 3, "CHUNK_DELAY": 0.0})
    assert app2 is not None
    assert not os.path.exists(orphan)           # 孤儿临时文件已清理
    assert os.path.exists(rec["filename"])      # 已登记文件不动
    with open(rec["filename"], "rb") as f:
        assert f.read() == registered_bytes     # 内容未被破坏


# ---------------------------------------------------------------- 接口与页面

def test_download_endpoint_and_archives_api(app, client):
    csv_upload(client, [("R1", "铅", "0.1", 1)])
    finish_pending(app)
    bid = make_inc_batch(client)
    before = online_logs(app, bid)
    arch = client.post(f"/api/archive/{bid}").get_json()["archive"]

    listing = client.get("/api/archives").get_json()["archives"]
    assert len(listing) == 1 and listing[0]["batch_id"] == bid

    r = client.get(arch["download_url"])
    assert r.status_code == 200
    assert "gzip" in r.headers["Content-Type"]
    lines = [json.loads(x) for x in
             gzip.decompress(r.data).decode("utf-8").splitlines() if x.strip()]
    assert lines == before

    assert client.get("/api/archives/999/download").status_code == 404


def test_candidates_endpoint_and_archive_page(app, client):
    assert client.get("/archive").status_code == 200
    csv_upload(client, [("R1", "铅", "0.1", 1)])
    finish_pending(app)
    bid = make_inc_batch(client)
    cands = client.get("/api/archive/candidates").get_json()["candidates"]
    c = [c for c in cands if c["id"] == bid][0]
    assert c["archivable"] is True and c["log_rows"] == 1 and c["archive"] is None

    client.post(f"/api/archive/{bid}")
    cands = client.get("/api/archive/candidates").get_json()["candidates"]
    c = [c for c in cands if c["id"] == bid][0]
    assert c["archive"] is not None and c["archivable"] is False


def test_business_continues_during_and_after_archive(app, client):
    """归档后增量上传、全量装载与取数照常;新日志 seq 不复用。"""
    csv_upload(client, [("R1", "铅", "0.1", 1)])
    finish_pending(app)
    bid1 = make_inc_batch(client)
    client.post(f"/api/archive/{bid1}")

    r = csv_upload(client, [("R1", "铅", "correct", "0.9", 3)], kind="inc")
    assert r.status_code == 200
    d = client.get("/api/results").get_json()
    assert d["results"][0]["result"] == "0.9"
    incs = client.get("/api/increments").get_json()["increments"]
    assert len(incs) == 1                       # 在线表只剩新批次的日志
    assert incs[0]["seq"] > 1                   # 全局序号不回退

    csv_upload(client, [("R1", "铅", "0.2", 4), ("R3", "汞", "0.3", 1)])
    finish_pending(app)
    assert client.get("/api/results").get_json()["count"] == 2
