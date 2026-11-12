"""增量批次归档:把已完成增量批次的已确认日志导出为 gzip NDJSON 归档文件,
登记归档并在同一事务内删除该批在线日志,为更正/撤回留存审计底稿。

一致性设计:
  - 先写文件后登记:日志行读出后在**不持有任何数据库事务**的情况下写入
    临时文件,fsync 后原子改名为正式归档文件,再对归档目录 fsync;
    写文件期间增量上传、全量装载与取数接口完全可用。
  - 登记与删除同事务:归档文件完整持久化后,INSERT archives +
    DELETE increments 在单个 BEGIN IMMEDIATE 事务内提交,崩溃则整体回滚
    (在线日志仍在),不会出现“日志没了但归档不可用”的窗口。
  - 回放保护:若某未完成全量装载(loading/staging_ready)的 start_seq 小于
    本批日志的最大 seq,则该批日志仍需供切换回放,拒绝归档;该检查在
    提交事务内复查,与全量批次创建(同样 BEGIN IMMEDIATE)串行化,无竞态。
  - 幂等:archives.batch_id 唯一约束 + 进程内每批互斥锁,同批重复或并发
    归档复用同一登记记录与文件。
  - 崩溃恢复:进程中断后,完整日志要么仍在在线表(登记未提交),要么在
    可下载归档中(登记已提交);启动时仅清理 *.tmp-* 孤儿临时文件,
    已登记的 .ndjson.gz 文件永不删除;未完成的归档可直接重试。
"""
import gzip
import hashlib
import json
import os
import sqlite3
import threading
import uuid

from . import db

ARCHIVE_DIR_NAME = "archives"
TMP_MARK = ".tmp-"          # 临时文件标识:启动清理只删带此标记的文件

INC_LOG_COLUMNS = ("seq", "batch_id", "report_no", "item", "action",
                   "result", "revision", "effect", "created_at")

_batch_locks_guard = threading.Lock()
_batch_locks = {}           # batch_id -> threading.Lock(进程内每批互斥)


class ArchiveRefused(Exception):
    """归档被业务规则拒绝(批次状态不符、日志仍供全量回放等)。"""


def archives_dir(data_dir):
    path = os.path.join(data_dir, ARCHIVE_DIR_NAME)
    os.makedirs(path, exist_ok=True)
    return path


def final_path(data_dir, batch_id):
    """正式归档文件名:按批次确定,重试/并发写出的内容一致。"""
    return os.path.join(archives_dir(data_dir),
                        f"inc_batch_{batch_id}.ndjson.gz")


def get_archive(conn, batch_id):
    return conn.execute(
        "SELECT * FROM archives WHERE batch_id=?", (batch_id,)).fetchone()


def replay_blockers(conn, batch_id):
    """仍需本批日志做切换回放的未完成全量批次列表(空列表表示可归档)。

    切换回放 increments 中 seq > start_seq 的全部日志;只要本批最大 seq
    大于某未完成装载的 start_seq,删除在线日志就会丢回放数据。
    """
    return conn.execute(
        """
        SELECT b.id, b.status, b.start_seq
          FROM batches b
         WHERE b.kind = 'full' AND b.status IN ('loading', 'staging_ready')
           AND b.start_seq < COALESCE(
                   (SELECT MAX(i.seq) FROM increments i WHERE i.batch_id = ?), 0)
         ORDER BY b.id
        """,
        (batch_id,),
    ).fetchall()


def _blocker_message(batch_id, blockers):
    parts = ", ".join(
        f"全量批次 #{b['id']}({b['status']},回放水位 start_seq={b['start_seq']})"
        for b in blockers)
    return (f"批次 #{batch_id} 的增量日志仍需供未完成全量装载回放,拒绝归档: {parts}。"
            "请等待其切换完成后再归档。")


def _batch_lock(batch_id):
    with _batch_locks_guard:
        lock = _batch_locks.get(batch_id)
        if lock is None:
            lock = threading.Lock()
            _batch_locks[batch_id] = lock
        return lock


class _HashingWriter:
    """包装二进制文件对象:写出同时计算 SHA-256(对压缩后字节,即文件内容)。"""

    def __init__(self, raw):
        self._raw = raw
        self.sha256 = hashlib.sha256()

    def write(self, data):
        self.sha256.update(data)
        return self._raw.write(data)

    def flush(self):
        self._raw.flush()


def _write_archive_file(data_dir, batch_id, rows):
    """把日志行按 seq 序写成 gzip NDJSON 并持久化。全程不持有数据库事务。

    临时文件 -> fsync -> os.replace 原子改名 -> 目录 fsync。
    gzip 头 mtime=0:同批重写内容字节一致(幂等/并发安全)。
    返回 (最终路径, 字节数, sha256)。
    """
    adir = archives_dir(data_dir)
    tmp = os.path.join(
        adir, f"inc_batch_{batch_id}.ndjson.gz{TMP_MARK}{uuid.uuid4().hex}")
    sink = None
    try:
        raw = open(tmp, "wb")
        sink = _HashingWriter(raw)
        with gzip.GzipFile(filename="", mode="wb", fileobj=sink,
                           compresslevel=9, mtime=0) as gz:
            for row in rows:
                rec = {col: row[col] for col in INC_LOG_COLUMNS}
                line = json.dumps(rec, ensure_ascii=False,
                                  separators=(",", ":")) + "\n"
                gz.write(line.encode("utf-8"))
        raw.flush()
        os.fsync(raw.fileno())          # 文件内容落盘
        raw.close()
        final = final_path(data_dir, batch_id)
        os.replace(tmp, final)          # 原子改名:读者只见完整文件
        dir_fd = os.open(adir, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(dir_fd)            # 改名本身落盘
        finally:
            os.close(dir_fd)
        return final, os.path.getsize(final), sink.sha256.hexdigest()
    except BaseException:
        if sink is not None and not sink._raw.closed:
            sink._raw.close()
        try:
            os.unlink(tmp)              # 失败的临时文件不残留
        except OSError:
            pass
        raise


def _fetch_batch_logs(conn, batch_id):
    """只读快照查询(WAL 下不阻塞写者),不开启写事务。"""
    return conn.execute(
        "SELECT seq, batch_id, report_no, item, action, result, revision,"
        " effect, created_at FROM increments WHERE batch_id = ? ORDER BY seq",
        (batch_id,),
    ).fetchall()


def archive_batch(data_dir, batch_id):
    """归档一个已完成增量批次。返回 (归档记录 sqlite3.Row, 是否本次新建)。

    幂等:该批已归档时直接返回既有登记(复用同一文件与记录)。
    拒绝时抛出 ArchiveRefused;其他异常(如写盘失败)原样抛出,
    此时在线日志保持完整,可直接重试。
    """
    with _batch_lock(batch_id):         # 进程内同批互斥;跨进程由唯一索引兜底
        conn = db.connect(data_dir)
        try:
            batch = conn.execute(
                "SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
            if batch is None:
                raise ArchiveRefused(f"批次 #{batch_id} 不存在")
            existing = get_archive(conn, batch_id)
            if existing is not None:
                return existing, False              # 重复归档:复用同一结果
            if batch["kind"] != "inc":
                raise ArchiveRefused(
                    f"批次 #{batch_id} 是全量批次,只有增量批次可以归档")
            if batch["status"] != "done":
                raise ArchiveRefused(
                    f"批次 #{batch_id} 状态为 {batch['status']},"
                    "只有已完成(status=done)的增量批次可以归档")
            blockers = replay_blockers(conn, batch_id)
            if blockers:
                raise ArchiveRefused(_blocker_message(batch_id, blockers))

            rows = _fetch_batch_logs(conn, batch_id)

            # —— 阶段 1:写归档文件(不持有数据库事务,业务读写不受影响)——
            path, size, digest = _write_archive_file(data_dir, batch_id, rows)

            # —— 阶段 2:登记归档 + 删除在线日志,单事务提交 ——
            conn.execute("BEGIN IMMEDIATE")
            try:
                # 事务内复查:写文件期间可能新启动了需要回放本批日志的全量装载
                blockers = replay_blockers(conn, batch_id)
                if blockers:
                    raise ArchiveRefused(_blocker_message(batch_id, blockers))
                conn.execute(
                    "INSERT INTO archives (batch_id, filename, file_bytes, sha256,"
                    " row_count, first_seq, last_seq)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (batch_id, path, size, digest, len(rows),
                     rows[0]["seq"] if rows else None,
                     rows[-1]["seq"] if rows else None))
                conn.execute(
                    "DELETE FROM increments WHERE batch_id=?", (batch_id,))
                conn.execute("COMMIT")
            except ArchiveRefused:
                conn.execute("ROLLBACK")            # 在线日志保留,可稍后重试
                raise
            except sqlite3.IntegrityError:
                conn.execute("ROLLBACK")
                # 并发归档已抢先登记(唯一约束):复用其结果
                existing = get_archive(conn, batch_id)
                if existing is not None:
                    return existing, False
                raise
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            return get_archive(conn, batch_id), True
        finally:
            conn.close()


def list_archives(conn):
    return conn.execute(
        "SELECT * FROM archives ORDER BY id DESC LIMIT 200").fetchall()


def cleanup_orphan_temp_files(data_dir):
    """进程启动时清理归档目录中的孤儿临时文件(上次写盘中断的残留)。

    只删除带临时标记的文件;已登记的正式归档文件绝不删除。
    返回清理的文件数。
    """
    removed = 0
    try:
        names = os.listdir(archives_dir(data_dir))
    except OSError:
        return 0
    for name in names:
        if TMP_MARK in name:
            try:
                os.unlink(os.path.join(data_dir, ARCHIVE_DIR_NAME, name))
                removed += 1
            except OSError:
                pass
    return removed
