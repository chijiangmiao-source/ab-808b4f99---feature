"""Compose verify 容器入口：

1. 代码测试：unittest 全量（含真实子进程的崩溃前滚/回滚恢复）；
2. 构建检查：全量字节码编译；
3. API/HTTP 冒烟：对 app 服务复现锁升级死锁、FIFO 队列推进、
   幂等/非法事件拒绝，以及撤销持久化阶段崩溃后的重启恢复；
4. 等待历程：升级等待后获授、死锁撤销终局、重启后历程查询与拒绝语义。

任一阶段失败即以非零退出码报告验收结果。
"""

from __future__ import annotations

import json
import os
import py_compile
import subprocess
import sys
import time
import urllib.error
import urllib.request

APP_URL = os.environ.get("APP_URL", "http://app:8080")
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def section(title: str) -> None:
    print("\n" + "=" * 68)
    print(f"  {title}")
    print("=" * 68, flush=True)


def http(method: str, path: str, body: dict | None = None,
         timeout: int = 5):
    url = f"{APP_URL}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url, data=data,
        headers={"Content-Type": "application/json"}, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())


def wait_health(timeout: float = 30.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status, body = http("GET", "/health")
            if status == 200 and body.get("status") == "ok":
                return True
        except Exception:  # noqa: BLE001
            time.sleep(0.5)
    return False


def check(cond: bool, msg: str) -> None:
    print(f"  [{'PASS' if cond else 'FAIL'}] {msg}")
    if not cond:
        raise AssertionError(msg)


def stage_unit_tests() -> None:
    section("阶段 1/5：代码测试（unittest 全量，含子进程崩溃恢复）")
    proc = subprocess.run(
        [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-v"],
        cwd=REPO_ROOT)
    check(proc.returncode == 0, "全部单元/集成测试通过")
    if proc.returncode != 0:
        raise SystemExit(1)


def stage_build_check() -> None:
    section("阶段 2/5：构建检查（字节码编译）")
    for rel in ("app", "tests", "scripts"):
        base = os.path.join(REPO_ROOT, rel)
        for name in sorted(os.listdir(base)):
            if name.endswith(".py"):
                py_compile.compile(os.path.join(base, name), doraise=True)
    check(True, "app/tests/scripts 全部 Python 文件编译通过")


def stage_deadlock_and_recovery() -> None:
    section("阶段 3/5：API 冒烟 —— 锁升级死锁、撤销与重启恢复")
    check(wait_health(), "app 服务健康检查通过")
    sid = f"accept-dl-{int(time.time())}"

    def ev(eid, tid, action, res=None, mode=None, crash=False, expect=201):
        payload = {"event_id": eid, "tid": tid, "action": action}
        if res:
            payload["resource"] = res
        if mode:
            payload["mode"] = mode
        if crash:
            payload["crash"] = True
        try:
            status, body = http("POST", f"/sessions/{sid}/events", payload)
        except (urllib.error.URLError, ConnectionError):
            # 预期：崩溃注入导致连接中断
            check(crash, "闭合环事件触发进程中断（连接被重置）")
            return None
        check(status == expect, f"事件 {eid} 返回 {status}（预期 {expect}）: {body}")
        return body

    setup = [
        ("b1", "T1", "begin", None, None),
        ("b2", "T2", "begin", None, None),
        ("s1", "T1", "request", "CH-A", "S"),
        ("s2", "T2", "request", "CH-B", "S"),
        ("x1", "T1", "request", "CH-B", "S"),
        ("x2", "T2", "request", "CH-A", "S"),
        ("u1", "T1", "upgrade", "CH-B", None),
    ]
    for args in setup:
        ev(*args)

    # 闭合环并要求在撤销持久化阶段中断；容器靠 restart 策略自动重启
    ev("u2", "T2", "upgrade", "CH-A", None, crash=True)
    print("  .. 服务在撤销检查点落盘后中断，等待自动重启并前滚恢复 ..", flush=True)
    check(wait_health(60), "服务崩溃后自动重启并恢复健康")

    status, snap = http("GET", f"/sessions/{sid}")
    check(status == 200, "恢复后可查询会话状态")
    check(snap["verdict_seq"] == 8, f"裁决序号为 8（实际 {snap['verdict_seq']}）")
    check(snap["aborted_transactions"] == ["T2"],
          "开始序号最大的事务 T2 被撤销")
    check(snap["locks"].get("CH-B") == {"mode": "X", "holders": ["T1"]},
          "T1 原子取得 CH-B 独占锁")
    check(snap["locks"].get("CH-A") == {"mode": "S", "holders": ["T1"]},
          "T1 仍持有 CH-A 共享锁（被撤销者的锁已清除）")
    check(not snap["waiting_queues"], "T2 的全部等待已原子清除")

    status, verdict = http("GET", f"/sessions/{sid}/events/u2")
    check(status == 200 and verdict["seq"] == 8,
          "可凭稳定事件标识查询首次裁决（seq=8）")
    check([a["tid"] for a in verdict["aborted"]] == ["T2"],
          "裁决记录中撤销事务为 T2")

    # 重启后的等待历程查询：死锁撤销终局与当前锁状态一致
    status, h2 = http("GET", f"/sessions/{sid}/transactions/T2/wait-history")
    check(status == 200 and len(h2["history"]) == 1,
          "重启后可按事务读取被撤销者的等待历程")
    rec = h2["history"][0]
    check(rec["event_id"] == "u2" and rec["resource"] == "CH-A"
          and rec["mode"] == "X" and rec["upgrade"] is True,
          "历程记录触发事件、资源与目标模式（u2 升级 CH-A 至 X）")
    check(rec["position"] == 1 and rec["blocked_by"]["holders"] == ["T1"],
          "历程记录当时队列位置与直接阻塞的持锁事务 T1")
    check(rec["outcome"] == "aborted" and rec["outcome_seq"] == 8,
          "死锁裁决为被撤销事务补齐终局 aborted（裁决序号 8）")
    status, h1 = http("GET", f"/sessions/{sid}/transactions/T1/wait-history")
    rec1 = h1["history"][0]
    check(status == 200 and rec1["event_id"] == "u1"
          and rec1["outcome"] == "granted" and rec1["outcome_seq"] == 8,
          "幸存者 T1 的升级等待历程终局为 granted（裁决序号 8）")

    body = ev("c1", "T1", "commit")
    check(body is not None and body["state"]["locks"] == {},
          "恢复后可继续处理合法事件（T1 commit 后锁清空）")

    status, body = http("POST", f"/sessions/{sid}/events",
                        {"event_id": "z1", "tid": "T2", "action": "request",
                         "resource": "CH-C", "mode": "S"})
    check(status == 422 and body["error"] == "transaction_aborted",
          "已撤销事务继续操作被拒绝（422）")


def stage_fifo_and_idempotency() -> None:
    section("阶段 4/5：API 冒烟 —— FIFO 队列推进与稳定标识语义")
    sid = f"accept-fifo-{int(time.time())}"

    def ev(eid, tid, action, res=None, mode=None, expect=201):
        payload = {"event_id": eid, "tid": tid, "action": action}
        if res:
            payload["resource"] = res
        if mode:
            payload["mode"] = mode
        status, body = http("POST", f"/sessions/{sid}/events", payload)
        check(status == expect, f"事件 {eid} -> {status}（预期 {expect}）: {body}")
        return status, body

    ev("b1", "T1", "begin")
    ev("b2", "T2", "begin")
    ev("b3", "T3", "begin")
    ev("r1", "T1", "request", "CH", "S")
    ev("r2", "T2", "request", "CH", "X")
    ev("r3", "T3", "request", "CH", "S")
    _, snap = http("GET", f"/sessions/{sid}")
    check([e["tid"] for e in snap["waiting_queues"]["CH"]] == ["T2", "T3"],
          "后到的兼容 S 请求没有越过不兼容的队首 T2(X)")

    status, body = http("POST", f"/sessions/{sid}/events",
                        {"event_id": "bad", "tid": "T3",
                         "action": "release", "resource": "CH"})
    check(status == 422 and body["error"] == "illegal_release",
          "等待中非法释放被拒绝（422）")

    seq_before = http("GET", f"/sessions/{sid}")[1]["verdict_seq"]
    status, body = http("POST", f"/sessions/{sid}/events",
                        {"event_id": "dup-begin", "tid": "T1", "action": "begin"})
    check(status == 422 and body["error"] == "duplicate_transaction",
          "重复 begin 被拒绝（422）")
    seq_after = http("GET", f"/sessions/{sid}")[1]["verdict_seq"]
    check(seq_before == seq_after, "被拒绝事件不消耗裁决序号、不改状态")

    status, body = http("POST", f"/sessions/{sid}/events",
                        {"event_id": "r1", "tid": "T1", "action": "request",
                         "resource": "CH", "mode": "S"})
    check(status == 200 and body.get("replayed") is True and body["seq"] == 4,
          "相同稳定标识重放返回首次裁决（replayed=true）")

    status, body = http("POST", f"/sessions/{sid}/events",
                        {"event_id": "r1", "tid": "T1", "action": "request",
                         "resource": "CH", "mode": "X"})
    check(status == 409 and body["error"] == "event_id_content_conflict",
          "标识相同而内容不同被拒绝（409）且不改状态")

    ev("rel", "T1", "release", "CH")
    _, snap = http("GET", f"/sessions/{sid}")
    check(snap["locks"].get("CH") == {"mode": "X", "holders": ["T2"]},
          "队首推进：T2 取得 X，T3 仍在等待")
    ev("c2", "T2", "commit")
    _, snap = http("GET", f"/sessions/{sid}")
    check(snap["locks"].get("CH") == {"mode": "S", "holders": ["T3"]},
          "继续推进：T3 取得 S")
    check(not snap["waiting_queues"], "等待队列清空")


def stage_wait_history() -> None:
    section("阶段 5/5：等待历程 —— 升级等待后获授、终局唯一与拒绝语义")
    sid = f"accept-hist-{int(time.time())}"

    def ev(eid, tid, action, res=None, mode=None, expect=201):
        payload = {"event_id": eid, "tid": tid, "action": action}
        if res:
            payload["resource"] = res
        if mode:
            payload["mode"] = mode
        status, body = http("POST", f"/sessions/{sid}/events", payload)
        check(status == expect, f"事件 {eid} -> {status}（预期 {expect}）: {body}")
        return status, body

    def history(tid, expect=200):
        status, body = http("GET", f"/sessions/{sid}/transactions/{tid}/wait-history")
        check(status == expect,
              f"查询 {tid} 等待历程 -> {status}（预期 {expect}）: {body}")
        return status, body

    ev("b1", "T1", "begin")
    ev("b2", "T2", "begin")
    ev("s1", "T1", "request", "CH-U", "S")
    ev("s2", "T2", "request", "CH-U", "S")
    _, body = ev("u1", "T2", "upgrade", "CH-U")
    check(bool(body["enqueued"]), "存在其他 S 持有者时升级进入等待")

    # 等待中：历程已建档，记录触发事件/资源/目标模式/位置/直接阻塞者，终局未补齐
    _, h = history("T2")
    check(len(h["history"]) == 1, "等待中的事务已有一条历程记录")
    rec = h["history"][0]
    check(rec["event_id"] == "u1" and rec["resource"] == "CH-U"
          and rec["mode"] == "X" and rec["upgrade"] is True,
          "历程记录触发事件 u1、资源 CH-U、目标模式 X（升级）")
    check(rec["position"] == 1 and rec["enq_seq"] == 5
          and rec["blocked_by"]["holders"] == ["T1"]
          and rec["blocked_by"]["waiters"] == [],
          "历程记录当时队列位置 1 与直接阻塞的持锁事务 T1")
    check(rec["outcome"] is None and rec["outcome_seq"] is None,
          "等待中的历程终局未补齐")
    _, snap = http("GET", f"/sessions/{sid}")
    check([e["tid"] for e in snap["waiting_queues"]["CH-U"]] == ["T2"],
          "打开中的历程与当前等待队列一致（T2 仍在队首）")

    # 重放同一稳定事件：不新增、不改写历程
    ev("u1", "T2", "upgrade", "CH-U", expect=200)
    _, h = history("T2")
    check(len(h["history"]) == 1 and h["history"][0]["outcome"] is None,
          "重复事件重放不新增、不改写历程")

    # T1 释放 -> 队列推进，升级获授，终局只补齐一次
    ev("r1", "T1", "release", "CH-U")
    _, h = history("T2")
    rec = h["history"][0]
    check(rec["outcome"] == "granted" and rec["outcome_seq"] == 6,
          "升级等待后获授：终局 granted，裁决序号为释放事件的 6")
    _, snap = http("GET", f"/sessions/{sid}")
    check(snap["locks"].get("CH-U") == {"mode": "X", "holders": ["T2"]},
          "获授后锁状态与历程终局一致（T2 独占 CH-U）")
    ev("c2", "T2", "commit")
    _, h = history("T2")
    check(len(h["history"]) == 1 and h["history"][0]["outcome"] == "granted"
          and h["history"][0]["outcome_seq"] == 6,
          "后续事件不再改写已补齐的终局")

    # 明确拒绝：尚未产生等待的事务 / 不存在（含跨会话）的事务 / 不存在的会话
    status, body = history("T1", expect=404)
    check(body["error"] == "wait_history_not_found",
          "尚未产生等待的事务被明确拒绝（404 wait_history_not_found）")
    status, body = history("T9", expect=404)
    check(body["error"] == "transaction_not_found",
          "不存在或跨会话的事务被明确拒绝（404 transaction_not_found）")
    status, body = http("GET", "/sessions/hist-nope/transactions/T2/wait-history")
    check(status == 404 and body["error"] == "session_not_found",
          "不存在的会话被明确拒绝（404 session_not_found）")


def main() -> int:
    try:
        stage_unit_tests()
        stage_build_check()
        stage_deadlock_and_recovery()
        stage_fifo_and_idempotency()
        stage_wait_history()
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001
        print(f"\n验收失败: {exc!r}", flush=True)
        return 1
    section("验收结论：全部通过 —— 死锁撤销、队列推进、幂等拒绝、重启恢复与等待历程均可观察")
    return 0


if __name__ == "__main__":
    sys.exit(main())
