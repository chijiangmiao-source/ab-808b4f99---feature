"""Compose verify 容器入口：

1. 代码测试：unittest 全量（含真实子进程的崩溃前滚/回滚恢复）；
2. 构建检查：全量字节码编译；
3. API/HTTP 冒烟：对 app 服务复现锁升级死锁、FIFO 队列推进、
   幂等/非法事件拒绝、等待历程（为何排队、终局唯一、重启一致），
   以及撤销持久化阶段崩溃后的重启恢复。

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

    # 等待历程（重启后查询）：升级等待后获授、死锁撤销，均与锁状态一致
    status, wh1 = http("GET", f"/sessions/{sid}/transactions/T1/waits")
    check(status == 200 and len(wh1["waits"]) == 1, "重启后 T1 的等待历程可读")
    rec = wh1["waits"][0]
    check(rec["event_id"] == "u1" and rec["upgrade"] is True
          and rec["resource"] == "CH-B" and rec["mode"] == "X",
          "历程记录触发事件 u1、资源 CH-B 与目标模式 X（升级）")
    check(rec["position"] == 1
          and [h["tid"] for h in rec["blocked_by"]["holders"]] == ["T2"],
          "历程记录当时队首位置与直接阻塞者 T2")
    check(rec["outcome"] == {"kind": "granted", "seq": 8},
          "升级等待后获授：终局 granted@8，与 T1 持有 CH-B 独占锁一致")
    status, wh2 = http("GET", f"/sessions/{sid}/transactions/T2/waits")
    check(status == 200
          and wh2["waits"][0]["event_id"] == "u2"
          and wh2["waits"][0]["outcome"] == {"kind": "aborted", "seq": 8},
          "死锁撤销：T2 的历程终局 aborted@8，与撤销标记一致")

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

    # 等待历程：T2/T3 的等待分别由释放与提交推进获授，终局只补齐一次
    _, wh = http("GET", f"/sessions/{sid}/transactions/T2/waits")
    check(wh["waits"][0]["outcome"] == {"kind": "granted", "seq": 7},
          "T2 的等待在 T1 释放后获授（granted@7）")
    _, wh = http("GET", f"/sessions/{sid}/transactions/T3/waits")
    check(wh["waits"][0]["outcome"] == {"kind": "granted", "seq": 8}
          and [w["tid"] for w in wh["waits"][0]["blocked_by"]["waiters"]] == ["T2"],
          "T3 的等待在 T2 提交后获授（granted@8），在先等待项为 T2")
    status, body = http("GET", f"/sessions/{sid}/transactions/T1/waits")
    check(status == 404 and body["error"] == "wait_history_not_found",
          "从未等待的 T1 查询历程被明确拒绝（404）")


def stage_wait_history() -> None:
    section("阶段 5/5：API 冒烟 —— 等待历程（为何排队、终局唯一与重启一致）")
    sid = f"accept-waits-{int(time.time())}"
    sid_other = f"accept-waits-other-{int(time.time())}"

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

    ev("b1", "T1", "begin")
    ev("b2", "T2", "begin")
    ev("b3", "T3", "begin")
    ev("b4", "T4", "begin")
    ev("r1", "T1", "request", "R", "X")
    ev("r2", "T2", "request", "R", "X")   # T2 排队等待 T1
    ev("q1", "T3", "request", "Q", "X")
    ev("q2", "T1", "request", "Q", "X")   # T1 排队等待 T3
    ev("z1", "T4", "request", "Z", "S")   # T4 立即获授，从不等待

    # 等待中的历程立即可读：触发事件、资源、目标模式、位置与直接阻塞者
    status, wh = http("GET", f"/sessions/{sid}/transactions/T2/waits")
    check(status == 200 and len(wh["waits"]) == 1, "T2 的等待历程在等待期间即可读")
    rec = wh["waits"][0]
    check(rec["event_id"] == "r2" and rec["resource"] == "R"
          and rec["mode"] == "X" and rec["upgrade"] is False,
          "历程记录触发事件、资源与目标模式")
    check(rec["position"] == 1 and rec["outcome"] is None,
          "历程记录当时队首位置，且仍在等待（outcome 为空）")
    check([h["tid"] for h in rec["blocked_by"]["holders"]] == ["T1"],
          "直接阻塞者为持锁事务 T1")

    # 重复事件重放不得新增或改写历程
    ev("r2", "T2", "request", "R", "X", expect=200)
    status, wh = http("GET", f"/sessions/{sid}/transactions/T2/waits")
    check(len(wh["waits"]) == 1 and wh["waits"][0]["outcome"] is None,
          "重放后历程不新增、不改写")

    # 明确拒绝：不存在的事务 / 尚未产生等待的事务 / 跨会话查询
    status, body = http("GET", f"/sessions/{sid}/transactions/T9/waits")
    check(status == 404 and body["error"] == "transaction_not_found",
          "不存在的事务查询历程被明确拒绝（404）")
    status, body = http("GET", f"/sessions/{sid}/transactions/T4/waits")
    check(status == 404 and body["error"] == "wait_history_not_found",
          "尚未产生等待的事务查询历程被明确拒绝（404）")
    status, body = http("POST", f"/sessions/{sid_other}/events",
                        {"event_id": "b1", "tid": "T9", "action": "begin"})
    check(status == 201, "另一会话中 T9 已 begin")
    status, body = http("GET", f"/sessions/{sid}/transactions/T9/waits")
    check(status == 404 and body["error"] == "transaction_not_found",
          "跨会话查询被明确拒绝（他会话事务在本会话不可读）")

    # 闭合环 T1<->T3 并在撤销持久化阶段崩溃：T3 被撤销，T1 获授，T2 继续等待
    ev("q3", "T3", "request", "R", "X", crash=True)
    print("  .. 服务在撤销检查点落盘后中断，等待自动重启并前滚恢复 ..", flush=True)
    check(wait_health(60), "服务崩溃后自动重启并恢复健康")

    status, snap = http("GET", f"/sessions/{sid}")
    check(snap["verdict_seq"] == 10, f"裁决序号为 10（实际 {snap['verdict_seq']}）")
    check(snap["aborted_transactions"] == ["T3"],
          "环上 begin 序号最大的 T3 被撤销")
    check(snap["locks"].get("Q") == {"mode": "X", "holders": ["T1"]},
          "T1 取得 Q 独占锁")
    check([e["tid"] for e in snap["waiting_queues"].get("R", [])] == ["T2"],
          "T2 仍在 R 的等待队列中")

    # 重启后：已结束与仍在等待的历程都必须与当前锁状态一致
    _, wh1 = http("GET", f"/sessions/{sid}/transactions/T1/waits")
    check(wh1["waits"][0]["outcome"] == {"kind": "granted", "seq": 10},
          "重启后 T1 的历程终局为 granted@10，与其持有 Q 独占锁一致")
    _, wh2 = http("GET", f"/sessions/{sid}/transactions/T2/waits")
    check(len(wh2["waits"]) == 1 and wh2["waits"][0]["outcome"] is None,
          "重启后 T2 的历程仍在等待，与当前等待队列一致")
    _, wh3 = http("GET", f"/sessions/{sid}/transactions/T3/waits")
    rec3 = wh3["waits"][0]
    check(rec3["outcome"] == {"kind": "aborted", "seq": 10},
          "重启后 T3 的历程终局为 aborted@10，与撤销标记一致")
    check(rec3["position"] == 2
          and [w["tid"] for w in rec3["blocked_by"]["waiters"]] == ["T2"],
          "T3 的历程记录当时位置 2 与在先等待项 T2")


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
    section("验收结论：全部通过 —— 死锁撤销、队列推进、幂等拒绝、等待历程与重启恢复均可观察")
    return 0


if __name__ == "__main__":
    sys.exit(main())
