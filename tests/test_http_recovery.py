"""崩溃恢复与 HTTP 冒烟：启动真实服务子进程，在撤销持久化阶段制造崩溃。"""

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_health(port: int, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/health", timeout=1
            ) as r:
                if r.status == 200:
                    return
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(0.2)
    raise RuntimeError(f"服务未在 {timeout}s 内就绪: {last}")


def start_server(data_dir: str, port: int, extra_env: dict | None = None) -> subprocess.Popen:
    env = dict(os.environ)
    env.update({"DATA_DIR": data_dir, "PORT": str(port),
                "HOST": "127.0.0.1", "QUIET": "1"})
    if extra_env:
        env.update(extra_env)
    proc = subprocess.Popen(
        [sys.executable, "-m", "app.server"],
        cwd=REPO_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    try:
        wait_health(port)
    except Exception:
        proc.kill()
        out = proc.stdout.read().decode() if proc.stdout else ""
        raise
    return proc


def post(port: int, sid: str, payload: dict):
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/sessions/{sid}/events",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())


def get(port: int, path: str):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=5) as r:
        return r.status, json.loads(r.read().decode())


def ring_events(prefix: str = ""):
    def p(eid, tid, action, res=None, mode=None):
        d = {"event_id": prefix + eid, "tid": tid, "action": action}
        if res:
            d["resource"] = res
        if mode:
            d["mode"] = mode
        return d

    return [
        p("b1", "T1", "begin"),
        p("b2", "T2", "begin"),
        p("s1", "T1", "request", "CH-A", "S"),
        p("s2", "T2", "request", "CH-B", "S"),
        p("x1", "T1", "request", "CH-B", "S"),
        p("x2", "T2", "request", "CH-A", "S"),
        p("u1", "T1", "upgrade", "CH-B"),
    ]


class CrashRecoveryTest(unittest.TestCase):
    def _drive_to_brink(self, data_dir: str, port: int, sid: str) -> subprocess.Popen:
        proc = start_server(data_dir, port, {"FAILPOINT": "abort_persist"})
        for e in ring_events():
            status, body = post(port, sid, e)
            self.assertEqual(status, 201, body)
        # 闭合环的事件触发撤销 -> 检查点落盘后进程退出(7)
        with self.assertRaises((urllib.error.URLError, ConnectionError)):
            post(port, sid,
                 {"event_id": "u2", "tid": "T2", "action": "upgrade",
                  "resource": "CH-A"})
        deadline = time.time() + 5
        while time.time() < deadline and proc.poll() is None:
            time.sleep(0.1)
        self.assertIsNotNone(proc.poll(), "故障注入应使进程退出")
        self.assertEqual(proc.returncode, 7)
        return proc

    def test_rollforward_after_crash(self):
        data_dir = tempfile.mkdtemp()
        port = free_port()
        sid = "rollforward-session"
        proc = self._drive_to_brink(data_dir, port, sid)
        proc.wait()
        # 磁盘上只能看到该事件之前：裁决序号为 7，两个事务都还活着
        with open(os.path.join(data_dir, f"{sid}.json")) as fh:
            on_disk = json.load(fh)
        self.assertEqual(on_disk["next_seq"], 8)
        self.assertIsNotNone(on_disk["pending"])
        self.assertNotIn("u2", on_disk["events"])

        # 重启（默认前滚）：恢复到完整裁决后
        proc2 = start_server(data_dir, port)
        try:
            _, snap = get(port, f"/sessions/{sid}")
            self.assertEqual(snap["verdict_seq"], 8)
            self.assertEqual(snap["aborted_transactions"], ["T2"])
            self.assertEqual(snap["locks"]["CH-B"],
                             {"mode": "X", "holders": ["T1"]})
            self.assertEqual(snap["locks"]["CH-A"],
                             {"mode": "S", "holders": ["T1"]})
            self.assertEqual(snap["waiting_queues"], {})
            # 裁决可凭稳定标识查询
            _, v = get(port, f"/sessions/{sid}/events/u2")
            self.assertEqual(v["seq"], 8)
            self.assertEqual([a["tid"] for a in v["aborted"]], ["T2"])
            # 可继续处理后续合法事件
            status, v2 = post(port, sid,
                              {"event_id": "c1", "tid": "T1", "action": "commit"})
            self.assertEqual(status, 201)
            self.assertEqual(v2["state"]["locks"], {})
            # 已撤销事务继续操作仍被拒绝
            status, body = post(port, sid,
                                {"event_id": "z1", "tid": "T2",
                                 "action": "request", "resource": "CH-C",
                                 "mode": "S"})
            self.assertEqual(status, 422)
            self.assertEqual(body["error"], "transaction_aborted")
            # 等待历程：重启后已结束的历程与锁状态一致
            _, h2 = get(port, f"/sessions/{sid}/transactions/T2/wait-history")
            self.assertEqual(len(h2["history"]), 1)
            rec = h2["history"][0]
            self.assertEqual(rec["event_id"], "u2")
            self.assertEqual(rec["resource"], "CH-A")
            self.assertEqual(rec["mode"], "X")
            self.assertTrue(rec["upgrade"])
            self.assertEqual(rec["blocked_by"]["holders"], ["T1"])
            self.assertEqual(rec["outcome"], "aborted")
            self.assertEqual(rec["outcome_seq"], 8)
            _, h1 = get(port, f"/sessions/{sid}/transactions/T1/wait-history")
            self.assertEqual(h1["history"][0]["event_id"], "u1")
            self.assertEqual(h1["history"][0]["outcome"], "granted")
            self.assertEqual(h1["history"][0]["outcome_seq"], 8)
            # 不存在的事务（含跨会话 tid）明确拒绝
            with self.assertRaises(urllib.error.HTTPError) as cm:
                get(port, f"/sessions/{sid}/transactions/T9/wait-history")
            self.assertEqual(cm.exception.code, 404)
            self.assertEqual(json.loads(cm.exception.read().decode())["error"],
                             "transaction_not_found")
        finally:
            proc2.kill()
            proc2.wait()

    def test_rollback_to_before_event_then_resubmit(self):
        data_dir = tempfile.mkdtemp()
        port = free_port()
        sid = "rollback-session"
        proc = self._drive_to_brink(data_dir, port, sid)
        proc.wait()
        # 以回滚模式重启：状态回到该事件之前
        proc2 = start_server(data_dir, port, {"RECOVERY_MODE": "rollback"})
        try:
            _, snap = get(port, f"/sessions/{sid}")
            self.assertEqual(snap["verdict_seq"], 7)
            self.assertEqual(snap["aborted_transactions"], [])
            self.assertIn("CH-B", snap["waiting_queues"])
            # 回到事件前：u2 的历程不存在，T1 的等待历程仍处于打开状态
            with self.assertRaises(urllib.error.HTTPError) as cm:
                get(port, f"/sessions/{sid}/transactions/T2/wait-history")
            self.assertEqual(cm.exception.code, 404)
            self.assertEqual(json.loads(cm.exception.read().decode())["error"],
                             "wait_history_not_found")
            _, h1 = get(port, f"/sessions/{sid}/transactions/T1/wait-history")
            self.assertEqual(h1["history"][0]["event_id"], "u1")
            self.assertIsNone(h1["history"][0]["outcome"])
            # 用同一稳定事件标识重新提交，完成完整裁决
            status, v = post(port, sid,
                             {"event_id": "u2", "tid": "T2",
                              "action": "upgrade", "resource": "CH-A"})
            self.assertEqual(status, 201, v)
            self.assertEqual([a["tid"] for a in v["aborted"]], ["T2"])
            self.assertEqual(v["state"]["locks"]["CH-B"],
                             {"mode": "X", "holders": ["T1"]})
            _, snap2 = get(port, f"/sessions/{sid}")
            self.assertEqual(snap2["verdict_seq"], 8)
            # 重提交后历程与完整裁决一致
            _, h2 = get(port, f"/sessions/{sid}/transactions/T2/wait-history")
            self.assertEqual(h2["history"][0]["outcome"], "aborted")
            self.assertEqual(h2["history"][0]["outcome_seq"], 8)
            _, h1 = get(port, f"/sessions/{sid}/transactions/T1/wait-history")
            self.assertEqual(h1["history"][0]["outcome"], "granted")
            self.assertEqual(h1["history"][0]["outcome_seq"], 8)
        finally:
            proc2.kill()
            proc2.wait()


class HttpSmokeTest(unittest.TestCase):
    def test_full_scenario_over_http(self):
        data_dir = tempfile.mkdtemp()
        port = free_port()
        sid = "smoke"
        proc = start_server(data_dir, port)
        try:
            status, health = get(port, "/health")
            self.assertEqual(status, 200)
            self.assertEqual(health["status"], "ok")

            def ok(eid, tid, action, res=None, mode=None):
                payload = {"event_id": eid, "tid": tid, "action": action}
                if res:
                    payload["resource"] = res
                if mode:
                    payload["mode"] = mode
                status, body = post(port, sid, payload)
                self.assertIn(status, (200, 201), body)
                return body

            ok("b1", "T1", "begin")
            ok("b2", "T2", "begin")
            ok("b3", "T3", "begin")
            ok("r1", "T1", "request", "CH", "S")
            ok("r2", "T2", "request", "CH", "X")  # 队首等待
            ok("r3", "T3", "request", "CH", "S")  # 兼容也不得越过队首
            _, snap = get(port, f"/sessions/{sid}")
            self.assertEqual(
                [e["tid"] for e in snap["waiting_queues"]["CH"]],
                ["T2", "T3"],
            )

            # 非法释放被拒绝，422 且状态不变
            status, body = post(port, sid,
                                {"event_id": "bad", "tid": "T3",
                                 "action": "release", "resource": "CH"})
            self.assertEqual(status, 422)
            self.assertEqual(body["error"], "illegal_release")
            _, snap2 = get(port, f"/sessions/{sid}")
            self.assertEqual(snap2["verdict_seq"], snap["verdict_seq"])

            # 同标识重放返回首次裁决
            status, replay = post(
                port, sid,
                {"event_id": "r1", "tid": "T1", "action": "request",
                 "resource": "CH", "mode": "S"})
            self.assertEqual(status, 200)
            self.assertTrue(replay["replayed"])
            self.assertEqual(replay["seq"], 4)

            # 同标识不同内容 -> 409
            status, body = post(
                port, sid,
                {"event_id": "r1", "tid": "T1", "action": "request",
                 "resource": "CH", "mode": "X"})
            self.assertEqual(status, 409)
            self.assertEqual(body["error"], "event_id_content_conflict")

            # 队列推进：T1 释放后 T2 取 X，T3 继续等；T2 提交后 T3 取 S
            ok("rel", "T1", "release", "CH")
            _, snap3 = get(port, f"/sessions/{sid}")
            self.assertEqual(snap3["locks"]["CH"],
                             {"mode": "X", "holders": ["T2"]})
            ok("c2", "T2", "commit")
            _, snap4 = get(port, f"/sessions/{sid}")
            self.assertEqual(snap4["locks"]["CH"],
                             {"mode": "S", "holders": ["T3"]})

            # 未知会话与未知事件
            with self.assertRaises(urllib.error.HTTPError) as cm:
                get(port, "/sessions/nope")
            self.assertEqual(cm.exception.code, 404)
        finally:
            proc.kill()
            proc.wait()


class WaitHistoryHttpTest(unittest.TestCase):
    def test_upgrade_wait_then_granted_over_http(self):
        data_dir = tempfile.mkdtemp()
        port = free_port()
        sid = "hist-http"
        proc = start_server(data_dir, port)
        try:
            def ok(eid, tid, action, res=None, mode=None, expect=201):
                payload = {"event_id": eid, "tid": tid, "action": action}
                if res:
                    payload["resource"] = res
                if mode:
                    payload["mode"] = mode
                status, body = post(port, sid, payload)
                self.assertEqual(status, expect, body)
                return body

            ok("b1", "T1", "begin")
            ok("b2", "T2", "begin")
            ok("s1", "T1", "request", "CH-U", "S")
            ok("s2", "T2", "request", "CH-U", "S")
            v = ok("u1", "T2", "upgrade", "CH-U")  # seq5：存在其他 S 持有者，进入等待
            self.assertTrue(v["enqueued"])

            # 等待中：历程已建档，终局未补齐
            _, h = get(port, f"/sessions/{sid}/transactions/T2/wait-history")
            self.assertEqual(h["session_id"], sid)
            self.assertEqual(h["tid"], "T2")
            self.assertEqual(len(h["history"]), 1)
            rec = h["history"][0]
            self.assertEqual(rec["event_id"], "u1")
            self.assertEqual(rec["resource"], "CH-U")
            self.assertEqual(rec["mode"], "X")
            self.assertTrue(rec["upgrade"])
            self.assertEqual(rec["position"], 1)
            self.assertEqual(rec["enq_seq"], 5)
            self.assertEqual(rec["blocked_by"]["holders"], ["T1"])
            self.assertEqual(rec["blocked_by"]["waiters"], [])
            self.assertIsNone(rec["outcome"])
            self.assertIsNone(rec["outcome_seq"])

            # 重放同一事件：不新增、不改写历程
            ok("u1", "T2", "upgrade", "CH-U", expect=200)
            _, h = get(port, f"/sessions/{sid}/transactions/T2/wait-history")
            self.assertEqual(len(h["history"]), 1)
            self.assertIsNone(h["history"][0]["outcome"])

            # T1 释放 -> 队列推进，T2 的升级获授，终局只补齐一次
            ok("r1", "T1", "release", "CH-U")  # seq6
            _, h = get(port, f"/sessions/{sid}/transactions/T2/wait-history")
            rec = h["history"][0]
            self.assertEqual(rec["outcome"], "granted")
            self.assertEqual(rec["outcome_seq"], 6)
            _, snap = get(port, f"/sessions/{sid}")
            self.assertEqual(snap["locks"]["CH-U"],
                             {"mode": "X", "holders": ["T2"]})
            self.assertNotIn("CH-U", snap["waiting_queues"])

            # 明确拒绝：从未等待的事务、不存在/跨会话的事务、不存在的会话
            with self.assertRaises(urllib.error.HTTPError) as cm:
                get(port, f"/sessions/{sid}/transactions/T1/wait-history")
            self.assertEqual(cm.exception.code, 404)
            self.assertEqual(json.loads(cm.exception.read().decode())["error"],
                             "wait_history_not_found")
            with self.assertRaises(urllib.error.HTTPError) as cm:
                get(port, f"/sessions/{sid}/transactions/T9/wait-history")
            self.assertEqual(cm.exception.code, 404)
            self.assertEqual(json.loads(cm.exception.read().decode())["error"],
                             "transaction_not_found")
            with self.assertRaises(urllib.error.HTTPError) as cm:
                get(port, "/sessions/nope/transactions/T2/wait-history")
            self.assertEqual(cm.exception.code, 404)
            self.assertEqual(json.loads(cm.exception.read().decode())["error"],
                             "session_not_found")
        finally:
            proc.kill()
            proc.wait()


if __name__ == "__main__":
    unittest.main()
