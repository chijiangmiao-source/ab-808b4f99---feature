"""锁管理器单元测试：兼容性、FIFO 队列、升级死锁、幂等与非法事件拒绝。"""

import json
import os
import tempfile
import unittest

from app.lockmgr import EXCLUSIVE, SHARED, LockError, LockManager


def ev(eid, tid, action, resource=None, mode=None):
    p = {"event_id": eid, "tid": tid, "action": action}
    if resource is not None:
        p["resource"] = resource
    if mode is not None:
        p["mode"] = mode
    return p


class Mgr:
    def __init__(self, max_transactions=4, max_events=48):
        self.dir = tempfile.mkdtemp()
        self.m = LockManager("s1", self.dir, max_transactions, max_events)

    def run(self, *a, **kw):
        return self.m.submit(ev(*a, **kw))


class BasicTest(unittest.TestCase):
    def test_begin_shared_release_commit_seq(self):
        g = Mgr()
        v = g.run("e1", "T1", "begin")
        self.assertEqual(v["seq"], 1)
        self.assertEqual(v["state"]["verdict_seq"], 1)
        v = g.run("e2", "T1", "request", "CH-A", SHARED)
        self.assertEqual(v["state"]["locks"]["CH-A"],
                         {"mode": "S", "holders": ["T1"]})
        v = g.run("e3", "T1", "release", "CH-A")
        self.assertNotIn("CH-A", v["state"]["locks"])
        g.run("e4", "T1", "request", "CH-B", EXCLUSIVE)
        v = g.run("e5", "T1", "commit")
        self.assertEqual(v["state"]["transactions"]["T1"]["status"], "committed")
        self.assertEqual(v["state"]["locks"], {})

    def test_shared_compatible_exclusive_not(self):
        g = Mgr()
        g.run("e1", "T1", "begin")
        g.run("e2", "T2", "begin")
        g.run("e3", "T3", "begin")
        g.run("e4", "T1", "request", "R", SHARED)
        v = g.run("e5", "T2", "request", "R", SHARED)
        self.assertEqual(v["state"]["locks"]["R"]["holders"], ["T1", "T2"])
        v = g.run("e6", "T3", "request", "R", EXCLUSIVE)
        self.assertTrue(v["enqueued"])
        self.assertEqual(v["state"]["waiting_queues"]["R"][0]["tid"], "T3")
        self.assertEqual(v["state"]["locks"]["R"]["mode"], "S")

    def test_unknown_txn_and_bad_inputs(self):
        g = Mgr()
        with self.assertRaises(LockError):
            g.run("e1", "T1", "request", "R", SHARED)
        g.run("e2", "T1", "begin")
        with self.assertRaises(LockError) as cm:
            g.run("e3", "T1", "begin")
        self.assertEqual(cm.exception.code, "duplicate_transaction")
        with self.assertRaises(LockError):
            g.run("e4", "T1", "request", "R", "BAD")
        with self.assertRaises(LockError):
            g.m.submit({"event_id": "e5", "tid": "T1", "action": "frobnicate"})

    def test_limits(self):
        g = Mgr(max_transactions=2, max_events=5)
        g.run("e1", "T1", "begin")
        g.run("e2", "T2", "begin")
        with self.assertRaises(LockError) as cm:
            g.run("e3", "T3", "begin")
        self.assertEqual(cm.exception.code, "too_many_transactions")
        g.run("e4", "T1", "request", "R", SHARED)
        g.run("e5", "T1", "release", "R")
        g.run("e6", "T1", "request", "R", SHARED)  # 第 5 个事件，允许
        with self.assertRaises(LockError) as cm2:
            g.run("e7", "T1", "release", "R")  # 第 6 个，超限
        self.assertEqual(cm2.exception.code, "too_many_events")


class FIFOQueueTest(unittest.TestCase):
    def test_compatible_request_cannot_jump_queue_head(self):
        g = Mgr()
        for i, t in enumerate(("T1", "T2", "T3"), start=1):
            g.run(f"b{i}", t, "begin")
        g.run("r1", "T1", "request", "CH", SHARED)
        # T2 排队等待 X（队首）
        v = g.run("r2", "T2", "request", "CH", EXCLUSIVE)
        self.assertEqual(v["state"]["waiting_queues"]["CH"][0]["tid"], "T2")
        # 后到的 T3 请求 S，虽与当前持有者 T1 的 S 兼容，也不得越过队首
        v = g.run("r3", "T3", "request", "CH", SHARED)
        q = v["state"]["waiting_queues"]["CH"]
        self.assertEqual([e["tid"] for e in q], ["T2", "T3"])
        self.assertEqual(v["state"]["locks"]["CH"]["holders"], ["T1"])
        # T1 释放：队首 T2 取得 X，T3 必须继续等待
        v = g.run("r4", "T1", "release", "CH")
        self.assertEqual(v["state"]["locks"]["CH"],
                         {"mode": "X", "holders": ["T2"]})
        self.assertEqual([e["tid"] for e in v["state"]["waiting_queues"]["CH"]], ["T3"])
        # T2 提交后队首推进，T3 取得 S
        v = g.run("r5", "T2", "commit")
        self.assertEqual(v["state"]["locks"]["CH"],
                         {"mode": "S", "holders": ["T3"]})
        self.assertNotIn("CH", v["state"]["waiting_queues"])

    def test_shared_batch_grant_in_fifo_order(self):
        g = Mgr()
        g.run("b1", "T1", "begin")
        g.run("b2", "T2", "begin")
        g.run("r1", "T1", "request", "CH", EXCLUSIVE)
        g.run("r2", "T2", "request", "CH", SHARED)
        v = g.run("c1", "T1", "commit")
        self.assertEqual(v["state"]["locks"]["CH"],
                         {"mode": "S", "holders": ["T2"]})

    def test_illegal_release_rejected_without_state_change(self):
        g = Mgr()
        g.run("b1", "T1", "begin")
        before = json.dumps(g.m.snapshot(), sort_keys=True)
        with self.assertRaises(LockError) as cm:
            g.run("e1", "T1", "release", "CH")
        self.assertEqual(cm.exception.code, "illegal_release")
        self.assertEqual(json.dumps(g.m.snapshot(), sort_keys=True), before)
        # 等待中的资源也不能 release
        g.run("b2", "T2", "begin")
        g.run("r1", "T1", "request", "CH", EXCLUSIVE)
        g.run("r2", "T2", "request", "CH", EXCLUSIVE)
        with self.assertRaises(LockError) as cm2:
            g.run("r3", "T2", "release", "CH")
        self.assertEqual(cm2.exception.code, "illegal_release")


class UpgradeDeadlockTest(unittest.TestCase):
    def _ring(self, g):
        # T1 先开始（序号1），T2 后开始（序号2）；各持一个通道的 S
        g.run("b1", "T1", "begin")
        g.run("b2", "T2", "begin")
        g.run("s1", "T1", "request", "CH-A", SHARED)
        g.run("s2", "T2", "request", "CH-B", SHARED)
        g.run("x1", "T1", "request", "CH-B", SHARED)
        g.run("x2", "T2", "request", "CH-A", SHARED)

    def test_cross_upgrade_aborts_latest_begin_seq(self):
        g = Mgr()
        self._ring(g)
        # T1 先升级 B：等待 T2 释放
        v = g.run("u1", "T1", "upgrade", "CH-B")
        self.assertTrue(v["enqueued"])
        self.assertEqual(v["state"]["waiting_queues"]["CH-B"][0]["upgrade"], True)
        # T2 升级 A：形成环
        v = g.run("u2", "T2", "upgrade", "CH-A")
        # 撤销开始序号最大的事务 T2
        self.assertEqual([a["tid"] for a in v["aborted"]], ["T2"])
        self.assertEqual(v["aborted"][0]["begin_seq"], 2)
        # 原子清除 T2 的全部等待与锁；T1 取得 B 的独占锁，仍持 A 的 S
        self.assertEqual(v["state"]["locks"]["CH-B"],
                         {"mode": "X", "holders": ["T1"]})
        self.assertEqual(v["state"]["locks"]["CH-A"],
                         {"mode": "S", "holders": ["T1"]})
        self.assertEqual(v["state"]["waiting_queues"], {})
        self.assertEqual(v["state"]["aborted_transactions"], ["T2"])
        self.assertEqual(v["state"]["verdict_seq"], 8)

    def test_victim_is_highest_begin_seq_regardless_of_order(self):
        # 反向顺序制造环：后提交升级的一方 begin 序号更小
        g = Mgr()
        g.run("b1", "T1", "begin")
        g.run("b2", "T2", "begin")
        g.run("a1", "T1", "request", "R1", SHARED)
        g.run("a2", "T2", "request", "R2", SHARED)
        g.run("q1", "T2", "upgrade", "R2")
        self.assertEqual(g.m.snapshot()["locks"]["R2"]["mode"], "X")
        # T2 此时已独占 R2；构造环需要第二资源，改用独立场景
        # 三方环 A->C->B->A：A 持 X/Z 的 S，B 持 X/Y 的 S，C 持 Y/Z 的 S
        g2 = Mgr()
        g2.run("b1", "A", "begin")
        g2.run("b2", "B", "begin")
        g2.run("b3", "C", "begin")
        g2.run("s1", "A", "request", "X", SHARED)
        g2.run("s2", "B", "request", "X", SHARED)
        g2.run("s3", "B", "request", "Y", SHARED)
        g2.run("s4", "C", "request", "Y", SHARED)
        g2.run("s5", "C", "request", "Z", SHARED)
        g2.run("s6", "A", "request", "Z", SHARED)
        g2.run("u1", "A", "upgrade", "Z")  # A 等 C
        g2.run("u2", "B", "upgrade", "X")  # B 等 A
        v = g2.run("u3", "C", "upgrade", "Y")  # C 等 B，环闭合
        # 环上 begin_seq 最大者 C（序号3）被撤销
        self.assertEqual([a["tid"] for a in v["aborted"]], ["C"])
        # C 清除后 Z 上 A 的升级被满足
        self.assertEqual(v["state"]["locks"]["Z"],
                         {"mode": "X", "holders": ["A"]})
        self.assertEqual(v["state"]["aborted_transactions"], ["C"])
        # 仍在等待的 B 保持排队
        self.assertEqual([e["tid"] for e in v["state"]["waiting_queues"]["X"]], ["B"])

    def test_aborted_txn_further_ops_rejected(self):
        g = Mgr()
        self._ring(g)
        g.run("u1", "T1", "upgrade", "CH-B")
        g.run("u2", "T2", "upgrade", "CH-A")
        before = json.dumps(g.m.snapshot(), sort_keys=True)
        with self.assertRaises(LockError) as cm:
            g.run("z1", "T2", "request", "CH-C", SHARED)
        self.assertEqual(cm.exception.code, "transaction_aborted")
        with self.assertRaises(LockError):
            g.run("z2", "T2", "release", "CH-A")
        with self.assertRaises(LockError):
            g.run("z3", "T2", "commit")
        self.assertEqual(json.dumps(g.m.snapshot(), sort_keys=True), before)

    def test_upgrade_without_shared_lock_rejected(self):
        g = Mgr()
        g.run("b1", "T1", "begin")
        with self.assertRaises(LockError) as cm:
            g.run("u1", "T1", "upgrade", "CH")
        self.assertEqual(cm.exception.code, "illegal_upgrade")

    def test_survivor_commits_after_deadlock(self):
        g = Mgr()
        self._ring(g)
        g.run("u1", "T1", "upgrade", "CH-B")
        v = g.run("u2", "T2", "upgrade", "CH-A")
        self.assertEqual(v["aborted"][0]["tid"], "T2")
        v = g.run("c1", "T1", "commit")
        self.assertEqual(v["state"]["locks"], {})


class WaitHistoryTest(unittest.TestCase):
    def test_no_history_when_granted_immediately(self):
        g = Mgr()
        g.run("e1", "T1", "begin")
        g.run("e2", "T1", "request", "R", SHARED)
        with self.assertRaises(LockError) as cm:
            g.m.wait_history("T1")
        self.assertEqual(cm.exception.code, "wait_history_not_found")
        self.assertEqual(cm.exception.http_status, 404)

    def test_unknown_transaction_rejected(self):
        g = Mgr()
        g.run("e1", "T1", "begin")
        with self.assertRaises(LockError) as cm:
            g.m.wait_history("T9")
        self.assertEqual(cm.exception.code, "transaction_not_found")
        self.assertEqual(cm.exception.http_status, 404)

    def test_wait_then_grant_fills_outcome_once(self):
        g = Mgr()
        g.run("b1", "T1", "begin")                    # seq1
        g.run("b2", "T2", "begin")                    # seq2
        g.run("r1", "T1", "request", "R", EXCLUSIVE)  # seq3
        v = g.run("r2", "T2", "request", "R", SHARED)  # seq4 等待
        self.assertTrue(v["enqueued"])
        h = g.m.wait_history("T2")
        self.assertEqual(len(h), 1)
        rec = h[0]
        self.assertEqual(rec["event_id"], "r2")
        self.assertEqual(rec["tid"], "T2")
        self.assertEqual(rec["resource"], "R")
        self.assertEqual(rec["mode"], SHARED)
        self.assertFalse(rec["upgrade"])
        self.assertEqual(rec["position"], 1)
        self.assertEqual(rec["enq_seq"], 4)
        self.assertEqual(rec["blocked_by"]["holders"], ["T1"])
        self.assertEqual(rec["blocked_by"]["waiters"], [])
        self.assertIsNone(rec["outcome"])
        self.assertIsNone(rec["outcome_seq"])
        # T1 提交 -> 队列推进，T2 获授，终局只补齐一次
        g.run("c1", "T1", "commit")                   # seq5
        g.run("c2", "T2", "commit")                   # seq6 不再触碰历程
        h = g.m.wait_history("T2")
        self.assertEqual(len(h), 1)
        self.assertEqual(h[0]["outcome"], "granted")
        self.assertEqual(h[0]["outcome_seq"], 5)

    def test_upgrade_blocked_by_holders_and_prior_waiters(self):
        # T1、T2 共持 S；T3 排 X 居队首；T2 升级 -> 阻塞者为持有人 T1 与在先等待 T3
        g = Mgr()
        g.run("b1", "T1", "begin")
        g.run("b2", "T2", "begin")
        g.run("b3", "T3", "begin")
        g.run("s1", "T1", "request", "R", SHARED)
        g.run("s2", "T2", "request", "R", SHARED)
        g.run("x1", "T3", "request", "R", EXCLUSIVE)  # seq6 等待
        v = g.run("u1", "T2", "upgrade", "R")          # seq7 等待，位置 2
        self.assertEqual(v["enqueued"][0]["position"], 2)
        rec = g.m.wait_history("T2")[0]
        self.assertEqual(rec["event_id"], "u1")
        self.assertEqual(rec["mode"], EXCLUSIVE)
        self.assertTrue(rec["upgrade"])
        self.assertEqual(rec["position"], 2)
        self.assertEqual(rec["enq_seq"], 7)
        self.assertEqual(rec["blocked_by"]["holders"], ["T1"])
        waiters = rec["blocked_by"]["waiters"]
        self.assertEqual([w["tid"] for w in waiters], ["T3"])
        self.assertEqual(waiters[0]["mode"], EXCLUSIVE)
        self.assertEqual(waiters[0]["event_id"], "x1")

    def test_upgrade_self_holding_not_a_blocker(self):
        # T1 独持 S，T2 排 X；T1 升级 -> 直接阻塞者只有在先等待的 T2
        g = Mgr()
        g.run("b1", "T1", "begin")
        g.run("b2", "T2", "begin")
        g.run("s1", "T1", "request", "R", SHARED)
        g.run("x1", "T2", "request", "R", EXCLUSIVE)
        g.run("u1", "T1", "upgrade", "R")
        rec = g.m.wait_history("T1")[0]
        self.assertEqual(rec["blocked_by"]["holders"], [])
        self.assertEqual([w["tid"] for w in rec["blocked_by"]["waiters"]], ["T2"])
        self.assertEqual(rec["position"], 2)

    def test_deadlock_closes_victim_and_grants_survivor(self):
        g = Mgr()
        g.run("b1", "T1", "begin")
        g.run("b2", "T2", "begin")
        g.run("s1", "T1", "request", "CH-A", SHARED)
        g.run("s2", "T2", "request", "CH-B", SHARED)
        g.run("x1", "T1", "request", "CH-B", SHARED)
        g.run("x2", "T2", "request", "CH-A", SHARED)
        g.run("u1", "T1", "upgrade", "CH-B")           # seq7 等待
        v = g.run("u2", "T2", "upgrade", "CH-A")       # seq8 成环，撤销 T2
        self.assertEqual([a["tid"] for a in v["aborted"]], ["T2"])
        h2 = g.m.wait_history("T2")
        self.assertEqual(len(h2), 1)
        self.assertEqual(h2[0]["event_id"], "u2")
        self.assertEqual(h2[0]["outcome"], "aborted")
        self.assertEqual(h2[0]["outcome_seq"], 8)
        self.assertEqual(h2[0]["blocked_by"]["holders"], ["T1"])
        h1 = g.m.wait_history("T1")
        self.assertEqual(h1[0]["event_id"], "u1")
        self.assertEqual(h1[0]["outcome"], "granted")
        self.assertEqual(h1[0]["outcome_seq"], 8)

    def test_replay_and_rejection_do_not_touch_history(self):
        g = Mgr()
        g.run("b1", "T1", "begin")
        g.run("b2", "T2", "begin")
        g.run("r1", "T1", "request", "R", EXCLUSIVE)
        g.run("r2", "T2", "request", "R", SHARED)      # seq4 等待
        again = g.run("r2", "T2", "request", "R", SHARED)  # 重放
        self.assertTrue(again["replayed"])
        h = g.m.wait_history("T2")
        self.assertEqual(len(h), 1)
        self.assertIsNone(h[0]["outcome"])
        # 同标识不同内容被拒，不新增不改写
        with self.assertRaises(LockError):
            g.run("r2", "T2", "request", "R", EXCLUSIVE)
        # 语义拒绝（等待中重复申请）也不新增
        with self.assertRaises(LockError):
            g.run("r3", "T2", "request", "R", SHARED)
        h = g.m.wait_history("T2")
        self.assertEqual(len(h), 1)
        self.assertIsNone(h[0]["outcome"])

    def test_multiple_waits_stable_order(self):
        g = Mgr()
        g.run("b1", "T1", "begin")
        g.run("b2", "T2", "begin")
        g.run("r1", "T2", "request", "R1", EXCLUSIVE)
        g.run("r2", "T2", "request", "R2", EXCLUSIVE)
        g.run("w1", "T1", "request", "R1", SHARED)     # seq5 等待
        g.run("w2", "T1", "request", "R2", SHARED)     # seq6 等待
        h = g.m.wait_history("T1")
        self.assertEqual([r["event_id"] for r in h], ["w1", "w2"])
        self.assertEqual([r["enq_seq"] for r in h], [5, 6])
        self.assertEqual([r["resource"] for r in h], ["R1", "R2"])

    def test_history_survives_reload_and_stays_consistent(self):
        d = tempfile.mkdtemp()
        m = LockManager("hist", d)
        m.submit(ev("b1", "T1", "begin"))
        m.submit(ev("b2", "T2", "begin"))
        m.submit(ev("r1", "T1", "request", "R", EXCLUSIVE))
        m.submit(ev("r2", "T2", "request", "R", SHARED))
        m2 = LockManager("hist", d)
        h = m2.wait_history("T2")
        self.assertEqual(len(h), 1)
        self.assertIsNone(h[0]["outcome"])
        # 仍在等待的历程与当前等待队列一致
        self.assertEqual(
            [e["tid"] for e in m2.snapshot()["waiting_queues"]["R"]], ["T2"]
        )
        # 重载后继续推进，终局正确补齐
        m2.submit(ev("c1", "T1", "commit"))
        self.assertEqual(m2.wait_history("T2")[0]["outcome"], "granted")
        self.assertEqual(m2.wait_history("T2")[0]["outcome_seq"], 5)
        # 再次重载：已结束的历程保持终局
        m3 = LockManager("hist", d)
        self.assertEqual(m3.wait_history("T2")[0]["outcome"], "granted")


class IdempotencyTest(unittest.TestCase):
    def test_duplicate_event_id_returns_first_verdict(self):
        g = Mgr()
        g.run("e1", "T1", "begin")
        v1 = g.run("e2", "T1", "request", "R", SHARED)
        v1j = json.dumps({k: v for k, v in v1.items() if k != "replayed"},
                         sort_keys=True)
        again = g.run("e2", "T1", "request", "R", SHARED)
        self.assertTrue(again["replayed"])
        againj = json.dumps({k: v for k, v in again.items() if k != "replayed"},
                            sort_keys=True)
        self.assertEqual(againj, v1j)
        self.assertEqual(g.m.snapshot()["verdict_seq"], 2)

    def test_same_id_different_content_rejected_no_state_change(self):
        g = Mgr()
        g.run("e1", "T1", "begin")
        g.run("e2", "T1", "request", "R", SHARED)
        before = json.dumps(g.m.snapshot(), sort_keys=True)
        with self.assertRaises(LockError) as cm:
            g.run("e2", "T1", "request", "R", EXCLUSIVE)
        self.assertEqual(cm.exception.code, "event_id_content_conflict")
        with self.assertRaises(LockError):
            # 同标识但动作不同
            g.run("e2", "T1", "release", "R")
        self.assertEqual(json.dumps(g.m.snapshot(), sort_keys=True), before)

    def test_rejected_event_does_not_consume_seq(self):
        g = Mgr()
        g.run("e1", "T1", "begin")
        with self.assertRaises(LockError):
            g.run("e2", "T1", "release", "R")
        v = g.run("e3", "T1", "request", "R", SHARED)
        self.assertEqual(v["seq"], 2)


class PersistenceReloadTest(unittest.TestCase):
    def test_state_survives_normal_reload(self):
        d = tempfile.mkdtemp()
        m = LockManager("persist", d)
        m.submit(ev("e1", "T1", "begin"))
        m.submit(ev("e2", "T1", "request", "R", SHARED))
        m2 = LockManager("persist", d)
        snap = m2.snapshot()
        self.assertEqual(snap["locks"]["R"], {"mode": "S", "holders": ["T1"]})
        # 重载后同标识重放仍返回首次裁决
        v = m2.submit(ev("e2", "T1", "request", "R", SHARED))
        self.assertTrue(v["replayed"])
        self.assertEqual(v["seq"], 2)


if __name__ == "__main__":
    unittest.main()
