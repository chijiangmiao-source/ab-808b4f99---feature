"""核心锁管理逻辑：严格 FIFO 等待队列、共享/独占/升级、死锁裁决与崩溃恢复。

持久化采用单文件 JSON + 临时文件原子 rename（含 fsync）。每个事件只有两个落盘时刻：

* 普通事件：仅在完整裁决后落盘 —— 崩溃后只能看到上一完整裁决；
* 撤销事件：先把**该事件之前**的状态连同 pending 载荷落盘并 fsync，
  再在内存中执行撤销与队列推进，最后写入「完整裁决后」状态并 fsync。

因此在撤销持久化阶段中断后重启，磁盘状态只可能是：该事件之前，
或该事件完整裁决之后（默认前滚到完整裁决；RECOVERY_MODE=rollback
则回到事件前，调用方可凭同一稳定事件标识重新提交继续推进）。

等待历程（waits）随状态一同原子落盘：请求/升级首次入队时追加一条
不可变记录（触发事件、资源、目标模式、当时队列位置、直接阻塞者），
获授/被撤销时只为同一历程补齐一次终局与裁决序号，重启后与锁状态一致。
"""

from __future__ import annotations

import copy
import json
import os
import tempfile
from dataclasses import dataclass

SHARED = "S"
EXCLUSIVE = "X"

MAX_TRANSACTIONS_DEFAULT = 4
MAX_EVENTS_DEFAULT = 48

VALID_ACTIONS = {"begin", "request", "upgrade", "release", "commit"}
VALID_MODES = {SHARED, EXCLUSIVE}


class LockError(Exception):
    """语义级非法请求：拒绝且不改变任何状态。"""

    def __init__(self, code: str, message: str, http_status: int = 422):
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status


class ConflictError(LockError):
    def __init__(self, code: str, message: str):
        super().__init__(code, message, http_status=409)


@dataclass
class FaultConfig:
    # abort_persist: 在撤销事件的「事件前」检查点 fsync 之后、完整裁决落盘之前崩溃
    failpoint: str | None = None
    # rollforward（默认）：重启后把 pending 撤销执行到底；rollback：丢弃 pending，回到事件前
    recovery_mode: str = "rollforward"
    # 允许按事件注入崩溃（验收用，由 HTTP 层开关控制）
    allow_crash_injection: bool = False


def _canonical_payload(payload: dict) -> str:
    """稳定事件内容指纹：相同标识不同内容可被识别。"""
    # 事件内容由会话内的 tid/action/resource/mode 决定；event_id 本身已按会话隔离
    keys = ("tid", "action", "resource", "mode")
    compact = {k: payload.get(k) for k in keys if payload.get(k) is not None}
    return json.dumps(compact, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


class LockManager:
    def __init__(
        self,
        session_id: str,
        data_dir: str,
        max_transactions: int = MAX_TRANSACTIONS_DEFAULT,
        max_events: int = MAX_EVENTS_DEFAULT,
        fault: FaultConfig | None = None,
    ):
        self.session_id = session_id
        self.data_dir = data_dir
        self.max_transactions = max_transactions
        self.max_events = max_events
        self.fault = fault or FaultConfig()
        self.path = os.path.join(data_dir, f"{session_id}.json")
        self.state = self._load_or_init()

    # ------------------------------------------------------------------ 持久化

    def _blank_state(self) -> dict:
        return {
            "version": 1,
            "session_id": self.session_id,
            "next_seq": 1,
            "txs": {},  # tid -> {"begin_seq": int, "status": active|aborted|committed}
            "locks": {},  # resource -> {"mode": "S"|"X", "holders": [tids]}
            "queues": {},  # resource -> [{"tid","mode","upgrade","event_id","enq_seq"}]
            "events": {},  # event_id -> 完整裁决（首次裁决，幂等返回）
            "waits": [],  # 等待历程：首次入队追加一条不可变记录，终局只补齐一次
            "pending": None,  # 撤销事件两阶段提交的「事件前」检查点
        }

    def _load_or_init(self) -> dict:
        if not os.path.exists(self.path):
            return self._blank_state()
        with open(self.path, "r", encoding="utf-8") as fh:
            state = json.load(fh)
        state.setdefault("waits", [])  # 兼容没有等待历程字段的旧状态文件
        pending = state.get("pending")
        if pending:
            if self.fault.recovery_mode == "rollback":
                # 磁盘上本就是事件前快照：仅摘除 pending 标记
                state["pending"] = None
            else:
                # 前滚：从事件前状态把该撤销事件执行到完整裁决之后
                state["pending"] = None
                self._execute_verdict(state, pending["payload"], persist=False)
            self._atomic_write(state)
        return state

    def _atomic_write(self, state: dict) -> None:
        os.makedirs(self.data_dir, exist_ok=True)
        fd, tmp = tempfile.mkstemp(
            prefix=f".{self.session_id}.", suffix=".tmp", dir=self.data_dir
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(state, fh, ensure_ascii=False, indent=2)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.path)
            dir_fd = os.open(self.data_dir, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)

    # ------------------------------------------------------------------ 视图

    def snapshot(self) -> dict:
        return self._snapshot_from(self.state)

    def stored_verdict(self, event_id: str) -> dict | None:
        verdict = self.state["events"].get(event_id)
        if verdict is None:
            return None
        out = dict(verdict)
        out.pop("_content_hash", None)
        out["replayed"] = True
        return out

    def wait_history(self, tid: str) -> list[dict]:
        """按事务读取稳定排序的完整等待历程。

        不存在的事务（含属于其他会话的事务）与尚未产生等待的事务一律
        明确拒绝；历程按进入等待的裁决序号稳定排序，仍在等待的记录
        outcome 为 None。
        """
        if tid not in self.state["txs"]:
            raise LockError(
                "transaction_not_found",
                f"事务 {tid} 在本会话中不存在（跨会话查询一律拒绝）",
                404,
            )
        waits = [w for w in self.state["waits"] if w["tid"] == tid]
        if not waits:
            raise LockError(
                "wait_history_not_found",
                f"事务 {tid} 尚未产生等待历程",
                404,
            )
        waits.sort(key=lambda w: w["enq_seq"])
        return copy.deepcopy(waits)

    # ------------------------------------------------------------------ 入口

    def submit(self, payload: dict, crash_this_event: bool = False) -> dict:
        """提交一个事件；返回首次裁决（或同标识重放的首次裁决）。

        crash_this_event=True 时（需服务端开启崩溃注入开关），若本事件
        触发死锁撤销，则在「事件前」检查点落盘后模拟进程中断。
        """
        event_id = payload.get("event_id")
        if not isinstance(event_id, str) or not event_id:
            raise LockError("invalid_event_id", "event_id 必须是非空字符串", 400)

        existing = self.state["events"].get(event_id)
        if existing is not None:
            if existing["_content_hash"] != _canonical_payload(payload):
                raise ConflictError(
                    "event_id_content_conflict",
                    f"事件标识 {event_id} 已存在但内容不同，拒绝且不改变状态",
                )
            verdict = dict(existing)
            verdict.pop("_content_hash", None)
            verdict["replayed"] = True
            return verdict

        clean = self._validate(payload)

        # 所有语义校验与执行都在工作副本上进行；任何拒绝都不会触及已提交状态
        work = copy.deepcopy(self.state)
        verdict = self._execute_verdict(
            work, clean, persist=True, crash_this_event=crash_this_event
        )

        out = dict(verdict)
        out.pop("_content_hash", None)
        out["replayed"] = False
        return out

    def _validate(self, payload: dict) -> dict:
        tid = payload.get("tid")
        action = payload.get("action")
        if not isinstance(tid, str) or not tid:
            raise LockError("invalid_tid", "tid 必须是非空字符串", 400)
        if action not in VALID_ACTIONS:
            raise LockError(
                "invalid_action",
                f"action 必须是 {sorted(VALID_ACTIONS)} 之一",
                400,
            )
        resource = payload.get("resource")
        mode = payload.get("mode")
        if action in ("request", "upgrade", "release"):
            if not isinstance(resource, str) or not resource:
                raise LockError("invalid_resource", "resource 必须是非空字符串", 400)
        elif resource is not None:
            raise LockError("invalid_resource", f"{action} 事件不允许携带 resource", 400)
        if action == "request":
            if mode not in VALID_MODES:
                raise LockError("invalid_mode", "request 的 mode 必须是 S 或 X", 400)
        elif mode is not None:
            raise LockError("invalid_mode", f"{action} 事件不允许携带 mode", 400)
        return {
            "session_id": self.session_id,
            "event_id": payload["event_id"],
            "tid": tid,
            "action": action,
            "resource": resource if action in ("request", "upgrade", "release") else None,
            "mode": mode if action == "request" else None,
        }

    # ------------------------------------------------------------------ 执行

    def _execute_verdict(self, st: dict, p: dict, persist: bool,
                         crash_this_event: bool = False) -> dict:
        """在 st 上执行一个已校验的新事件并落盘。persist=False 用于崩溃后前滚。"""
        tid, action, resource = p["tid"], p["action"], p["resource"]
        enqueue_info: list[dict] = []
        will_abort = False

        if len(st["events"]) >= self.max_events:
            raise LockError("too_many_events", f"每个会话至多 {self.max_events} 个事件")

        seq = st["next_seq"]
        st["next_seq"] = seq + 1

        if action == "begin":
            if tid in st["txs"]:
                raise LockError(
                    "duplicate_transaction",
                    f"事务 {tid} 已经开始（重复 begin 拒绝且不改状态）",
                )
            if len(st["txs"]) >= self.max_transactions:
                raise LockError(
                    "too_many_transactions",
                    f"每个会话至多 {self.max_transactions} 个事务",
                )
            st["txs"][tid] = {"begin_seq": seq, "status": "active"}
        else:
            tx = st["txs"].get(tid)
            if tx is None:
                raise LockError("unknown_transaction", f"事务 {tid} 尚未 begin")
            if tx["status"] == "aborted":
                raise LockError(
                    "transaction_aborted",
                    f"事务 {tid} 已被撤销，撤销后继续操作一律拒绝且不改状态",
                )
            if tx["status"] == "committed":
                raise LockError(
                    "transaction_committed",
                    f"事务 {tid} 已提交，不能再接受新操作",
                )
            if action == "request":
                will_abort = self._apply_request(st, p, enqueue_info, seq)
            elif action == "upgrade":
                will_abort = self._apply_upgrade(st, p, enqueue_info, seq)
            elif action == "release":
                self._apply_release(st, tid, resource)
            elif action == "commit":
                self._apply_commit(st, tid)

        aborted: list[dict] = []
        granted: list[dict] = []

        if will_abort:
            if persist:
                # 阶段一：把真正的「事件前」状态连同 pending 载荷原子落盘
                pre = copy.deepcopy(self.state)
                pre["pending"] = {"event_id": p["event_id"], "payload": p}
                self._atomic_write(pre)
                crash = (
                    crash_this_event
                    if self.fault.allow_crash_injection
                    else self.fault.failpoint == "abort_persist"
                )
                if crash:
                    # 模拟在撤销持久化阶段中断：磁盘上恰好是事件前状态
                    os._exit(7)
            victims = self._resolve_deadlocks(st, granted, seq)
            aborted = [
                {"tid": v, "begin_seq": st["txs"][v]["begin_seq"]} for v in victims
            ]
        else:
            self._pump_all(st, granted, seq)

        verdict = {
            "ok": True,
            "error": None,
            "seq": seq,
            "event_id": p["event_id"],
            "session_id": self.session_id,
            "tid": tid,
            "action": action,
            "resource": resource,
            "mode": p.get("mode"),
            "enqueued": enqueue_info,
            "newly_granted": granted,
            "aborted": aborted,
            "state": self._snapshot_from(st),
            "_content_hash": _canonical_payload(p),
        }
        st["events"][p["event_id"]] = verdict

        if persist:
            st["pending"] = None
            self._atomic_write(st)
            self.state = st
        return verdict

    # ---------------------------------------------------------- 各事件原语

    def _holders(self, st: dict, resource: str) -> list[str]:
        lk = st["locks"].get(resource)
        return list(lk["holders"]) if lk else []

    def _is_queued(self, st: dict, tid: str, resource: str) -> bool:
        return any(e["tid"] == tid for e in st["queues"].get(resource, []))

    def _enqueue(self, st: dict, p: dict, mode: str, upgrade: bool,
                 enqueue_info: list[dict], seq: int) -> None:
        queue = st["queues"].setdefault(p["resource"], [])
        entry = {
            "tid": p["tid"],
            "mode": mode,
            "upgrade": upgrade,
            "event_id": p["event_id"],
            "enq_seq": st["next_seq"],
        }
        queue.append(entry)
        # 首次进入等待即落一条不可变历程：触发事件、资源、目标模式、当时
        # 队列位置，以及直接阻塞它的持锁事务与在先等待项（不含自己：
        # 升级等待期间自己保留的 S 锁并不阻塞自己）
        lk = st["locks"].get(p["resource"])
        st["waits"].append(
            {
                "event_id": p["event_id"],
                "tid": p["tid"],
                "resource": p["resource"],
                "mode": mode,
                "upgrade": upgrade,
                "enq_seq": seq,
                "position": len(queue),
                "blocked_by": {
                    "holders": [
                        {"tid": h, "mode": lk["mode"]}
                        for h in (lk["holders"] if lk else [])
                        if h != p["tid"]
                    ],
                    "waiters": [
                        {
                            "tid": e["tid"],
                            "mode": e["mode"],
                            "upgrade": e["upgrade"],
                            "event_id": e["event_id"],
                        }
                        for e in queue[:-1]
                    ],
                },
                "outcome": None,  # 仍在等待；终局只由 _finalize_wait 补齐一次
            }
        )
        enqueue_info.append(
            {
                "resource": p["resource"],
                "mode": mode,
                "upgrade": upgrade,
                "position": len(st["queues"][p["resource"]]),
            }
        )

    def _finalize_wait(self, st: dict, entry: dict, kind: str, seq: int) -> None:
        """为同一历程补齐唯一一次终局及对应裁决序号。

        kind 为 granted（队列推进获授）/ aborted（死锁裁决撤销其事务）/
        committed（提交清理等待；当前 commit 拒绝带等待的事务，此终局预留）。
        已补齐或旧数据无历程记录时不再改写。
        """
        for rec in st["waits"]:
            if rec["event_id"] == entry["event_id"]:
                if rec["outcome"] is None:
                    rec["outcome"] = {"kind": kind, "seq": seq}
                return

    def _apply_request(self, st: dict, p: dict, enqueue_info: list[dict],
                       seq: int) -> bool:
        tid, resource, mode = p["tid"], p["resource"], p["mode"]
        if self._is_queued(st, tid, resource):
            raise LockError(
                "already_waiting", f"事务 {tid} 已在 {resource} 的等待队列中"
            )
        lk = st["locks"].get(resource)
        if lk is not None and tid in lk["holders"]:
            if lk["mode"] == mode:
                raise LockError(
                    "already_holds", f"事务 {tid} 已持有 {resource} 的 {mode} 锁"
                )
            # 持 S 请 X 等价于升级
            return self._apply_upgrade(st, p, enqueue_info, seq)

        queue = st["queues"].get(resource, [])
        holders = self._holders(st, resource)
        can_grant = (
            not queue
            and (
                (mode == SHARED and (lk is None or lk["mode"] == SHARED))
                or (mode == EXCLUSIVE and not holders)
            )
        )
        if can_grant:
            self._grant(st, resource, tid, mode, upgrade=False)
            return False
        self._enqueue(st, p, mode, upgrade=False, enqueue_info=enqueue_info, seq=seq)
        return self._in_deadlock(st)

    def _apply_upgrade(self, st: dict, p: dict, enqueue_info: list[dict],
                       seq: int) -> bool:
        tid, resource = p["tid"], p["resource"]
        if self._is_queued(st, tid, resource):
            raise LockError(
                "already_waiting", f"事务 {tid} 已在 {resource} 的等待队列中"
            )
        lk = st["locks"].get(resource)
        if lk is None or tid not in lk["holders"]:
            raise LockError(
                "illegal_upgrade", f"事务 {tid} 未持有 {resource} 的 S 锁，无法升级"
            )
        if lk["mode"] == EXCLUSIVE:
            raise LockError("already_holds", f"事务 {tid} 已持有 {resource} 的 X 锁")
        others = [h for h in lk["holders"] if h != tid]
        queue = st["queues"].get(resource, [])
        # 队首存在任何等待（即使与当前 S 兼容）也不得插入其前；有其他 S 持有者也不能升级
        if not queue and not others:
            self._grant(st, resource, tid, EXCLUSIVE, upgrade=True)
            return False
        self._enqueue(st, p, EXCLUSIVE, upgrade=True, enqueue_info=enqueue_info,
                      seq=seq)
        # 升级等待期间继续保留自己的 S 锁
        return self._in_deadlock(st)

    def _apply_release(self, st: dict, tid: str, resource: str) -> None:
        if self._is_queued(st, tid, resource):
            raise LockError(
                "illegal_release",
                f"事务 {tid} 对 {resource} 的申请仍在等待队列中，不能 release",
            )
        lk = st["locks"].get(resource)
        if lk is None or tid not in lk["holders"]:
            raise LockError(
                "illegal_release", f"事务 {tid} 未持有 {resource} 的锁，非法释放"
            )
        lk["holders"].remove(tid)
        if not lk["holders"]:
            del st["locks"][resource]

    def _apply_commit(self, st: dict, tid: str) -> None:
        # 既有语义：尚有等待中申请的事务拒绝 commit，因此 commit 不会清理
        # 任何等待；若未来允许提交清理等待，须在此以 "committed" 补齐历程终局
        for queue in st["queues"].values():
            if any(e["tid"] == tid for e in queue):
                raise LockError(
                    "illegal_commit", f"事务 {tid} 尚有等待中的申请，不能 commit"
                )
        for resource in [r for r, lk in st["locks"].items() if tid in lk["holders"]]:
            lk = st["locks"][resource]
            lk["holders"].remove(tid)
            if not lk["holders"]:
                del st["locks"][resource]
        st["txs"][tid]["status"] = "committed"

    # ---------------------------------------------------------- 授予与推进

    def _grant(self, st: dict, resource: str, tid: str, mode: str, upgrade: bool) -> None:
        lk = st["locks"].get(resource)
        if lk is None:
            st["locks"][resource] = {"mode": mode, "holders": [tid]}
        elif upgrade or mode == EXCLUSIVE:
            # S->X：自身 S 收敛为独占
            st["locks"][resource] = {"mode": EXCLUSIVE, "holders": [tid]}
        else:
            lk["holders"].append(tid)

    def _pump(self, st: dict, resource: str, granted: list[dict], seq: int) -> None:
        """严格 FIFO 推进：只看队首，后到请求绝不越过队首；连续可授予的队首连续推进。"""
        queue = st["queues"].get(resource, [])
        while queue:
            entry = queue[0]
            lk = st["locks"].get(resource)
            if entry["mode"] == SHARED:
                ok = lk is None or lk["mode"] == SHARED
            else:
                ok = lk is None or lk["holders"] == [entry["tid"]]
            if not ok:
                break
            queue.pop(0)
            self._grant(st, resource, entry["tid"], entry["mode"], entry["upgrade"])
            self._finalize_wait(st, entry, "granted", seq)
            granted.append(
                {
                    "resource": resource,
                    "tid": entry["tid"],
                    "mode": entry["mode"],
                    "upgrade": entry["upgrade"],
                }
            )
        if not queue:
            st["queues"].pop(resource, None)

    def _pump_all(self, st: dict, granted: list[dict], seq: int) -> None:
        for resource in list(st["queues"].keys()):
            self._pump(st, resource, granted, seq)

    # ---------------------------------------------------------- 死锁检测

    def _waits_for_graph(self, st: dict) -> dict[str, set[str]]:
        graph: dict[str, set[str]] = {}
        for resource, queue in st["queues"].items():
            holders = self._holders(st, resource)
            for entry in queue:
                edges = graph.setdefault(entry["tid"], set())
                for h in holders:
                    if h != entry["tid"]:
                        edges.add(h)
        return graph

    def _cycle_nodes(self, graph: dict[str, set[str]]) -> list[str]:
        """Tarjan SCC：返回处于非平凡强连通分量（环）中的全部事务。"""
        index = 0
        stack: list[str] = []
        on_stack: set[str] = set()
        indices: dict[str, int] = {}
        low: dict[str, int] = {}
        result: list[str] = []

        def strongconnect(v: str) -> None:
            nonlocal index
            indices[v] = low[v] = index
            index += 1
            stack.append(v)
            on_stack.add(v)
            for w in graph.get(v, ()):
                if w not in indices:
                    strongconnect(w)
                    low[v] = min(low[v], low[w])
                elif w in on_stack:
                    low[v] = min(low[v], indices[w])
            if low[v] == indices[v]:
                comp = []
                while True:
                    w = stack.pop()
                    on_stack.discard(w)
                    comp.append(w)
                    if w == v:
                        break
                if len(comp) > 1:
                    result.extend(comp)

        for v in list(graph.keys()):
            if v not in indices:
                strongconnect(v)
        return result

    def _in_deadlock(self, st: dict) -> bool:
        return bool(self._cycle_nodes(self._waits_for_graph(st)))

    def _resolve_deadlocks(self, st: dict, granted: list[dict],
                           seq: int) -> list[str]:
        victims: list[str] = []
        while True:
            cycle = self._cycle_nodes(self._waits_for_graph(st))
            if not cycle:
                break
            # 裁决规则：撤销环上开始序号最大者
            victim = max(cycle, key=lambda t: (st["txs"][t]["begin_seq"], t))
            self._abort(st, victim, seq)
            victims.append(victim)
            # 原子清除后推进队列；推进可能暴露新的环，继续裁决
            self._pump_all(st, granted, seq)
        return victims

    def _abort(self, st: dict, tid: str, seq: int) -> None:
        # 原子清除该事务的全部等待，并为其历程补齐 aborted 终局
        for resource in list(st["queues"].keys()):
            kept = []
            for e in st["queues"][resource]:
                if e["tid"] == tid:
                    self._finalize_wait(st, e, "aborted", seq)
                else:
                    kept.append(e)
            if kept:
                st["queues"][resource] = kept
            else:
                del st["queues"][resource]
        # 原子清除该事务的全部锁
        for resource in list(st["locks"].keys()):
            lk = st["locks"][resource]
            if tid in lk["holders"]:
                lk["holders"].remove(tid)
                if not lk["holders"]:
                    del st["locks"][resource]
        st["txs"][tid]["status"] = "aborted"

    # ------------------------------------------------------------------ 快照

    def _snapshot_from(self, st: dict) -> dict:
        return {
            "verdict_seq": st["next_seq"] - 1,
            "transactions": {
                t: {"begin_seq": i["begin_seq"], "status": i["status"]}
                for t, i in st["txs"].items()
            },
            "locks": {
                r: {"mode": lk["mode"], "holders": list(lk["holders"])}
                for r, lk in st["locks"].items()
            },
            "waiting_queues": {
                r: [
                    {
                        "tid": e["tid"],
                        "mode": e["mode"],
                        "upgrade": e["upgrade"],
                        "event_id": e["event_id"],
                    }
                    for e in q
                ]
                for r, q in st["queues"].items()
                if q
            },
            "aborted_transactions": [
                t for t, i in st["txs"].items() if i["status"] == "aborted"
            ],
        }
