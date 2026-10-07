"""HTTP 服务：维护事务锁管理会话。

路由：
  GET  /health                         健康检查
  POST /sessions/{sid}/events          提交一个稳定事件，返回裁决
  GET  /sessions/{sid}                 查看当前持锁集合、等待队列、撤销事务与裁决序号
  GET  /sessions/{sid}/events/{eid}    查看某稳定事件标识的首次裁决
"""

from __future__ import annotations

import json
import os
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .lockmgr import (
    FaultConfig,
    LockError,
    LockManager,
    MAX_EVENTS_DEFAULT,
    MAX_TRANSACTIONS_DEFAULT,
)

DATA_DIR = os.environ.get("DATA_DIR", "/data")
HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "8080"))
MAX_TRANSACTIONS = int(os.environ.get("MAX_TRANSACTIONS", str(MAX_TRANSACTIONS_DEFAULT)))
MAX_EVENTS = int(os.environ.get("MAX_EVENTS", str(MAX_EVENTS_DEFAULT)))


class Registry:
    def __init__(self) -> None:
        self._managers: dict[str, LockManager] = {}
        self._locks: dict[str, threading.Lock] = {}
        self._guard = threading.Lock()

    def _lock_for(self, sid: str) -> threading.Lock:
        with self._guard:
            lk = self._locks.get(sid)
            if lk is None:
                lk = threading.Lock()
                self._locks[sid] = lk
            return lk

    def get(self, sid: str, create: bool = False) -> LockManager | None:
        with self._guard:
            mgr = self._managers.get(sid)
            if mgr is None:
                existed = os.path.exists(os.path.join(DATA_DIR, f"{sid}.json"))
                if create or existed:
                    mgr = LockManager(
                        sid,
                        DATA_DIR,
                        max_transactions=MAX_TRANSACTIONS,
                        max_events=MAX_EVENTS,
                        fault=FaultConfig(
                            failpoint=os.environ.get("FAILPOINT"),
                            recovery_mode=os.environ.get("RECOVERY_MODE", "rollforward"),
                            allow_crash_injection=(
                                os.environ.get("ALLOW_CRASH_INJECTION", "").lower()
                                in ("1", "true", "yes")
                            ),
                        ),
                    )
                    self._managers[sid] = mgr
            return mgr

    def lock(self, sid: str) -> threading.Lock:
        return self._lock_for(sid)


REGISTRY = Registry()

EVENTS_PATH = re.compile(r"^/sessions/([A-Za-z0-9_.\-]+)/events$")
SESSION_PATH = re.compile(r"^/sessions/([A-Za-z0-9_.\-]+)$")
EVENT_PATH = re.compile(r"^/sessions/([A-Za-z0-9_.\-]+)/events/([A-Za-z0-9_.\-]+)$")


class Handler(BaseHTTPRequestHandler):
    server_version = "RadLockMgr/1.0"

    def log_message(self, fmt: str, *args) -> None:  # noqa: A003
        if os.environ.get("QUIET"):
            return
        super().log_message(fmt, *args)

    def _send_json(self, status: int, body: dict) -> None:
        data = json.dumps(body, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _error(self, exc: LockError) -> None:
        self._send_json(
            exc.http_status,
            {"ok": False, "error": exc.code, "message": exc.message},
        )

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            raise LockError("invalid_body", "请求体必须是 JSON 对象", 400)
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise LockError("invalid_body", "请求体不是合法 JSON", 400)
        if not isinstance(data, dict):
            raise LockError("invalid_body", "请求体必须是 JSON 对象", 400)
        return data

    def do_GET(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        if path == "/health":
            self._send_json(200, {"status": "ok", "service": "rad-lock-mgr"})
            return
        m = EVENT_PATH.match(path)
        if m:
            sid, eid = m.group(1), m.group(2)
            mgr = REGISTRY.get(sid)
            if mgr is None:
                self._send_json(404, {"ok": False, "error": "session_not_found",
                                      "message": f"会话 {sid} 不存在"})
                return
            with REGISTRY.lock(sid):
                verdict = mgr.stored_verdict(eid)
            if verdict is None:
                self._send_json(404, {"ok": False, "error": "event_not_found",
                                      "message": f"事件 {eid} 不存在"})
            else:
                self._send_json(200, verdict)
            return
        m = SESSION_PATH.match(path)
        if m:
            sid = m.group(1)
            mgr = REGISTRY.get(sid)
            if mgr is None:
                self._send_json(404, {"ok": False, "error": "session_not_found",
                                      "message": f"会话 {sid} 不存在"})
                return
            with REGISTRY.lock(sid):
                self._send_json(200, mgr.snapshot())
            return
        self._send_json(404, {"ok": False, "error": "not_found", "message": path})

    def do_POST(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        m = EVENTS_PATH.match(path)
        if not m:
            self._send_json(404, {"ok": False, "error": "not_found", "message": path})
            return
        sid = m.group(1)
        try:
            payload = self._read_json()
        except LockError as exc:
            self._error(exc)
            return
        crash = bool(payload.pop("crash", False)) and os.environ.get(
            "ALLOW_CRASH_INJECTION", ""
        ).lower() in ("1", "true", "yes")
        with REGISTRY.lock(sid):
            mgr = REGISTRY.get(sid, create=True)
            try:
                verdict = mgr.submit(payload, crash_this_event=crash)
            except LockError as exc:
                self._error(exc)
                return
        self._send_json(200 if verdict.get("replayed") else 201, verdict)


def main() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    httpd = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"rad-lock-mgr listening on {HOST}:{PORT} (data={DATA_DIR})", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
