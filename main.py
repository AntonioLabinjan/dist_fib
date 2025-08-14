
import argparse
import asyncio
import base64
import hashlib
import json
import os
import signal
import string
import time
from collections import deque, defaultdict
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List

POLL_TIMEOUT_S = 20
HEARTBEAT_INTERVAL_S = 5
WORKER_STALE_S = 15
TASK_ACK_TIMEOUT_S = 15
MAX_RETRIES = 5


def now_s() -> float:
    return time.time()


def gen_id(prefix: str) -> str:
    raw = f"{prefix}:{now_s()}:{os.getpid()}:{os.urandom(8).hex()}".encode()
    return base64.urlsafe_b64encode(hashlib.blake2b(raw, digest_size=10).digest()).decode().rstrip("=")


def stable_task_id(payload: Any) -> str:
    b = json.dumps(payload, sort_keys=True).encode()
    return hashlib.blake2b(b, digest_size=12).hexdigest()



@dataclass
class Task:
    task_id: str
    payload: Any
    status: str = "pending"  # pending | assigned | done | failed
    assigned_to: Optional[str] = None
    updated_at: float = field(default_factory=now_s)
    attempts: int = 0
    result: Optional[Any] = None


class InMemoryState:
    def __init__(self):
        self.tasks: Dict[str, Task] = {}
        self.pending: deque[str] = deque()
        self.inflight_by_worker: Dict[str, set[str]] = defaultdict(set)
        self.workers: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def register_worker(self, name: str) -> str:
        async with self._lock:
            worker_id = gen_id("w")
            self.workers[worker_id] = {
                "name": name,
                "last_seen": now_s(),
                "since": now_s(),
            }
            return worker_id

    async def heartbeat(self, worker_id: str) -> bool:
        async with self._lock:
            w = self.workers.get(worker_id)
            if not w:
                return False
            w["last_seen"] = now_s()
            return True

    async def submit_tasks(self, payloads: List[Any]) -> List[str]:
        ids = []
        async with self._lock:
            for p in payloads:
                tid = stable_task_id(p)
                t = self.tasks.get(tid)
                if t is None or t.status in {"failed"}:
                    self.tasks[tid] = Task(task_id=tid, payload=p)
                    self.pending.append(tid)
                ids.append(tid)
        return ids

    async def next_task(self, worker_id: str) -> Optional[Task]:
        async with self._lock:
            # Ako je worker nepoznat, odbij
            if worker_id not in self.workers:
                return None
            # Uzmi pending task
            while self.pending:
                tid = self.pending.popleft()
                t = self.tasks.get(tid)
                if not t or t.status in {"done"}:
                    continue
                t.status = "assigned"
                t.assigned_to = worker_id
                t.updated_at = now_s()
                t.attempts += 1
                self.inflight_by_worker[worker_id].add(tid)
                return t
            return None

    async def ack_result(self, worker_id: str, task_id: str, result: Any, status: str) -> bool:
        async with self._lock:
            t = self.tasks.get(task_id)
            if not t:
                return False
            # Idempotentno: ako je već done, ok
            if t.status == "done":
                return True
            t.result = result
            t.status = status
            t.updated_at = now_s()
            # makni iz inflight-a
            self.inflight_by_worker[worker_id].discard(task_id)
            return True

    async def requeue_stale(self):
        async with self._lock:
            now = now_s()
            # Requeue taskova kojima je istekao ACK timeout
            for tid, t in list(self.tasks.items()):
                if t.status == "assigned" and (now - t.updated_at) > TASK_ACK_TIMEOUT_S:
                    if t.attempts < MAX_RETRIES:
                        t.status = "pending"
                        t.assigned_to = None
                        t.updated_at = now
                        self.pending.appendleft(tid)
                    else:
                        t.status = "failed"
                        t.updated_at = now
            # Detektiraj stale workere i requeue njihov inflight
            for wid, meta in list(self.workers.items()):
                if (now - meta["last_seen"]) > WORKER_STALE_S:
                    for tid in list(self.inflight_by_worker[wid]):
                        t = self.tasks.get(tid)
                        if t and t.status == "assigned":
                            if t.attempts < MAX_RETRIES:
                                t.status = "pending"
                                t.assigned_to = None
                                t.updated_at = now
                                self.pending.appendleft(tid)
                            else:
                                t.status = "failed"
                                t.updated_at = now
                    self.inflight_by_worker[wid].clear()

    async def stats(self) -> Dict[str, Any]:
        async with self._lock:
            counts = defaultdict(int)
            for t in self.tasks.values():
                counts[t.status] += 1
            return {
                "workers": {
                    wid: {
                        "name": meta["name"],
                        "last_seen": meta["last_seen"],
                        "inflight": len(self.inflight_by_worker.get(wid, set())),
                    }
                    for wid, meta in self.workers.items()
                },
                "tasks": counts,
                "total_tasks": len(self.tasks),
                "pending_queue": len(self.pending),
            }


async def run_server(host: str, port: int):
    from fastapi import FastAPI, HTTPException
    from fastapi.responses import JSONResponse
    import uvicorn

    state = InMemoryState()
    app = FastAPI(title="MiniDist Server")

    @app.post("/register")
    async def register(body: Dict[str, Any]):
        wid = await state.register_worker(body.get("name", "worker"))
        return {"worker_id": wid, "poll": POLL_TIMEOUT_S, "heartbeat": HEARTBEAT_INTERVAL_S}

    @app.post("/heartbeat")
    async def heartbeat(body: Dict[str, Any]):
        ok = await state.heartbeat(body.get("worker_id", ""))
        if not ok:
            raise HTTPException(404, "unknown worker")
        return {"ok": True}

    @app.post("/submit")
    async def submit(body: Dict[str, Any]):
        payloads = body.get("payloads", [])
        ids = await state.submit_tasks(payloads)
        return {"submitted": ids}

    @app.get("/next_task")
    async def next_task(worker_id: str):
        # Long-polling: čekaj do POLL_TIMEOUT_S na task
        deadline = now_s() + POLL_TIMEOUT_S
        while now_s() < deadline:
            t = await state.next_task(worker_id)
            if t:
                return JSONResponse({
                    "task_id": t.task_id,
                    "payload": t.payload,
                    "attempts": t.attempts,
                })
            await asyncio.sleep(0.2)
        return JSONResponse(status_code=204, content=None)

    @app.post("/result")
    async def result(body: Dict[str, Any]):
        worker_id = body.get("worker_id")
        task_id = body.get("task_id")
        status = body.get("status", "done")
        ok = await state.ack_result(worker_id, task_id, body.get("result"), status)
        if not ok:
            raise HTTPException(404, "unknown task or worker")
        return {"ok": True}

    @app.get("/stats")
    async def stats():
        return await state.stats()

    async def janitor():
        while True:
            await state.requeue_stale()
            await asyncio.sleep(1)

    @app.on_event("startup")
    async def _startup():
        asyncio.create_task(janitor())

    config = uvicorn.Config(app, host=host, port=port, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()


# =====================
# WORKER
# =====================

async def fib_sum(n: int) -> int:
    a, b = 0, 1
    s = 0
    for _ in range(n):
        s += a
        a, b = b, a + b
    await asyncio.sleep(0)  # yield
    return s


def compute(payload: Any) -> Any:
    if isinstance(payload, dict) and payload.get("type") == "fibsum":
        n = int(payload.get("n", 0))
        return asyncio.run(fib_sum(n))
    return None


async def worker_loop(server: str, name: str):
    import httpx

    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(f"{server}/register", json={"name": name})
        r.raise_for_status()
        worker_id = r.json()["worker_id"]

        async def heartbeat_loop():
            while True:
                try:
                    await client.post(f"{server}/heartbeat", json={"worker_id": worker_id})
                except Exception:
                    pass
                await asyncio.sleep(HEARTBEAT_INTERVAL_S)

        asyncio.create_task(heartbeat_loop())

        while True:
            try:
                r = await client.get(f"{server}/next_task", params={"worker_id": worker_id})
                if r.status_code == 204:
                    await asyncio.sleep(0.5)
                    continue
                t = r.json()
                tid = t["task_id"]
                payload = t["payload"]
                try:
                    result = compute(payload)
                    status = "done"
                except Exception as e:
                    result = {"error": str(e)}
                    status = "failed"
                await client.post(f"{server}/result", json={
                    "worker_id": worker_id,
                    "task_id": tid,
                    "result": result,
                    "status": status,
                })
            except Exception:
                await asyncio.sleep(1)


# =====================
# KLIJENT & MONITOR
# =====================

async def submit_n(server: str, n: int):
    import httpx
    payloads = [{"type": "fibsum", "n": i} for i in range(20, 20 + n)]
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(f"{server}/submit", json={"payloads": payloads})
        r.raise_for_status()
        ids = r.json()["submitted"]
        print("submitted:", len(ids))
        print("first ids:", ids[:5])


async def monitor_loop(server: str):
    import httpx
    try:
        while True:
            async with httpx.AsyncClient(timeout=10.0) as client:
                r = await client.get(f"{server}/stats")
                s = r.json()
                print(json.dumps(s, indent=2))
            await asyncio.sleep(2)
    except KeyboardInterrupt:
        pass


# =====================
# MAIN
# =====================

def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="mode", required=True)

    ps = sub.add_parser("server")
    ps.add_argument("--host", default="127.0.0.1")
    ps.add_argument("--port", type=int, default=8000)

    pw = sub.add_parser("worker")
    pw.add_argument("--server", default="http://127.0.0.1:8000")
    pw.add_argument("--name", default="worker")

    pc = sub.add_parser("submit")
    pc.add_argument("--server", default="http://127.0.0.1:8000")
    pc.add_argument("--n", type=int, default=20)

    pm = sub.add_parser("monitor")
    pm.add_argument("--server", default="http://127.0.0.1:8000")

    args = parser.parse_args()

    if args.mode == "server":
        asyncio.run(run_server(args.host, args.port))
    elif args.mode == "worker":
        asyncio.run(worker_loop(args.server, args.name))
    elif args.mode == "submit":
        asyncio.run(submit_n(args.server, args.n))
    elif args.mode == "monitor":
        asyncio.run(monitor_loop(args.server))


if __name__ == "__main__":
    main()
