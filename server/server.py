"""
server.py  –  Central Job Queue Server
=======================================
* Accepts TCP connections from both clients and workers
* Maintains a thread-safe priority job queue
* Assigns jobs to workers, tracks heartbeats, re-queues on worker failure
* Logs performance stats every 10 seconds

Run:  python server.py
"""

import socket
import threading
import queue
import time
import logging
import sys
import os

# ── make sure common.py is importable ──────────────────────────────────────
sys.path.insert(0, os.path.dirname(__file__))
from common import (
    SERVER_HOST, SERVER_PORT,
    WORKER_TIMEOUT_SECONDS, WORKER_HEARTBEAT_INTERVAL, SOCKET_TIMEOUT,
    MsgType, JobState,
    send_msg, recv_msg, setup_logger, make_id
)

log = setup_logger("Server")


# ═══════════════════════════════════════════════════════════════════════════
#  Data Structures (all protected by their own locks)
# ═══════════════════════════════════════════════════════════════════════════

class JobRecord:
    """Holds full information about a single job."""
    def __init__(self, job_id: str, payload: str, priority: int = 5):
        self.job_id    = job_id
        self.payload   = payload
        self.priority  = priority          # lower number = higher priority
        self.state     = JobState.PENDING
        self.worker_id = None
        self.submitted_at = time.time()
        self.assigned_at  = None
        self.completed_at = None
        self.result       = None

    def to_dict(self):
        return {
            "job_id":    self.job_id,
            "payload":   self.payload,
            "priority":  self.priority,
            "state":     self.state,
            "worker_id": self.worker_id,
        }

    # Allow PriorityQueue comparison by priority then submit time
    def __lt__(self, other):
        if self.priority != other.priority:
            return self.priority < other.priority
        return self.submitted_at < other.submitted_at


class WorkerTracker:
    """Tracks connected workers and their last heartbeat times."""
    def __init__(self):
        self._lock    = threading.Lock()
        self._workers = {}   # worker_id → {"sock": sock, "last_hb": float, "job_id": str|None}

    def register(self, worker_id: str, sock: socket.socket):
        with self._lock:
            self._workers[worker_id] = {
                "sock":    sock,
                "last_hb": time.time(),
                "job_id":  None,
            }
        log.info(f"Worker registered: {worker_id}")

    def unregister(self, worker_id: str):
        with self._lock:
            self._workers.pop(worker_id, None)
        log.info(f"Worker unregistered: {worker_id}")

    def heartbeat(self, worker_id: str):
        with self._lock:
            if worker_id in self._workers:
                self._workers[worker_id]["last_hb"] = time.time()

    def assign_job(self, worker_id: str, job_id: str):
        with self._lock:
            if worker_id in self._workers:
                self._workers[worker_id]["job_id"] = job_id

    def clear_job(self, worker_id: str):
        with self._lock:
            if worker_id in self._workers:
                self._workers[worker_id]["job_id"] = None

    def get_timed_out(self) -> list:
        """Return list of (worker_id, job_id) for workers that timed out."""
        now = time.time()
        timed_out = []
        with self._lock:
            for wid, info in list(self._workers.items()):
                if now - info["last_hb"] > WORKER_TIMEOUT_SECONDS:
                    timed_out.append((wid, info["job_id"]))
        return timed_out

    def count(self) -> int:
        with self._lock:
            return len(self._workers)


# ═══════════════════════════════════════════════════════════════════════════
#  Server Core
# ═══════════════════════════════════════════════════════════════════════════

class JobQueueServer:
    def __init__(self, host=SERVER_HOST, port=SERVER_PORT):
        self.host = host
        self.port = port

        # Job storage
        self._job_queue   = queue.PriorityQueue()   # (priority, JobRecord)
        self._all_jobs    = {}                       # job_id → JobRecord
        self._jobs_lock   = threading.Lock()

        # Worker tracking
        self._workers     = WorkerTracker()

        # Stats
        self._submitted   = 0
        self._completed   = 0
        self._failed      = 0
        self._stats_lock  = threading.Lock()

        self._running     = False

    # ── Queue helpers ───────────────────────────────────────────────────────

    def _enqueue(self, record: JobRecord):
        self._job_queue.put((record.priority, record))
        with self._jobs_lock:
            self._all_jobs[record.job_id] = record

    def _try_dequeue(self):
        """Non-blocking dequeue. Returns JobRecord or None."""
        try:
            _, record = self._job_queue.get_nowait()
            return record
        except queue.Empty:
            return None

    def _requeue(self, job_id: str):
        """Put an assigned-but-unfinished job back to PENDING."""
        with self._jobs_lock:
            record = self._all_jobs.get(job_id)
        if record and record.state == JobState.ASSIGNED:
            record.state     = JobState.PENDING
            record.worker_id = None
            record.assigned_at = None
            self._job_queue.put((record.priority, record))
            log.warning(f"Re-queued job {job_id}")

    # ── Connection handler dispatcher ───────────────────────────────────────

    def _handle_connection(self, conn: socket.socket, addr):
        """
        First message decides if this connection is CLIENT or WORKER.
        """
        conn.settimeout(SOCKET_TIMEOUT if not hasattr(conn, '_timeout') else None)
        try:
            msg = recv_msg(conn)
            if msg is None:
                return
            mtype = msg.get("type")

            if mtype == MsgType.SUBMIT or mtype == MsgType.QUERY_STATUS:
                self._handle_client(conn, addr, msg)

            elif mtype == MsgType.REQUEST_JOB:
                worker_id = msg.get("worker_id", make_id("w"))
                self._workers.register(worker_id, conn)
                self._handle_worker(conn, addr, worker_id, msg)
                self._workers.unregister(worker_id)

            else:
                send_msg(conn, {"type": MsgType.ERROR, "detail": "Unknown first message"})
        except Exception as e:
            log.error(f"Connection handler error from {addr}: {e}")
        finally:
            conn.close()

    # ── Client handler ──────────────────────────────────────────────────────

    def _handle_client(self, conn: socket.socket, addr, first_msg: dict):
        log.info(f"Client connected from {addr}")
        msg = first_msg
        while msg is not None:
            mtype = msg.get("type")

            if mtype == MsgType.SUBMIT:
                job_id   = msg.get("job_id") or make_id("job")
                payload  = msg.get("payload", "")
                priority = int(msg.get("priority", 5))

                # Deduplicate: ignore if job_id already exists
                with self._jobs_lock:
                    if job_id in self._all_jobs:
                        send_msg(conn, {
                            "type":   MsgType.SUBMIT_ACK,
                            "job_id": job_id,
                            "status": "DUPLICATE – already queued",
                        })
                        msg = recv_msg(conn)
                        continue

                record = JobRecord(job_id, payload, priority)
                self._enqueue(record)

                with self._stats_lock:
                    self._submitted += 1

                log.info(f"Job queued: {job_id}  payload='{payload}'  priority={priority}")
                ok = send_msg(conn, {
                    "type":   MsgType.SUBMIT_ACK,
                    "job_id": job_id,
                    "status": JobState.PENDING,
                })
                if not ok:
                    break

            elif mtype == MsgType.QUERY_STATUS:
                job_id = msg.get("job_id", "")
                with self._jobs_lock:
                    record = self._all_jobs.get(job_id)
                if record:
                    send_msg(conn, {
                        "type":   MsgType.STATUS_RESPONSE,
                        "job_id": job_id,
                        "state":  record.state,
                        "result": record.result,
                    })
                else:
                    send_msg(conn, {
                        "type":   MsgType.STATUS_RESPONSE,
                        "job_id": job_id,
                        "state":  "NOT_FOUND",
                    })

            else:
                send_msg(conn, {"type": MsgType.ERROR, "detail": f"Unexpected type: {mtype}"})

            msg = recv_msg(conn)

        log.info(f"Client {addr} disconnected")

    # ── Worker handler ──────────────────────────────────────────────────────

    def _handle_worker(self, conn: socket.socket, addr, worker_id: str, first_msg: dict):
        log.info(f"Worker connected: {worker_id} from {addr}")
        msg = first_msg

        while msg is not None:
            mtype = msg.get("type")

            if mtype == MsgType.REQUEST_JOB:
                record = self._try_dequeue()
                if record is None:
                    send_msg(conn, {"type": MsgType.NO_JOB})
                else:
                    record.state      = JobState.ASSIGNED
                    record.worker_id  = worker_id
                    record.assigned_at = time.time()
                    self._workers.assign_job(worker_id, record.job_id)

                    log.info(f"Assigned job {record.job_id} → worker {worker_id}")
                    ok = send_msg(conn, {
                        "type":    MsgType.JOB_ASSIGN,
                        "job_id":  record.job_id,
                        "payload": record.payload,
                    })
                    if not ok:
                        self._requeue(record.job_id)
                        break

            elif mtype == MsgType.JOB_DONE:
                job_id = msg.get("job_id", "")
                result = msg.get("result", "")
                with self._jobs_lock:
                    record = self._all_jobs.get(job_id)
                if record:
                    record.state        = JobState.COMPLETED
                    record.result       = result
                    record.completed_at = time.time()
                    elapsed = round(record.completed_at - record.submitted_at, 3)
                    log.info(f"Job COMPLETED: {job_id} by {worker_id}  total_time={elapsed}s")
                self._workers.clear_job(worker_id)
                with self._stats_lock:
                    self._completed += 1
                send_msg(conn, {"type": MsgType.OK, "detail": "job recorded"})

            elif mtype == MsgType.JOB_FAILED:
                job_id = msg.get("job_id", "")
                reason = msg.get("reason", "unknown")
                log.warning(f"Job FAILED: {job_id} by {worker_id}  reason={reason}")
                self._workers.clear_job(worker_id)
                with self._stats_lock:
                    self._failed += 1
                # Re-queue so another worker can attempt it
                self._requeue(job_id)
                send_msg(conn, {"type": MsgType.OK, "detail": "job re-queued"})

            elif mtype == MsgType.HEARTBEAT:
                self._workers.heartbeat(worker_id)
                # no reply needed – worker doesn't wait for one

            else:
                send_msg(conn, {"type": MsgType.ERROR, "detail": f"Unknown type: {mtype}"})

            msg = recv_msg(conn)

        log.info(f"Worker {worker_id} disconnected")
        # If worker held a job, re-queue it
        with self._jobs_lock:
            for jid, rec in self._all_jobs.items():
                if rec.worker_id == worker_id and rec.state == JobState.ASSIGNED:
                    self._requeue(jid)

    # ── Background: watchdog for timed-out workers ──────────────────────────

    def _watchdog(self):
        while self._running:
            time.sleep(5)
            timed_out = self._workers.get_timed_out()
            for worker_id, job_id in timed_out:
                log.warning(f"Worker {worker_id} timed out (no heartbeat)")
                self._workers.unregister(worker_id)
                if job_id:
                    self._requeue(job_id)

    # ── Background: periodic stats ──────────────────────────────────────────

    def _stats_reporter(self):
        while self._running:
            time.sleep(10)
            with self._stats_lock:
                s, c, f = self._submitted, self._completed, self._failed
            q_depth = self._job_queue.qsize()
            log.info(
                f"[STATS]  submitted={s}  completed={c}  failed={f}"
                f"  queue_depth={q_depth}  workers={self._workers.count()}"
            )

    # ── Main start ──────────────────────────────────────────────────────────

    def start(self):
        self._running = True

        # Start background threads
        threading.Thread(target=self._watchdog,       daemon=True, name="watchdog").start()
        threading.Thread(target=self._stats_reporter, daemon=True, name="stats").start()

        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))
        srv.listen(50)
        log.info(f"Server listening on {self.host}:{self.port}")

        try:
            while True:
                conn, addr = srv.accept()
                t = threading.Thread(
                    target=self._handle_connection,
                    args=(conn, addr),
                    daemon=True
                )
                t.start()
        except KeyboardInterrupt:
            log.info("Server shutting down …")
        finally:
            self._running = False
            srv.close()


# ═══════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    JobQueueServer().start()
