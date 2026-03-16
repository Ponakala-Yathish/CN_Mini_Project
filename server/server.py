"""
server.py  -  Central Job Queue Server  (SSL/TLS enabled)
==========================================================
* Wraps every accepted TCP connection with SSL
* Accepts connections from both clients and workers
* Maintains a thread-safe priority job queue
* Assigns jobs to workers, tracks heartbeats, re-queues on worker failure
* Logs performance stats every 10 seconds

Run:  python3 server.py
"""

import socket
import ssl
import threading
import queue
import time
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from common import (
    SERVER_HOST, SERVER_PORT,
    WORKER_TIMEOUT_SECONDS, SOCKET_TIMEOUT,
    MsgType, JobState,
    make_server_ssl_context,
    send_msg, recv_msg, setup_logger, make_id
)

log = setup_logger("Server")


# ═══════════════════════════════════════════════════════════════════════════
#  Data Structures
# ═══════════════════════════════════════════════════════════════════════════

class JobRecord:
    """Holds all information about one job."""
    def __init__(self, job_id, payload, priority=5):
        self.job_id       = job_id
        self.payload      = payload
        self.priority     = priority
        self.state        = JobState.PENDING
        self.worker_id    = None
        self.submitted_at = time.time()
        self.assigned_at  = None
        self.completed_at = None
        self.result       = None

    def __lt__(self, other):
        if self.priority != other.priority:
            return self.priority < other.priority
        return self.submitted_at < other.submitted_at


class WorkerTracker:
    """Tracks connected workers and their last heartbeat times."""
    def __init__(self):
        self._lock    = threading.Lock()
        self._workers = {}  # worker_id → {last_hb, job_id}

    def register(self, worker_id):
        with self._lock:
            self._workers[worker_id] = {"last_hb": time.time(), "job_id": None}
        log.info(f"Worker registered: {worker_id}")

    def unregister(self, worker_id):
        with self._lock:
            self._workers.pop(worker_id, None)
        log.info(f"Worker unregistered: {worker_id}")

    def heartbeat(self, worker_id):
        with self._lock:
            if worker_id in self._workers:
                self._workers[worker_id]["last_hb"] = time.time()

    def assign_job(self, worker_id, job_id):
        with self._lock:
            if worker_id in self._workers:
                self._workers[worker_id]["job_id"] = job_id

    def clear_job(self, worker_id):
        with self._lock:
            if worker_id in self._workers:
                self._workers[worker_id]["job_id"] = None

    def get_timed_out(self):
        now = time.time()
        result = []
        with self._lock:
            for wid, info in list(self._workers.items()):
                if now - info["last_hb"] > WORKER_TIMEOUT_SECONDS:
                    result.append((wid, info["job_id"]))
        return result

    def count(self):
        with self._lock:
            return len(self._workers)


# ═══════════════════════════════════════════════════════════════════════════
#  Server
# ═══════════════════════════════════════════════════════════════════════════

class JobQueueServer:
    def __init__(self, host=SERVER_HOST, port=SERVER_PORT):
        self.host = host
        self.port = port

        self._job_queue  = queue.PriorityQueue()
        self._all_jobs   = {}
        self._jobs_lock  = threading.Lock()
        self._workers    = WorkerTracker()

        self._submitted  = 0
        self._completed  = 0
        self._failed     = 0
        self._stats_lock = threading.Lock()
        self._running    = False

    # ── Queue helpers ───────────────────────────────────────────────────────

    def _enqueue(self, record):
        self._job_queue.put((record.priority, record))
        with self._jobs_lock:
            self._all_jobs[record.job_id] = record

    def _try_dequeue(self):
        try:
            _, record = self._job_queue.get_nowait()
            return record
        except queue.Empty:
            return None

    def _requeue(self, job_id):
        with self._jobs_lock:
            record = self._all_jobs.get(job_id)
        if record and record.state == JobState.ASSIGNED:
            record.state      = JobState.PENDING
            record.worker_id  = None
            record.assigned_at = None
            self._job_queue.put((record.priority, record))
            log.warning(f"Re-queued job {job_id}")

    # ── Connection dispatcher ───────────────────────────────────────────────

    def _handle_connection(self, raw_conn, addr, ssl_ctx):
        # Wrap the raw socket with SSL here (server-side handshake)
        try:
            conn = ssl_ctx.wrap_socket(raw_conn, server_side=True)
        except ssl.SSLError as e:
            # Edge case: client connected without SSL (plain TCP), or wrong cert
            log.warning(f"SSL handshake FAILED from {addr}: {e}")
            log.warning(f"  → Client may not have server.crt, or is not using SSL")
            raw_conn.close()
            return
        except OSError as e:
            # Edge case: client disconnected during handshake
            log.warning(f"Client {addr} disconnected during SSL handshake: {e}")
            raw_conn.close()
            return

        log.info(f"SSL connection from {addr}  cipher={conn.cipher()[0]}")
        conn.settimeout(SOCKET_TIMEOUT)

        try:
            msg = recv_msg(conn)
            if msg is None:
                return
            mtype = msg.get("type")

            if mtype in (MsgType.SUBMIT, MsgType.QUERY_STATUS):
                self._handle_client(conn, addr, msg)

            elif mtype == MsgType.REQUEST_JOB:
                worker_id = msg.get("worker_id") or make_id("w")
                self._workers.register(worker_id)
                self._handle_worker(conn, addr, worker_id, msg)
                self._workers.unregister(worker_id)

            else:
                send_msg(conn, {"type": MsgType.ERROR, "detail": "Unknown first message"})

        except Exception as e:
            log.error(f"Handler error from {addr}: {e}")
        finally:
            conn.close()

    # ── Client handler ──────────────────────────────────────────────────────

    def _handle_client(self, conn, addr, first_msg):
        log.info(f"Client connected from {addr}")
        msg = first_msg

        while msg is not None:
            mtype = msg.get("type")

            if mtype == MsgType.SUBMIT:
                job_id   = msg.get("job_id") or make_id("job")
                payload  = msg.get("payload", "")
                priority = int(msg.get("priority", 5))

                # ── Edge case: empty payload ───────────────────────────────
                if not payload.strip():
                    send_msg(conn, {"type": MsgType.ERROR,
                                    "detail": "Payload cannot be empty"})
                    msg = recv_msg(conn)
                    continue

                # ── Edge case: payload too long ────────────────────────────
                if len(payload) > 500:
                    send_msg(conn, {"type": MsgType.ERROR,
                                    "detail": f"Payload too long ({len(payload)} chars, max 500)"})
                    msg = recv_msg(conn)
                    continue

                # ── Edge case: invalid priority ────────────────────────────
                try:
                    priority = int(priority)
                    if not (1 <= priority <= 10):
                        priority = 5   # silently clamp to default
                except (ValueError, TypeError):
                    priority = 5

                with self._jobs_lock:
                    if job_id in self._all_jobs:
                        send_msg(conn, {"type": MsgType.SUBMIT_ACK,
                                        "job_id": job_id,
                                        "status": "DUPLICATE"})
                        msg = recv_msg(conn)
                        continue

                record = JobRecord(job_id, payload, priority)
                self._enqueue(record)

                with self._stats_lock:
                    self._submitted += 1

                log.info(f"Job queued: {job_id}  payload='{payload}'")
                ok = send_msg(conn, {"type":   MsgType.SUBMIT_ACK,
                                     "job_id": job_id,
                                     "status": JobState.PENDING})
                if not ok:
                    break

            elif mtype == MsgType.QUERY_STATUS:
                job_id = msg.get("job_id", "")
                with self._jobs_lock:
                    record = self._all_jobs.get(job_id)
                if record:
                    send_msg(conn, {"type":   MsgType.STATUS_RESPONSE,
                                    "job_id": job_id,
                                    "state":  record.state,
                                    "result": record.result})
                else:
                    send_msg(conn, {"type":   MsgType.STATUS_RESPONSE,
                                    "job_id": job_id,
                                    "state":  "NOT_FOUND"})
            else:
                send_msg(conn, {"type": MsgType.ERROR, "detail": f"Unexpected: {mtype}"})

            msg = recv_msg(conn)

        log.info(f"Client {addr} disconnected")

    # ── Worker handler ──────────────────────────────────────────────────────

    def _handle_worker(self, conn, addr, worker_id, first_msg):
        log.info(f"Worker connected: {worker_id} from {addr}")
        msg = first_msg

        while msg is not None:
            mtype = msg.get("type")

            if mtype == MsgType.REQUEST_JOB:
                record = self._try_dequeue()
                if record is None:
                    send_msg(conn, {"type": MsgType.NO_JOB})
                else:
                    record.state       = JobState.ASSIGNED
                    record.worker_id   = worker_id
                    record.assigned_at = time.time()
                    self._workers.assign_job(worker_id, record.job_id)
                    log.info(f"Assigned job {record.job_id} → worker {worker_id}")
                    ok = send_msg(conn, {"type":    MsgType.JOB_ASSIGN,
                                         "job_id":  record.job_id,
                                         "payload": record.payload})
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
                    log.info(f"Job COMPLETED: {job_id} by {worker_id}  time={elapsed}s")
                self._workers.clear_job(worker_id)
                with self._stats_lock:
                    self._completed += 1
                send_msg(conn, {"type": MsgType.OK, "detail": "recorded"})

            elif mtype == MsgType.JOB_FAILED:
                job_id = msg.get("job_id", "")
                reason = msg.get("reason", "unknown")
                log.warning(f"Job FAILED: {job_id} reason={reason}")
                self._workers.clear_job(worker_id)
                with self._stats_lock:
                    self._failed += 1
                self._requeue(job_id)
                send_msg(conn, {"type": MsgType.OK, "detail": "re-queued"})

            elif mtype == MsgType.HEARTBEAT:
                self._workers.heartbeat(worker_id)

            else:
                send_msg(conn, {"type": MsgType.ERROR, "detail": f"Unknown: {mtype}"})

            msg = recv_msg(conn)

        log.info(f"Worker {worker_id} disconnected")
        # Re-queue any job the worker was holding
        with self._jobs_lock:
            for jid, rec in self._all_jobs.items():
                if rec.worker_id == worker_id and rec.state == JobState.ASSIGNED:
                    self._requeue(jid)

    # ── Watchdog ────────────────────────────────────────────────────────────

    def _watchdog(self):
        while self._running:
            time.sleep(5)
            for worker_id, job_id in self._workers.get_timed_out():
                log.warning(f"Worker {worker_id} timed out")
                self._workers.unregister(worker_id)
                if job_id:
                    self._requeue(job_id)

    # ── Stats reporter ──────────────────────────────────────────────────────

    def _stats_reporter(self):
        while self._running:
            time.sleep(10)
            with self._stats_lock:
                s, c, f = self._submitted, self._completed, self._failed
            log.info(f"[STATS] submitted={s}  completed={c}  failed={f}"
                     f"  queue={self._job_queue.qsize()}  workers={self._workers.count()}")

    # ── Start ───────────────────────────────────────────────────────────────

    def start(self):
        self._running = True
        ssl_ctx = make_server_ssl_context()

        threading.Thread(target=self._watchdog,       daemon=True).start()
        threading.Thread(target=self._stats_reporter, daemon=True).start()

        raw_srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        raw_srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        raw_srv.bind((self.host, self.port))
        raw_srv.listen(50)
        log.info(f"Server listening on {self.host}:{self.port}  [SSL ON]")

        try:
            while True:
                raw_conn, addr = raw_srv.accept()
                threading.Thread(
                    target=self._handle_connection,
                    args=(raw_conn, addr, ssl_ctx),
                    daemon=True
                ).start()
        except KeyboardInterrupt:
            log.info("Server shutting down...")
        finally:
            self._running = False
            raw_srv.close()


if __name__ == "__main__":
    JobQueueServer().start()