"""
worker.py  -  Job Execution Worker  (SSL/TLS enabled)
======================================================
* Connects to server over SSL
* Requests jobs, executes them using jobs.py, reports back
* Sends heartbeat every 3 seconds so server knows it's alive
* Auto-reconnects if connection drops

Run:
  python3 worker.py           auto ID
  python3 worker.py w001      named worker
"""

import socket
import ssl
import threading
import time
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from common import (
    SERVER_HOST, SERVER_PORT,
    WORKER_HEARTBEAT_INTERVAL, NO_JOB_WAIT_SECONDS,
    MsgType,
    make_client_ssl_context,
    send_msg, recv_msg, setup_logger, make_id, Timer
)
from jobs import run_job


class Worker:
    def __init__(self, worker_id=None):
        self.worker_id  = worker_id or make_id("w")
        self.log        = setup_logger(f"Worker-{self.worker_id}")
        self._sock      = None
        self._sock_lock = threading.Lock()
        self._running   = False
        self.jobs_done  = 0
        self.jobs_failed= 0

    # ── SSL connect ─────────────────────────────────────────────────────────

    def _connect(self, retries=10):
        # ── Edge case: check cert file exists ─────────────────────────────
        from common import CERT_FILE
        import os
        if not os.path.exists(CERT_FILE):
            self.log.error(f"SSL ERROR: Certificate not found: {CERT_FILE}")
            self.log.error("Put server.crt in the same folder as worker.py")
            return False

        ssl_ctx = make_client_ssl_context()
        for attempt in range(1, retries + 1):
            try:
                raw = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                raw.settimeout(15)
                raw.connect((SERVER_HOST, SERVER_PORT))

                # ── Edge case: SSL handshake failure ──────────────────────
                ssl_sock = ssl_ctx.wrap_socket(raw, server_hostname=SERVER_HOST)
                with self._sock_lock:
                    self._sock = ssl_sock
                self.log.info(f"SSL connected  cipher={ssl_sock.cipher()[0]}")
                return True

            except ConnectionRefusedError:
                self.log.warning(f"Server not reachable at {SERVER_HOST}:{SERVER_PORT} "
                                 f"(attempt {attempt}/{retries})")
                time.sleep(min(2 * attempt, 10))

            except ssl.SSLError as e:
                # Server may not be using SSL, or cert mismatch
                self.log.error(f"SSL HANDSHAKE FAILED: {e}")
                self.log.error("Check server.crt matches the server certificate")
                return False

            except OSError as e:
                self.log.warning(f"Network error (attempt {attempt}/{retries}): {e}")
                time.sleep(min(2 * attempt, 10))

        self.log.error(f"Cannot connect after {retries} attempts")
        return False

    def _disconnect(self):
        with self._sock_lock:
            if self._sock:
                try:
                    self._sock.close()
                except OSError:
                    pass
                self._sock = None

    # ── Thread-safe send ────────────────────────────────────────────────────

    def _send(self, msg):
        with self._sock_lock:
            if self._sock is None:
                return False
            return send_msg(self._sock, msg)

    def _recv(self):
        return recv_msg(self._sock)

    # ── Heartbeat thread ────────────────────────────────────────────────────

    def _heartbeat_loop(self):
        while self._running:
            time.sleep(WORKER_HEARTBEAT_INTERVAL)
            if not self._running:
                break
            ok = self._send({"type": MsgType.HEARTBEAT, "worker_id": self.worker_id})
            if not ok:
                self.log.warning("Heartbeat failed")

    # ── Main work loop ──────────────────────────────────────────────────────

    def _work_loop(self):
        while self._running:

            # Request a job
            ok = self._send({"type": MsgType.REQUEST_JOB, "worker_id": self.worker_id})
            if not ok:
                self.log.warning("Lost connection — reconnecting...")
                self._disconnect()
                if not self._connect():
                    break
                continue

            response = self._recv()
            if response is None:
                self.log.warning("No response — reconnecting...")
                self._disconnect()
                if not self._connect():
                    break
                continue

            rtype = response.get("type")

            if rtype == MsgType.NO_JOB:
                self.log.info(f"Queue empty — waiting {NO_JOB_WAIT_SECONDS}s...")
                time.sleep(NO_JOB_WAIT_SECONDS)
                continue

            if rtype == MsgType.JOB_ASSIGN:
                job_id  = response.get("job_id", "?")
                payload = response.get("payload", "")
                self.log.info(f"Got job {job_id}  payload='{payload}'")

                # ── Edge case: empty payload from server ───────────────────
                if not payload.strip():
                    self.log.warning(f"Job {job_id} has empty payload — skipping")
                    self._send({"type": MsgType.JOB_FAILED,
                                "job_id": job_id,
                                "worker_id": self.worker_id,
                                "reason": "Empty payload received"})
                    continue

                timer = Timer()
                try:
                    result  = run_job(payload)
                    elapsed = timer.elapsed()
                    self.jobs_done += 1
                    self.log.info(f"Job {job_id} done in {elapsed}s  →  '{result}'")

                    ok = self._send({
                        "type":      MsgType.JOB_DONE,
                        "job_id":    job_id,
                        "worker_id": self.worker_id,
                        "result":    result,
                    })
                    if ok:
                        ack = self._recv()
                        if ack:
                            self.log.info(f"Server ACK: {ack.get('detail','')}")

                except Exception as exc:
                    self.jobs_failed += 1
                    self.log.error(f"Job {job_id} raised exception: {exc}")
                    self._send({
                        "type":      MsgType.JOB_FAILED,
                        "job_id":    job_id,
                        "worker_id": self.worker_id,
                        "reason":    str(exc),
                    })

    # ── Start ───────────────────────────────────────────────────────────────

    def start(self):
        self.log.info(f"Worker starting  id={self.worker_id}  [SSL ON]")
        self._running = True

        if not self._connect():
            self.log.error("Cannot connect. Exiting.")
            return

        threading.Thread(target=self._heartbeat_loop, daemon=True).start()

        try:
            self._work_loop()
        except KeyboardInterrupt:
            self.log.info("Stopped by user")
        finally:
            self._running = False
            self._disconnect()
            self.log.info(f"Done.  jobs_done={self.jobs_done}  jobs_failed={self.jobs_failed}")


if __name__ == "__main__":
    worker_id = sys.argv[1] if len(sys.argv) > 1 else None
    Worker(worker_id=worker_id).start()