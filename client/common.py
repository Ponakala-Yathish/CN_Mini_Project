"""
common.py - Shared protocol, constants, and utilities
Used by server.py, client.py, and worker.py
"""

import json
import socket
import ssl
import struct
import logging
import time
import os

# ─────────────────────────────────────────────
#  Network Config
#  !! CHANGE SERVER_HOST to Person 1's IP when running on 3 PCs !!
# ─────────────────────────────────────────────
SERVER_HOST = "127.0.0.1"
SERVER_PORT = 9000

# ─────────────────────────────────────────────
#  SSL Certificate file paths
#  All 3 files must be in the SAME folder as the scripts
# ─────────────────────────────────────────────
BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
CERT_FILE    = os.path.join(BASE_DIR, "server.crt")   # public cert  (needed by all 3)
KEY_FILE     = os.path.join(BASE_DIR, "server.key")   # private key  (server only)

# ─────────────────────────────────────────────
#  Timeouts & Thresholds
# ─────────────────────────────────────────────
WORKER_HEARTBEAT_INTERVAL = 3      # seconds between heartbeats
WORKER_TIMEOUT_SECONDS    = 10     # server declares worker dead after this
NO_JOB_WAIT_SECONDS       = 2      # worker waits before re-requesting when queue empty
SOCKET_TIMEOUT            = 30     # general socket timeout

# ─────────────────────────────────────────────
#  Message Types  (client ↔ server ↔ worker)
# ─────────────────────────────────────────────
class MsgType:
    # Client → Server
    SUBMIT          = "SUBMIT"
    QUERY_STATUS    = "QUERY_STATUS"

    # Server → Client
    SUBMIT_ACK      = "SUBMIT_ACK"
    STATUS_RESPONSE = "STATUS_RESPONSE"

    # Worker → Server
    REQUEST_JOB     = "REQUEST_JOB"
    JOB_DONE        = "JOB_DONE"
    JOB_FAILED      = "JOB_FAILED"
    HEARTBEAT       = "HEARTBEAT"

    # Server → Worker
    JOB_ASSIGN      = "JOB_ASSIGN"
    NO_JOB          = "NO_JOB"

    # Generic
    ERROR           = "ERROR"
    OK              = "OK"


# ─────────────────────────────────────────────
#  Job States
# ─────────────────────────────────────────────
class JobState:
    PENDING   = "PENDING"
    ASSIGNED  = "ASSIGNED"
    COMPLETED = "COMPLETED"
    FAILED    = "FAILED"


# ─────────────────────────────────────────────
#  SSL Context builders
# ─────────────────────────────────────────────
def make_server_ssl_context() -> ssl.SSLContext:
    """
    Server-side SSL context.
    Loads the certificate + private key.
    Called only by server.py
    """
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(certfile=CERT_FILE, keyfile=KEY_FILE)
    return ctx


def make_client_ssl_context() -> ssl.SSLContext:
    """
    Client/Worker-side SSL context.
    Loads the server's public certificate to verify the server.
    Called by client.py and worker.py
    """
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_verify_locations(cafile=CERT_FILE)   # trust our self-signed cert
    ctx.check_hostname = False                     # no hostname in self-signed cert
    return ctx


# ─────────────────────────────────────────────
#  Framed send / receive  (length-prefix)
#  Works on both plain sockets AND ssl.SSLSocket
# ─────────────────────────────────────────────
def send_msg(sock, payload: dict) -> bool:
    """
    Serialize dict → JSON, prefix with 4-byte big-endian length, send.
    Returns True on success, False on failure.
    """
    try:
        data  = json.dumps(payload).encode("utf-8")
        frame = struct.pack(">I", len(data)) + data
        sock.sendall(frame)
        return True
    except (OSError, BrokenPipeError, ssl.SSLError) as e:
        logging.debug(f"send_msg failed: {e}")
        return False


def recv_msg(sock):
    """
    Read exactly 4 bytes for length, then read that many bytes.
    Returns parsed dict or None on connection close / error.
    """
    try:
        raw_len = _recv_exact(sock, 4)
        if raw_len is None:
            return None
        msg_len = struct.unpack(">I", raw_len)[0]
        raw_data = _recv_exact(sock, msg_len)
        if raw_data is None:
            return None
        return json.loads(raw_data.decode("utf-8"))
    except (json.JSONDecodeError, OSError, ssl.SSLError) as e:
        logging.debug(f"recv_msg failed: {e}")
        return None


def _recv_exact(sock, n: int):
    """Read exactly n bytes from socket. Returns None if connection closed."""
    buf = b""
    while len(buf) < n:
        try:
            chunk = sock.recv(n - len(buf))
        except (OSError, ssl.SSLError):
            return None
        if not chunk:
            return None
        buf += chunk
    return buf


# ─────────────────────────────────────────────
#  Logging helper
# ─────────────────────────────────────────────
def setup_logger(name: str, level=logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        handler = logging.StreamHandler()
        fmt = logging.Formatter(
            "%(asctime)s  [%(name)-10s]  %(levelname)-7s  %(message)s",
            datefmt="%H:%M:%S"
        )
        handler.setFormatter(fmt)
        logger.addHandler(handler)
    return logger


# ─────────────────────────────────────────────
#  Unique ID generator
# ─────────────────────────────────────────────
def make_id(prefix: str = "id") -> str:
    import uuid
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


# ─────────────────────────────────────────────
#  Simple performance timer
# ─────────────────────────────────────────────
class Timer:
    def __init__(self):
        self.start = time.time()

    def elapsed(self) -> float:
        return round(time.time() - self.start, 4)

    def reset(self):
        self.start = time.time()