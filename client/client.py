"""
client.py  -  Job Submission Client  (SSL/TLS enabled)
=======================================================
Connects to the server over SSL, submits jobs, polls for results.

Run:
  python3 client.py              interactive mode
  python3 client.py batch 10    submit 10 jobs at once
"""

import socket
import ssl
import sys
import time
import os
import threading

sys.path.insert(0, os.path.dirname(__file__))
from common import (
    SERVER_HOST, SERVER_PORT,
    MsgType,
    make_client_ssl_context,
    send_msg, recv_msg, setup_logger, make_id, Timer
)

log = setup_logger("Client")


# ─────────────────────────────────────────────
#  Connect with SSL
# ─────────────────────────────────────────────
def connect(retries=5):
    """Open a new SSL connection to the server. Shows clear errors."""
    # ── Edge case: check cert file exists before even trying ──────────────
    from common import CERT_FILE
    if not os.path.exists(CERT_FILE):
        print(f"  ✗ SSL ERROR: Certificate file not found: {CERT_FILE}")
        print(f"    Make sure server.crt is in the same folder as client.py")
        sys.exit(1)

    ssl_ctx = make_client_ssl_context()
    for attempt in range(1, retries + 1):
        try:
            raw_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            raw_sock.settimeout(10)
            raw_sock.connect((SERVER_HOST, SERVER_PORT))

            # ── Edge case: SSL handshake failure (wrong cert, plain server) ──
            ssl_sock = ssl_ctx.wrap_socket(raw_sock, server_hostname=SERVER_HOST)
            log.info(f"SSL connected  cipher={ssl_sock.cipher()[0]}")
            return ssl_sock

        except ConnectionRefusedError:
            print(f"  ✗ Connection refused — is server.py running on {SERVER_HOST}:{SERVER_PORT}?")
            if attempt < retries:
                print(f"    Retrying ({attempt}/{retries})...")
                time.sleep(2)

        except ssl.SSLError as e:
            # This happens if server is not running SSL, or cert mismatch
            print(f"  ✗ SSL HANDSHAKE FAILED: {e}")
            print(f"    Make sure the server has the correct server.crt and server.key")
            print(f"    Make sure your server.crt matches the server's certificate")
            raw_sock.close()
            sys.exit(1)

        except OSError as e:
            print(f"  ✗ Network error: {e}")
            if attempt < retries:
                time.sleep(2)

    print(f"  ✗ Could not connect after {retries} attempts. Giving up.")
    sys.exit(1)


# ─────────────────────────────────────────────
#  Submit / Query helpers
# ─────────────────────────────────────────────
def submit_job(sock, payload, priority=5):
    job_id = make_id("job")
    ok = send_msg(sock, {
        "type":     MsgType.SUBMIT,
        "job_id":   job_id,
        "payload":  payload,
        "priority": priority,
    })
    if not ok:
        return None
    return recv_msg(sock)


def query_status(sock, job_id):
    ok = send_msg(sock, {"type": MsgType.QUERY_STATUS, "job_id": job_id})
    if not ok:
        return None
    return recv_msg(sock)


# ─────────────────────────────────────────────
#  Interactive mode
# ─────────────────────────────────────────────
def interactive_mode():
    print("\n=== Interactive Client  [SSL] ===")
    print("Commands:")
    print("  submit <payload>        e.g.  submit reverse:hello")
    print("  submit <payload>        e.g.  submit calc:10,+,5")
    print("  status <job_id>         check a job manually")
    print("  quit\n")
    print("Available jobs: reverse  calc  factorial  wordcount  password  hash")
    print("Format:  submit <jobname>:<argument>\n")

    submitted_ids = []

    while True:
        try:
            line = input("client> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nBye!")
            break

        if not line:
            continue

        parts = line.split()
        cmd   = parts[0].lower()

        if cmd == "quit":
            print("Bye!")
            break

        elif cmd == "submit":
            if len(parts) < 2:
                print("  Usage: submit <payload>   e.g.  submit reverse:hello")
                continue

            payload = " ".join(parts[1:])

            # ── Edge case: empty payload ───────────────────────────────────
            if not payload.strip():
                print("  ✗ ERROR: payload cannot be empty")
                print("    Usage: submit <jobname>:<argument>   e.g.  submit reverse:hello")
                continue

            # ── Edge case: payload too long ────────────────────────────────
            if len(payload) > 500:
                print(f"  ✗ ERROR: payload too long ({len(payload)} chars, max 500)")
                continue

            # ── Edge case: warn if format looks wrong (no colon) ──────────
            if ":" not in payload:
                print(f"  ⚠  Warning: no job name detected in '{payload}'")
                print(f"    Expected format:  submit <jobname>:<argument>")
                print(f"    Available jobs:   reverse  calc  factorial  wordcount  password  hash")
                ans = input("    Submit anyway? (y/n): ").strip().lower()
                if ans != "y":
                    continue

            try:
                # Step 1 — submit
                sock = connect()
                resp = submit_job(sock, payload)
                sock.close()

                if not resp:
                    print("  ✗ Submission failed — is server.py running?")
                    continue

                job_id = resp.get("job_id", "?")
                print(f"  ✓ Job submitted!   id = {job_id}  [sent over SSL]")
                print(f"  ⏳ Waiting for worker to finish...")
                submitted_ids.append(job_id)

                # Step 2 — poll every second until done (max 30s)
                for attempt in range(30):
                    time.sleep(1)
                    sock = connect()
                    sr   = query_status(sock, job_id)
                    sock.close()
                    if not sr:
                        continue
                    state  = sr.get("state", "?")
                    result = sr.get("result", "")
                    if state == "COMPLETED":
                        print(f"  ✅ COMPLETED!   result = '{result}'")
                        break
                    elif state == "FAILED":
                        print(f"  ❌ Job FAILED")
                        break
                    else:
                        print(f"     still {state}... ({attempt+1}s)")
                else:
                    print("  ⚠  Still pending after 30s — worker may be busy")

            except Exception as e:
                print(f"  ✗ Error: {e}")

        elif cmd == "status":
            if len(parts) < 2:
                if submitted_ids:
                    print(f"  Tip: last id was  {submitted_ids[-1]}")
                print("  Usage: status <job_id>")
                continue
            job_id = parts[1]
            try:
                sock = connect()
                resp = query_status(sock, job_id)
                sock.close()
                if resp:
                    state  = resp.get("state", "?")
                    result = resp.get("result", "")
                    print(f"  Job {job_id}  →  {state}" +
                          (f"   result='{result}'" if result else ""))
                else:
                    print("  ✗ Query failed")
            except Exception as e:
                print(f"  ✗ Error: {e}")

        else:
            print(f"  Unknown command '{cmd}'. Try: submit / status / quit")


# ─────────────────────────────────────────────
#  Batch mode  (load test)
# ─────────────────────────────────────────────
def batch_mode(n: int):
    print(f"\n=== Batch Mode: submitting {n} jobs over SSL ===")
    results      = []
    results_lock = threading.Lock()
    errors       = [0]

    jobs = [
        "reverse:hello", "calc:10,+,5", "factorial:7",
        "wordcount:the quick brown fox", "password:12", "hash:hello"
    ]

    def do_submit(i):
        try:
            sock    = connect()
            payload = jobs[i % len(jobs)]
            resp    = submit_job(sock, payload)
            sock.close()
            if resp:
                with results_lock:
                    results.append(resp.get("job_id"))
            else:
                errors[0] += 1
        except Exception as e:
            log.error(f"Batch {i} error: {e}")
            errors[0] += 1

    start   = time.time()
    threads = [threading.Thread(target=do_submit, args=(i,)) for i in range(n)]
    for t in threads: t.start()
    for t in threads: t.join()
    elapsed = round(time.time() - start, 3)

    print(f"\n  Submitted : {len(results)}/{n}")
    print(f"  Errors    : {errors[0]}")
    print(f"  Time      : {elapsed}s")
    print(f"  Throughput: {round(len(results)/elapsed, 1)} jobs/s")


# ─────────────────────────────────────────────
#  Entry point
# ─────────────────────────────────────────────
if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        # Launch the web UI instead of the interactive terminal UI
        try:
            import client_ui
            client_ui.run_ui(blocking=True)
        except Exception as e:
            print(f"Failed to start UI: {e}")
            print("Falling back to terminal interactive mode.")
            interactive_mode()
    elif args[0] == "batch":
        n = int(args[1]) if len(args) > 1 else 10
        batch_mode(n)
    else:
        print("Usage:  python3 client.py           (interactive)")
        print("        python3 client.py batch 10  (load test)")