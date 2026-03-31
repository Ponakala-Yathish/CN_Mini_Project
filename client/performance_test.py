"""
performance_test.py  -  Performance Evaluation Script
=======================================================
Measures:
  1. Throughput       — jobs submitted per second
  2. Latency          — time from submit to COMPLETED per job
  3. Single vs multi  — 1 worker vs 2 workers comparison
  4. Multi-client     — concurrent clients submitting at once

Run AFTER starting server.py and worker.py:
  python3 performance_test.py

Results are printed as a table and saved to performance_results.txt
"""

import socket
import ssl
import threading
import time
import sys
import os
import statistics

sys.path.insert(0, os.path.dirname(__file__))
from common import (
    SERVER_HOST, SERVER_PORT,
    MsgType,
    make_client_ssl_context,
    send_msg, recv_msg, make_id
)

# ─────────────────────────────────────────────
#  Connection helper
# ─────────────────────────────────────────────
def connect():
    from common import CERT_FILE
    ssl_ctx = make_client_ssl_context()
    raw     = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    raw.settimeout(10)
    raw.connect((SERVER_HOST, SERVER_PORT))
    return ssl_ctx.wrap_socket(raw, server_hostname=SERVER_HOST)


def submit_job(payload, priority=5):
    job_id = make_id("job")
    sock   = connect()
    send_msg(sock, {
        "type":     MsgType.SUBMIT,
        "job_id":   job_id,
        "payload":  payload,
        "priority": priority,
    })
    resp = recv_msg(sock)
    sock.close()
    return job_id if resp else None


def poll_until_done(job_id, timeout=60):
    """Poll server until job is COMPLETED. Returns latency in seconds."""
    start = time.time()
    for _ in range(timeout):
        time.sleep(1)
        try:
            sock = connect()
            send_msg(sock, {"type": MsgType.QUERY_STATUS, "job_id": job_id})
            resp = recv_msg(sock)
            sock.close()
            if resp and resp.get("state") == "COMPLETED":
                return round(time.time() - start, 3)
        except Exception:
            pass
    return None  # timed out


# ─────────────────────────────────────────────
#  Print helpers
# ─────────────────────────────────────────────
def header(title):
    print("\n" + "═" * 58)
    print(f"  {title}")
    print("═" * 58)


def row(label, value, unit=""):
    print(f"  {label:<35} {value} {unit}")


def divider():
    print("  " + "─" * 54)


# ═══════════════════════════════════════════════════════════════════════════
#  TEST 1 — Throughput
#  Submit N jobs as fast as possible, measure jobs/second
# ═══════════════════════════════════════════════════════════════════════════

def test_throughput(n=10):
    header(f"TEST 1 — Throughput  ({n} jobs)")
    print(f"  Submitting {n} jobs sequentially and measuring rate...\n")

    payloads = ["reverse:hello", "calc:10,+,5", "factorial:7",
                "wordcount:the quick brown fox", "hash:hello", "password:12"]
    success  = 0
    errors   = 0
    start    = time.time()

    for i in range(n):
        try:
            jid = submit_job(payloads[i % len(payloads)])
            if jid:
                success += 1
            else:
                errors += 1
        except Exception:
            errors += 1

    elapsed    = round(time.time() - start, 3)
    throughput = round(success / elapsed, 2) if elapsed > 0 else 0

    row("Jobs submitted successfully", success)
    row("Errors", errors)
    row("Total time", elapsed, "s")
    row("Throughput", throughput, "jobs/s")

    return {"test": "throughput", "n": n, "success": success,
            "elapsed": elapsed, "throughput": throughput}


# ═══════════════════════════════════════════════════════════════════════════
#  TEST 2 — Latency
#  Measure time from submit → COMPLETED for each job
# ═══════════════════════════════════════════════════════════════════════════

def test_latency(n=5):
    header(f"TEST 2 — Latency  ({n} jobs end-to-end)")
    print(f"  Measuring time from submit to COMPLETED for each job...\n")
    print(f"  (Make sure at least 1 worker is running)\n")

    payloads = ["reverse:hello", "calc:6,*,7", "factorial:5",
                "wordcount:hello world", "hash:test"]
    latencies = []

    for i in range(n):
        payload = payloads[i % len(payloads)]
        t_start = time.time()
        try:
            job_id  = submit_job(payload)
            if not job_id:
                print(f"  Job {i+1}: submission failed")
                continue
            latency = poll_until_done(job_id)
            if latency is not None:
                latencies.append(latency)
                print(f"  Job {i+1} ({payload:<25})  latency = {latency}s")
            else:
                print(f"  Job {i+1}: timed out waiting for completion")
        except Exception as e:
            print(f"  Job {i+1}: error — {e}")

    if latencies:
        divider()
        row("Jobs completed",    len(latencies))
        row("Min latency",       min(latencies),                      "s")
        row("Max latency",       max(latencies),                      "s")
        row("Avg latency",       round(statistics.mean(latencies), 3),"s")
        if len(latencies) > 1:
            row("Std deviation", round(statistics.stdev(latencies), 3),"s")
    else:
        print("  No jobs completed — is a worker running?")

    return {"test": "latency", "latencies": latencies}


# ═══════════════════════════════════════════════════════════════════════════
#  TEST 3 — Multiple concurrent clients
#  N clients submit simultaneously, measure total throughput
# ═══════════════════════════════════════════════════════════════════════════

def test_multi_client(num_clients=3, jobs_each=3):
    header(f"TEST 3 — Multi-Client  ({num_clients} clients × {jobs_each} jobs)")
    print(f"  {num_clients} clients submitting {jobs_each} jobs each simultaneously...\n")

    payloads    = ["reverse:hello", "calc:10,+,5", "factorial:5",
                   "hash:hello", "wordcount:hello world", "password:8"]
    results     = []
    res_lock    = threading.Lock()
    errors      = [0]

    def client_thread(client_id):
        for j in range(jobs_each):
            payload = payloads[(client_id * jobs_each + j) % len(payloads)]
            try:
                jid = submit_job(payload)
                if jid:
                    with res_lock:
                        results.append(jid)
                else:
                    errors[0] += 1
            except Exception:
                errors[0] += 1

    start   = time.time()
    threads = [threading.Thread(target=client_thread, args=(i,))
               for i in range(num_clients)]
    for t in threads: t.start()
    for t in threads: t.join()
    elapsed = round(time.time() - start, 3)

    total      = num_clients * jobs_each
    success    = len(results)
    throughput = round(success / elapsed, 2) if elapsed > 0 else 0

    row("Total jobs attempted",  total)
    row("Successful submissions",success)
    row("Errors",                errors[0])
    row("Wall time",             elapsed,    "s")
    row("Combined throughput",   throughput, "jobs/s")

    return {"test": "multi_client", "clients": num_clients,
            "success": success, "elapsed": elapsed, "throughput": throughput}


# ═══════════════════════════════════════════════════════════════════════════
#  TEST 4 — Single worker vs instruction to use 2 workers
#  We submit a batch and measure, print instructions for 2-worker comparison
# ═══════════════════════════════════════════════════════════════════════════

def test_worker_comparison(n=6):
    header(f"TEST 4 — Worker Scaling  ({n} jobs)")
    print(f"  Submitting {n} jobs and measuring completion time.")
    print(f"  Run this test TWICE:")
    print(f"    First  with 1 worker running  → note the time")
    print(f"    Second with 2 workers running → time should be ~half\n")

    payloads = ["reverse:hello", "calc:10,*,10", "factorial:8",
                "wordcount:distributed systems", "hash:password123", "password:20"]
    job_ids  = []

    # Submit all jobs
    submit_start = time.time()
    for i in range(n):
        try:
            jid = submit_job(payloads[i % len(payloads)])
            if jid:
                job_ids.append(jid)
                print(f"  Submitted job {i+1}/{n}  id={jid}")
        except Exception as e:
            print(f"  Job {i+1} submit failed: {e}")

    print(f"\n  All {len(job_ids)} jobs submitted in {round(time.time()-submit_start,3)}s")
    print(f"  Waiting for all to complete...\n")

    # Wait for all to complete
    complete_start = time.time()
    completed = 0
    for jid in job_ids:
        lat = poll_until_done(jid, timeout=60)
        if lat is not None:
            completed += 1
            print(f"  ✓ Job {jid} completed")

    total_time = round(time.time() - complete_start, 3)
    divider()
    row("Jobs submitted",        len(job_ids))
    row("Jobs completed",        completed)
    row("Total completion time", total_time, "s")
    row("Avg time per job",      round(total_time / completed, 3) if completed else 0, "s")
    print(f"\n  → Run again with 2 workers to compare!")

    return {"test": "worker_scaling", "n": n,
            "completed": completed, "total_time": total_time}


# ═══════════════════════════════════════════════════════════════════════════
#  Save results to file
# ═══════════════════════════════════════════════════════════════════════════

def save_results(all_results):
    fname = "performance_results.txt"
    with open(fname, "w") as f:
        f.write("DISTRIBUTED JOB QUEUE — PERFORMANCE RESULTS\n")
        f.write("=" * 50 + "\n")
        f.write(f"Date     : {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Server   : {SERVER_HOST}:{SERVER_PORT}\n")
        f.write("=" * 50 + "\n\n")

        for r in all_results:
            f.write(f"Test: {r['test']}\n")
            for k, v in r.items():
                if k != "test":
                    f.write(f"  {k}: {v}\n")
            f.write("\n")
    print(f"\n  Results saved → {fname}")


# ═══════════════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("\n" + "█" * 58)
    print("  DISTRIBUTED JOB QUEUE — PERFORMANCE EVALUATION")
    print("█" * 58)
    print(f"\n  Server : {SERVER_HOST}:{SERVER_PORT}")
    print(f"  Make sure server.py and at least 1 worker.py are running!\n")

    # Quick connection check
    try:
        s = connect()
        s.close()
        print("  ✓ Server reachable — starting tests...\n")
    except Exception as e:
        print(f"  ✗ Cannot reach server: {e}")
        print(f"    Start server.py first then run this script.")
        sys.exit(1)

    all_results = []

    try:
        all_results.append(test_throughput(n=10))
        time.sleep(1)

        all_results.append(test_latency(n=5))
        time.sleep(1)

        all_results.append(test_multi_client(num_clients=3, jobs_each=3))
        time.sleep(1)

        all_results.append(test_worker_comparison(n=6))

    except KeyboardInterrupt:
        print("\n\n  Interrupted by user")

    # Final summary
    header("SUMMARY")
    for r in all_results:
        if r["test"] == "throughput":
            row("Throughput (sequential)", r["throughput"], "jobs/s")
        elif r["test"] == "latency" and r["latencies"]:
            row("Avg latency (end-to-end)",
                round(statistics.mean(r["latencies"]), 3), "s")
        elif r["test"] == "multi_client":
            row(f"Multi-client throughput ({r['clients']} clients)",
                r["throughput"], "jobs/s")
        elif r["test"] == "worker_scaling":
            row("Worker scaling total time", r["total_time"], "s")

    save_results(all_results)
    print("\n  Done! ✓\n")
