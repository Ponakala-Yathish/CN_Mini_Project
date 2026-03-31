"""
client_ui.py  -  Web Browser UI for Job Queue Client  (SSL enabled)
=====================================================================
Opens a web browser UI on http://localhost:5000
The browser talks to this Flask app, which connects to the job queue
server over SSL just like the regular client.py does.

Run:
  pip install flask
  python3 client_ui.py

Then open browser: http://localhost:5000
"""

import socket
import ssl
import sys
import os
import time
import threading
import json
import webbrowser

# ── Flask import with helpful error ────────────────────────────────────────
try:
    from flask import Flask, request, jsonify, render_template_string
except ImportError:
    print("Flask not installed. Run:  pip install flask")
    sys.exit(1)

sys.path.insert(0, os.path.dirname(__file__))
from common import (
    SERVER_HOST, SERVER_PORT,
    MsgType,
    make_client_ssl_context,
    send_msg, recv_msg, make_id, setup_logger
)

log    = setup_logger("ClientUI")
app    = Flask(__name__)

# ── In-memory job history (shown in UI) ────────────────────────────────────
job_history = []
history_lock = threading.Lock()


# ═══════════════════════════════════════════════════════════════════════════
#  SSL connection helpers  (same as client.py)
# ═══════════════════════════════════════════════════════════════════════════

def connect():
    from common import CERT_FILE
    if not os.path.exists(CERT_FILE):
        raise FileNotFoundError(f"server.crt not found at {CERT_FILE}")
    ssl_ctx = make_client_ssl_context()
    raw     = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    raw.settimeout(10)
    raw.connect((SERVER_HOST, SERVER_PORT))
    return ssl_ctx.wrap_socket(raw, server_hostname=SERVER_HOST)


def submit_job(payload, priority=5):
    job_id = make_id("job")
    sock   = connect()
    ok     = send_msg(sock, {
        "type":     MsgType.SUBMIT,
        "job_id":   job_id,
        "payload":  payload,
        "priority": priority,
    })
    resp = recv_msg(sock) if ok else None
    sock.close()
    return resp


def query_status(job_id):
    sock = connect()
    ok   = send_msg(sock, {"type": MsgType.QUERY_STATUS, "job_id": job_id})
    resp = recv_msg(sock) if ok else None
    sock.close()
    return resp


# ═══════════════════════════════════════════════════════════════════════════
#  HTML Page  (single file — everything inline)
# ═══════════════════════════════════════════════════════════════════════════

HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Distributed Job Queue — Client UI</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }

  body {
    font-family: 'Segoe UI', Arial, sans-serif;
    background: #0f1117;
    color: #e0e0e0;
    min-height: 100vh;
  }

  /* ── Header ── */
  .header {
    background: linear-gradient(135deg, #1a1f2e, #16213e);
    border-bottom: 2px solid #00d4ff;
    padding: 20px 40px;
    display: flex;
    align-items: center;
    gap: 16px;
  }
  .header h1 { font-size: 22px; color: #00d4ff; }
  .header .sub { font-size: 13px; color: #888; margin-top: 4px; }
  .ssl-badge {
    background: #00c853;
    color: #000;
    font-size: 11px;
    font-weight: bold;
    padding: 4px 10px;
    border-radius: 20px;
    margin-left: auto;
  }

  /* ── Layout ── */
  .container { display: grid; grid-template-columns: 1fr 1fr; gap: 24px; padding: 30px 40px; }

  /* ── Cards ── */
  .card {
    background: #1a1f2e;
    border: 1px solid #2a2f3e;
    border-radius: 12px;
    padding: 24px;
  }
  .card h2 { font-size: 15px; color: #00d4ff; margin-bottom: 18px; text-transform: uppercase; letter-spacing: 1px; }

  /* ── Form elements ── */
  select, input[type=text], input[type=number] {
    width: 100%;
    background: #0f1117;
    border: 1px solid #2a2f3e;
    color: #e0e0e0;
    padding: 10px 14px;
    border-radius: 8px;
    font-size: 14px;
    margin-bottom: 12px;
    outline: none;
    transition: border-color 0.2s;
  }
  select:focus, input:focus { border-color: #00d4ff; }

  label { font-size: 12px; color: #888; display: block; margin-bottom: 6px; }

  .btn {
    width: 100%;
    padding: 12px;
    border: none;
    border-radius: 8px;
    font-size: 14px;
    font-weight: bold;
    cursor: pointer;
    transition: opacity 0.2s, transform 0.1s;
  }
  .btn:hover { opacity: 0.85; }
  .btn:active { transform: scale(0.98); }
  .btn-primary  { background: #00d4ff; color: #000; }
  .btn-danger   { background: #ff4444; color: #fff; margin-top: 8px; }
  .btn-secondary{ background: #2a2f3e; color: #e0e0e0; margin-top: 8px; }

  /* ── Argument hint ── */
  .arg-hint {
    font-size: 12px;
    color: #666;
    margin-bottom: 12px;
    min-height: 18px;
    font-style: italic;
  }

  /* ── Status box ── */
  .status-box {
    background: #0f1117;
    border: 1px solid #2a2f3e;
    border-radius: 8px;
    padding: 16px;
    margin-top: 16px;
    min-height: 80px;
    font-size: 14px;
    line-height: 1.8;
  }
  .status-pending  { color: #ffa726; }
  .status-assigned { color: #29b6f6; }
  .status-done     { color: #66bb6a; }
  .status-error    { color: #ef5350; }

  /* ── History table ── */
  .history-wrap { overflow-x: auto; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th { color: #888; font-weight: normal; text-align: left; padding: 8px 10px; border-bottom: 1px solid #2a2f3e; }
  td { padding: 10px; border-bottom: 1px solid #1a1f2e; vertical-align: top; }
  tr:hover td { background: #1e2333; }
  .tag {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 11px;
    font-weight: bold;
  }
  .tag-pending   { background: #332200; color: #ffa726; }
  .tag-assigned  { background: #002244; color: #29b6f6; }
  .tag-completed { background: #002200; color: #66bb6a; }
  .tag-failed    { background: #330000; color: #ef5350; }

  .result-cell { color: #aaa; max-width: 220px; word-break: break-all; }
  .empty-msg   { color: #444; text-align: center; padding: 30px; }

  /* ── Server info bar ── */
  .info-bar {
    background: #1a1f2e;
    border-top: 1px solid #2a2f3e;
    padding: 10px 40px;
    font-size: 12px;
    color: #555;
    display: flex;
    gap: 30px;
  }
  .info-bar span { color: #00d4ff; }

  /* ── Spinner ── */
  .spinner {
    display: inline-block;
    width: 14px; height: 14px;
    border: 2px solid #333;
    border-top-color: #00d4ff;
    border-radius: 50%;
    animation: spin 0.8s linear infinite;
    margin-right: 8px;
    vertical-align: middle;
  }
  @keyframes spin { to { transform: rotate(360deg); } }
</style>
</head>
<body>

<div class="header">
  <div>
    <h1>🖧 Distributed Job Queue</h1>
    <div class="sub">Client Interface — Submit and track jobs in real time</div>
  </div>
  <div class="ssl-badge">🔒 SSL/TLS SECURED</div>
</div>

<div class="container">

  <!-- ── Left: Submit Job ── -->
  <div class="card">
    <h2>Submit a Job</h2>

    <label>Job Type</label>
    <select id="jobType" onchange="updateHint()">
      <option value="reverse">reverse — Reverse a string</option>
      <option value="calc">calc — Calculator</option>
      <option value="factorial">factorial — Factorial of a number</option>
      <option value="wordcount">wordcount — Count words in text</option>
      <option value="password">password — Generate a password</option>
      <option value="hash">hash — SHA256 hash</option>
    </select>

    <label>Argument</label>
    <div class="arg-hint" id="argHint">e.g. hello</div>
    <input type="text" id="jobArg" placeholder="Enter argument..." />

    <label>Priority (1 = highest, 10 = lowest)</label>
    <input type="number" id="priority" value="5" min="1" max="10" />

    <button class="btn btn-primary" onclick="submitJob()">▶ Submit Job</button>
    <button class="btn btn-secondary" onclick="clearStatus()">✕ Clear</button>

    <div class="status-box" id="statusBox">
      <span style="color:#444">Status will appear here after submission...</span>
    </div>
  </div>

  <!-- ── Right: Job History ── -->
  <div class="card">
    <h2>Job History</h2>
    <div class="history-wrap">
      <table>
        <thead>
          <tr>
            <th>Job ID</th>
            <th>Payload</th>
            <th>Status</th>
            <th>Result</th>
            <th>Time</th>
          </tr>
        </thead>
        <tbody id="historyBody">
          <tr><td colspan="5" class="empty-msg">No jobs submitted yet</td></tr>
        </tbody>
      </table>
    </div>
    <button class="btn btn-danger" onclick="clearHistory()" style="margin-top:16px">🗑 Clear History</button>
  </div>

</div>

<div class="info-bar">
  <div>Server: <span id="serverAddr">{{ server }}</span></div>
  <div>Port: <span>{{ port }}</span></div>
  <div>Encryption: <span>TLS (self-signed cert)</span></div>
  <div>Jobs submitted: <span id="totalCount">0</span></div>
</div>

<script>
const HINTS = {
  reverse:   "Any text  e.g.  hello world",
  calc:      "num,op,num  e.g.  10,+,5  or  6,*,7",
  factorial: "A number  e.g.  7",
  wordcount: "Any sentence  e.g.  the quick brown fox",
  password:  "Length  e.g.  16",
  hash:      "Any text  e.g.  hello",
};

function updateHint() {
  const job = document.getElementById("jobType").value;
  document.getElementById("argHint").textContent = "e.g. " + HINTS[job];
}
updateHint();

let totalCount = 0;

async function submitJob() {
  const jobType  = document.getElementById("jobType").value;
  const arg      = document.getElementById("jobArg").value.trim();
  const priority = parseInt(document.getElementById("priority").value) || 5;
  const box      = document.getElementById("statusBox");

  if (!arg) {
    box.innerHTML = '<span class="status-error">✗ Please enter an argument</span>';
    return;
  }

  const payload = jobType + ":" + arg;
  box.innerHTML = '<span class="spinner"></span><span class="status-pending">Submitting over SSL...</span>';

  try {
    // Submit
    const subResp = await fetch("/submit", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({payload, priority})
    });
    const subData = await subResp.json();

    if (!subData.success) {
      box.innerHTML = '<span class="status-error">✗ ' + subData.error + '</span>';
      return;
    }

    const jobId    = subData.job_id;
    const submitAt = Date.now();
    totalCount++;
    document.getElementById("totalCount").textContent = totalCount;

    box.innerHTML = `<span class="status-pending">✓ Submitted  <b>${jobId}</b> [SSL encrypted]</span><br>
                     <span class="spinner"></span><span class="status-assigned">Waiting for worker...</span>`;

    addHistoryRow(jobId, payload, "PENDING", "", "...");

    // Poll for result
    let done = false;
    for (let i = 0; i < 30 && !done; i++) {
      await sleep(1000);
      const stResp = await fetch("/status/" + jobId);
      const stData = await stResp.json();
      const state  = stData.state || "?";
      const result = stData.result || "";

      updateHistoryRow(jobId, state, result);

      if (state === "COMPLETED") {
        const elapsed = ((Date.now() - submitAt) / 1000).toFixed(2);
        box.innerHTML = `<span class="status-done">✅ COMPLETED in ${elapsed}s</span><br>
                         <b>Job ID:</b> ${jobId}<br>
                         <b>Result:</b> ${result}`;
        updateHistoryRow(jobId, "COMPLETED", result, elapsed + "s");
        done = true;
      } else if (state === "FAILED") {
        box.innerHTML = `<span class="status-error">❌ Job FAILED</span>`;
        done = true;
      } else {
        box.innerHTML = `<span class="status-pending">✓ Submitted  <b>${jobId}</b></span><br>
                         <span class="spinner"></span>
                         <span class="status-assigned">Still ${state}... (${i+1}s)</span>`;
      }
    }
    if (!done) {
      box.innerHTML = '<span class="status-error">⚠ Timed out after 30s</span>';
    }

  } catch (err) {
    box.innerHTML = '<span class="status-error">✗ Network error: ' + err + '</span>';
  }
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function clearStatus() {
  document.getElementById("statusBox").innerHTML =
    '<span style="color:#444">Status will appear here after submission...</span>';
  document.getElementById("jobArg").value = "";
}

// ── History table helpers ───────────────────────────────────────────────
const rows = {};

function addHistoryRow(jobId, payload, state, result, time) {
  const tbody = document.getElementById("historyBody");
  // Remove empty message
  if (tbody.querySelector(".empty-msg")) tbody.innerHTML = "";

  const tr = document.createElement("tr");
  tr.id    = "row-" + jobId;
  tr.innerHTML = `
    <td style="font-size:11px;color:#666">${jobId}</td>
    <td>${payload}</td>
    <td><span class="tag tag-pending" id="tag-${jobId}">PENDING</span></td>
    <td class="result-cell" id="res-${jobId}">—</td>
    <td id="time-${jobId}">${time}</td>`;
  tbody.prepend(tr);
  rows[jobId] = true;
}

function updateHistoryRow(jobId, state, result, time) {
  const tag = document.getElementById("tag-" + jobId);
  const res = document.getElementById("res-" + jobId);
  const tim = document.getElementById("time-" + jobId);
  if (!tag) return;

  const cls = {
    PENDING:   "tag-pending",
    ASSIGNED:  "tag-assigned",
    COMPLETED: "tag-completed",
    FAILED:    "tag-failed",
  }[state] || "tag-pending";

  tag.className = "tag " + cls;
  tag.textContent = state;
  if (result) res.textContent = result;
  if (time)   tim.textContent = time;
}

function clearHistory() {
  document.getElementById("historyBody").innerHTML =
    '<tr><td colspan="5" class="empty-msg">No jobs submitted yet</td></tr>';
  totalCount = 0;
  document.getElementById("totalCount").textContent = 0;
}
</script>
</body>
</html>
"""


# ═══════════════════════════════════════════════════════════════════════════
#  Flask Routes
# ═══════════════════════════════════════════════════════════════════════════

@app.route("/")
def index():
    return render_template_string(HTML, server=SERVER_HOST, port=SERVER_PORT)


@app.route("/submit", methods=["POST"])
def api_submit():
    data     = request.get_json()
    payload  = (data.get("payload") or "").strip()
    priority = int(data.get("priority", 5))

    # Validation
    if not payload:
        return jsonify({"success": False, "error": "Payload is empty"})
    if len(payload) > 500:
        return jsonify({"success": False, "error": f"Payload too long ({len(payload)} chars)"})
    if ":" not in payload:
        return jsonify({"success": False, "error": "Format must be jobname:argument  e.g. reverse:hello"})

    try:
        resp = submit_job(payload, priority)
        if resp:
            job_id = resp.get("job_id", "?")
            with history_lock:
                job_history.append({
                    "job_id":    job_id,
                    "payload":   payload,
                    "state":     "PENDING",
                    "result":    None,
                    "submitted": time.time(),
                })
            log.info(f"UI submitted job {job_id} payload='{payload}'")
            return jsonify({"success": True, "job_id": job_id})
        else:
            return jsonify({"success": False, "error": "Server did not respond"})
    except ConnectionRefusedError:
        return jsonify({"success": False, "error": f"Cannot connect to server at {SERVER_HOST}:{SERVER_PORT} — is server.py running?"})
    except ssl.SSLError as e:
        return jsonify({"success": False, "error": f"SSL error: {e}"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/status/<job_id>")
def api_status(job_id):
    try:
        resp = query_status(job_id)
        if resp:
            state  = resp.get("state", "?")
            result = resp.get("result", "")
            # Update local history
            with history_lock:
                for j in job_history:
                    if j["job_id"] == job_id:
                        j["state"]  = state
                        j["result"] = result
            return jsonify({"state": state, "result": result})
        return jsonify({"state": "ERROR", "result": "No response from server"})
    except Exception as e:
        return jsonify({"state": "ERROR", "result": str(e)})


# ═══════════════════════════════════════════════════════════════════════════
#  Start
# ═══════════════════════════════════════════════════════════════════════════

def run_ui(host="0.0.0.0", port=5000, open_browser=True, blocking=True):
  """Start the Flask UI.

  If `blocking` is True this will call `app.run()` in the current thread (blocks).
  If `blocking` is False the Flask server is started in a daemon thread.
  """
  addr = f"http://{host}:{port}" if host not in ("0.0.0.0", "") else f"http://localhost:{port}"
  print("\n=== Job Queue Web UI ===")
  print(f"  Connecting to job server: {SERVER_HOST}:{SERVER_PORT}")
  print(f"  Opening browser at:       {addr}")
  print(f"  Press Ctrl+C to stop\n")

  if open_browser:
    threading.Timer(1.0, lambda: webbrowser.open(addr)).start()

  if blocking:
    app.run(host=host, port=port, debug=False, use_reloader=False)
  else:
    t = threading.Thread(target=app.run, kwargs={"host": host, "port": port, "debug": False, "use_reloader": False}, daemon=True)
    t.start()


if __name__ == "__main__":
  run_ui()
