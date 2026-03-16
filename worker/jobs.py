"""
jobs.py  -  Actual job functions executed by the worker
==========================================================
Each function:
  - takes a single string argument (the payload from the client)
  - returns a string result (shown back to the client)

To use a job, the client submits:
    submit <job_name>:<argument>
    e.g.   submit reverse:hello
           submit add:5,3
           submit factorial:10

The worker reads the job_name, calls the matching function with the argument.
"""

import math
import time
import random
import hashlib
import string


# ─────────────────────────────────────────────────────────────────
#  JOB 1 — Reverse a string
#  submit: reverse:hello
#  result: 'olleh'
# ─────────────────────────────────────────────────────────────────
def job_reverse(arg: str) -> str:
    if not arg:
        return "ERROR: give some text.  e.g.  reverse:hello"
    result = arg[::-1]
    return f"Reversed '{arg}' → '{result}'"


# ─────────────────────────────────────────────────────────────────
#  JOB 2 — Add / subtract / multiply two numbers
#  submit: calc:10,+,5     → 15
#  submit: calc:10,-,3     → 7
#  submit: calc:6,*,7      → 42
#  submit: calc:10,/,4     → 2.5
# ─────────────────────────────────────────────────────────────────
def job_calc(arg: str) -> str:
    try:
        parts = arg.split(",")
        if len(parts) != 3:
            return "ERROR: format is  calc:num1,op,num2   e.g.  calc:10,+,5"
        a, op, b = float(parts[0]), parts[1].strip(), float(parts[2])
        if   op == "+": result = a + b
        elif op == "-": result = a - b
        elif op == "*": result = a * b
        elif op == "/":
            if b == 0:
                return "ERROR: division by zero"
            result = a / b
        else:
            return f"ERROR: unknown operator '{op}'. Use + - * /"
        # show as int if whole number
        display = int(result) if result == int(result) else round(result, 4)
        return f"{a} {op} {b} = {display}"
    except ValueError:
        return "ERROR: numbers must be numeric.  e.g.  calc:10,+,5"


# ─────────────────────────────────────────────────────────────────
#  JOB 3 — Factorial of a number
#  submit: factorial:10
#  result: 10! = 3628800
# ─────────────────────────────────────────────────────────────────
def job_factorial(arg: str) -> str:
    try:
        n = int(arg.strip())
        if n < 0:
            return "ERROR: factorial not defined for negative numbers"
        if n > 20:
            return "ERROR: too large (max 20) to keep output readable"
        result = math.factorial(n)
        return f"{n}! = {result}"
    except ValueError:
        return f"ERROR: '{arg}' is not a valid integer.  e.g.  factorial:7"


# ─────────────────────────────────────────────────────────────────
#  JOB 4 — Count words in a sentence
#  submit: wordcount:the quick brown fox
#  result: 4 words, 16 characters
# ─────────────────────────────────────────────────────────────────
def job_wordcount(arg: str) -> str:
    if not arg.strip():
        return "ERROR: provide some text.  e.g.  wordcount:hello world"
    words   = arg.split()
    letters = sum(c.isalpha() for c in arg)
    return (f"'{arg}'  →  "
            f"{len(words)} words, "
            f"{len(arg)} characters, "
            f"{letters} letters")


# ─────────────────────────────────────────────────────────────────
#  JOB 5 — Generate a password
#  submit: password:12        → 12-character random password
#  submit: password:8
# ─────────────────────────────────────────────────────────────────
def job_password(arg: str) -> str:
    try:
        length = int(arg.strip()) if arg.strip() else 12
        if length < 4 or length > 64:
            return "ERROR: length must be between 4 and 64"
        chars    = string.ascii_letters + string.digits + "!@#$%&*"
        password = "".join(random.choices(chars, k=length))
        return f"Generated password ({length} chars): {password}"
    except ValueError:
        return "ERROR: provide a number.  e.g.  password:12"


# ─────────────────────────────────────────────────────────────────
#  JOB 6 — SHA256 hash of a string
#  submit: hash:hello
#  result: sha256 hex digest
# ─────────────────────────────────────────────────────────────────
def job_hash(arg: str) -> str:
    if not arg.strip():
        return "ERROR: provide text to hash.  e.g.  hash:hello"
    digest = hashlib.sha256(arg.encode()).hexdigest()
    return f"SHA256('{arg}') = {digest}"


# ─────────────────────────────────────────────────────────────────
#  REGISTRY — maps name → function
#  worker.py imports this and looks up the right function
# ─────────────────────────────────────────────────────────────────
JOB_REGISTRY = {
    "reverse"   : job_reverse,
    "calc"      : job_calc,
    "factorial" : job_factorial,
    "wordcount" : job_wordcount,
    "password"  : job_password,
    "hash"      : job_hash,
}


def run_job(payload: str) -> str:
    """
    Called by worker.py with the raw payload string.
    Payload format:  <job_name>:<argument>
    Example:         reverse:hello
                     calc:10,+,5
                     factorial:7
                     password:16
    If no colon is given, the whole payload is treated as the job name.
    """
    # ── Edge case: completely empty payload ────────────────────────────────
    if not payload or not payload.strip():
        return "ERROR: empty payload received — nothing to execute"

    # ── Edge case: payload is just whitespace or symbols ──────────────────
    if payload.strip() in (":", "::", ":::", "---"):
        return "ERROR: invalid payload format"

    if ":" in payload:
        job_name, arg = payload.split(":", 1)
    else:
        # ── Edge case: no colon — user forgot the format ──────────────────
        job_name = payload.strip()
        arg      = ""

    job_name = job_name.strip().lower()

    # ── Edge case: empty job name (payload was just ":something") ─────────
    if not job_name:
        available = ", ".join(JOB_REGISTRY.keys())
        return (f"ERROR: job name is missing. "
                f"Format is  jobname:argument  e.g.  reverse:hello\n"
                f"Available jobs: {available}")

    func = JOB_REGISTRY.get(job_name)

    # ── Edge case: unknown job name ────────────────────────────────────────
    if func is None:
        available = ", ".join(JOB_REGISTRY.keys())
        # Check if it looks like a typo (close match)
        close = [k for k in JOB_REGISTRY if k.startswith(job_name[:3])]
        hint  = f"  Did you mean: {close[0]}?" if close else ""
        return (f"ERROR: unknown job '{job_name}'.{hint}\n"
                f"Available jobs: {available}")

    # Small delay so client can see ASSIGNED state before COMPLETED
    time.sleep(0.5)

    # ── Edge case: wrap execution so crashes don't kill the worker ─────────
    try:
        return func(arg)
    except Exception as e:
        return f"ERROR: job '{job_name}' crashed unexpectedly: {e}"


# ─────────────────────────────────────────────────────────────────
#  Quick self-test — run this file directly to test all jobs
#  python jobs.py
# ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    tests = [
        "reverse:hello",
        "calc:10,+,5",
        "calc:6,*,7",
        "calc:10,/,4",
        "factorial:10",
        "wordcount:the quick brown fox",
        "password:16",
        "hash:hello",
        "unknown:test",          # should show error
        "calc:bad,+,input",      # should show error
    ]
    print("=== Job Self-Test ===\n")
    for t in tests:
        print(f"  Input : {t}")
        print(f"  Output: {run_job(t)}\n")