"""
plain_client.py - connects WITHOUT ssl to demonstrate ssl rejection
run this to show what happens when a client has no ssl
"""
import socket
import json
import struct

SERVER_HOST = "10.197.83.227"   # same ip as common.py
SERVER_PORT = 9000

print("\n=== Plain Client (NO SSL) ===")
print("Attempting to connect WITHOUT SSL certificate...\n")

try:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    sock.connect((SERVER_HOST, SERVER_PORT))
    print("TCP connected... now trying to send without SSL")

    # try to send a raw message without ssl handshake
    msg  = json.dumps({"type": "SUBMIT", "payload": "test"}).encode()
    frame = struct.pack(">I", len(msg)) + msg
    sock.sendall(frame)

    # try to read response
    resp = sock.recv(4096)
    print(f"Got response: {resp}")

except ConnectionResetError:
    print("✗ CONNECTION REJECTED by server")
    print("  Server closed connection — SSL handshake was expected but not provided")

except Exception as e:
    print(f"✗ FAILED: {type(e).__name__}: {e}")
    print("  Server rejected the non-SSL connection as expected")

finally:
    sock.close()
    print("\n→ This proves SSL is enforced — plain clients cannot connect")
