# SPDX-FileCopyrightText: 2026 The YAWN authors
# SPDX-License-Identifier: Apache-2.0
"""A stand-in for `vllm serve` in tests. It behaves like the real one where that matters:

  python3 fake_vllm.py (--uds PATH | --port N) [STARTUP_S] [CHUNKS] [CHUNK_S]
- like vLLM, an API key (VLLM_API_KEY) guards only paths under /v1; /health and /invocations are open, and
  /invocations runs the same inference. A test that relies on the key alone must therefore fail.
- /v1/chat/completions (stream or not, with usage), /v1/embeddings, /v1/models, /health, /invocations.
FAKE_HEALTH_S: make /v1/models and /health slow (a busy server).
"""
import json
import os
import socket
import socketserver
import sys
import time
from http.server import BaseHTTPRequestHandler

a = sys.argv[1:]
UDS = a[a.index("--uds") + 1] if "--uds" in a else None
PORT = int(a[a.index("--port") + 1]) if "--port" in a else None
rest = [x for i, x in enumerate(a) if x not in ("--uds", "--port") and (i == 0 or a[i - 1] not in ("--uds", "--port"))]
STARTUP = float(rest[0]) if len(rest) > 0 else 2
CHUNKS = int(rest[1]) if len(rest) > 1 else 5
CHUNK_S = float(rest[2]) if len(rest) > 2 else 0.05
KEY = os.environ.get("VLLM_API_KEY")
HEALTH_S = float(os.environ.get("FAKE_HEALTH_S", "0"))


class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def address_string(self):
        return "client"

    def send_json(self, code, obj):
        b = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def guarded(self):
        return bool(KEY) and self.path.startswith("/v1") and self.headers.get("Authorization") != "Bearer " + KEY

    def do_GET(self):
        if self.guarded():
            return self.send_json(401, {"error": "Unauthorized"})
        if self.path in ("/health", "/v1/models"):
            time.sleep(HEALTH_S)
        if self.path == "/health":
            return self.send_json(200, {})
        return self.send_json(200, {"object": "list", "data": [{"id": "fake"}]})

    def do_POST(self):
        raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        if self.guarded():
            return self.send_json(401, {"error": "Unauthorized"})
        req = json.loads(raw or b"{}")
        prompt = len(json.dumps(req.get("messages", req.get("input", ""))))
        if self.path.endswith("/embeddings"):
            return self.send_json(200, {"data": [{"embedding": [0.0] * 4}], "usage": {"prompt_tokens": prompt, "total_tokens": prompt}})
        if not req.get("stream"):
            time.sleep(CHUNKS * CHUNK_S)
            return self.send_json(200, {"id": "x", "model": req.get("model"), "seen_cache_salt": req.get("cache_salt"),
                                        "seen_path": self.path,
                                        "choices": [{"index": 0, "message": {"role": "assistant", "content": "hi " * CHUNKS}}],
                                        "usage": {"prompt_tokens": prompt, "completion_tokens": CHUNKS, "total_tokens": prompt + CHUNKS}})
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()

        def ev(obj):
            data = b"data: " + (obj if isinstance(obj, bytes) else json.dumps(obj).encode()) + b"\n\n"
            self.wfile.write(f"{len(data):x}\r\n".encode() + data + b"\r\n")
            self.wfile.flush()

        for i in range(CHUNKS):
            ev({"id": "x", "model": req.get("model"), "choices": [{"index": 0, "delta": {"content": "hi "}}]})
            time.sleep(CHUNK_S)
        if (req.get("stream_options") or {}).get("include_usage"):
            ev({"id": "x", "model": req.get("model"), "choices": [],
                "usage": {"prompt_tokens": prompt, "completion_tokens": CHUNKS, "total_tokens": prompt + CHUNKS}})
        ev(b"[DONE]")
        self.wfile.write(b"0\r\n\r\n")


class Unix(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True


class Tcp(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True
    allow_reuse_address = True


time.sleep(STARTUP)
srv = Unix(UDS, H) if UDS else Tcp(("127.0.0.1", PORT), H)
srv.serve_forever()
