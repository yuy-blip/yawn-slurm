# SPDX-FileCopyrightText: 2026 The YAWN authors
# SPDX-License-Identifier: Apache-2.0
"""yawn.worker: runs inside the Slurm job. Starts the model server, guards it, and tells the gateway where it is.

  python3 -m yawn.worker [--ready-path /v1/models] [--port N] -- vllm serve MODEL --uds {socket}

In the server command, `{socket}` is replaced by the path of a Unix socket in a directory only this account can open
(use it when the server can listen on a Unix socket, like vLLM's --uds). `{port}` is replaced by a free port on
127.0.0.1 instead, for servers that cannot; other jobs on the same node can then reach the server directly.

- The worker listens on the node's network and passes a request on only if it carries this job's secret, whatever
  the path. The model server itself is never on the network, so its own unguarded paths cannot be used.
- Once the server answers `--ready-path`, the worker registers with the gateway (YAWN_GATEWAY) using YAWN_TOKEN and
  repeats that every few seconds. The gateway learns the address from the connection itself.
- SIGTERM (cancel, preemption, time limit): unregister first, then stop the server.
- A user's own job (started with llm-up) also ends itself after YAWN_IDLE_S idle seconds, or after YAWN_HANDBACK_S
  once the shared model is answering again.
Environment, set by the gateway or by llm-up: YAWN_GATEWAY, YAWN_TOKEN, YAWN_MODEL; optional YAWN_IDLE_S,
YAWN_HANDBACK_S, YAWN_BEAT_S.
"""
import argparse
import hmac
import json
import os
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

DROP = {"authorization", "connection", "keep-alive", "host", "te", "upgrade", "proxy-authorization"}


def say(*a):
    print(time.strftime("[%F %T]"), *a, flush=True)


def connect(upstream, timeout):
    """A socket to the model server: ("unix", path) or ("tcp", port on 127.0.0.1)."""
    if upstream[0] == "unix":
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect(upstream[1])
    else:
        s = socket.create_connection(("127.0.0.1", upstream[1]), timeout=timeout)
    return s


def answers(upstream, path):
    """Does the model server answer `path` with 200?"""
    try:
        s = connect(upstream, 5)
        try:
            s.sendall(f"GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".encode())
            head = s.recv(64)
        finally:
            s.close()
        return head.split(b" ", 2)[1:2] == [b"200"]
    except OSError:
        return False


def make_handler(upstream, secret):
    want = ("Bearer " + secret).encode()

    class H(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.0"
        timeout = 60

        def log_message(self, fmt, *a):
            pass

        def refuse(self, code, text):
            body = json.dumps({"error": text}).encode()
            try:
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except OSError:
                pass

        def relay(self):
            self.close_connection = True
            if not hmac.compare_digest((self.headers.get("Authorization") or "").encode(), want):
                return self.refuse(401, "Unauthorized")  # every path, not only the ones the server itself guards
            try:
                n = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(n) if n > 0 else b""
            except (ValueError, OSError):
                return self.refuse(400, "Bad request")
            head = [f"{self.command} {self.path} HTTP/1.1", "Host: localhost", "Connection: close"]
            head += [f"{k}: {v}" for k, v in self.headers.items() if k.lower() not in DROP]
            try:
                up = connect(upstream, 30)
            except OSError:
                return self.refuse(502, "The model server is not answering")
            sent = False
            try:
                up.settimeout(None)  # the gateway decides how long an answer may take
                up.sendall(("\r\n".join(head) + "\r\n\r\n").encode("latin-1") + body)
                while True:
                    chunk = up.recv(65536)
                    if not chunk:
                        break
                    self.connection.sendall(chunk)  # if the gateway hung up this raises, and closing `up` stops the server's work
                    sent = True
            except OSError:
                if not sent:
                    self.refuse(502, "The model server is not answering")
            finally:
                up.close()

        do_GET = do_POST = relay

    return H


def main():
    argv = sys.argv[1:]
    if "--" not in argv:
        sys.exit(__doc__)
    i = argv.index("--")
    ap = argparse.ArgumentParser(prog="yawn.worker")
    ap.add_argument("--ready-path", default="/v1/models")
    ap.add_argument("--port", type=int, default=0, help="port to listen on (default: any free port)")
    a = ap.parse_args(argv[:i])
    cmd = argv[i + 1:]
    try:
        gw_url, model = os.environ["YAWN_GATEWAY"].rstrip("/"), os.environ["YAWN_MODEL"]
        token = os.environ["YAWN_TOKEN"]
    except KeyError as e:
        sys.exit(f"yawn.worker: {e.args[0]} is not set (the job must be submitted by the gateway or by llm-up)")
    idle_s = float(os.environ.get("YAWN_IDLE_S") or 0)  # 0: the gateway decides when to stop this job
    handback_s = float(os.environ.get("YAWN_HANDBACK_S") or 0)
    beat_s = float(os.environ.get("YAWN_BEAT_S") or 10)
    job = os.environ.get("SLURM_JOB_ID", str(os.getpid()))
    secret = secrets.token_hex(16)

    tmp = tempfile.mkdtemp(prefix="yawn-")  # mode 0700
    if any("{socket}" in c for c in cmd):
        upstream = ("unix", os.path.join(tmp, "s"))
        cmd = [c.replace("{socket}", upstream[1]) for c in cmd]
    elif any("{port}" in c for c in cmd):
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            upstream = ("tcp", s.getsockname()[1])
        cmd = [c.replace("{port}", str(upstream[1])) for c in cmd]
    else:
        sys.exit("yawn.worker: the server command needs {socket} or {port}")

    env = {k: v for k, v in os.environ.items() if k != "YAWN_TOKEN"}  # the server has no use for the token
    child = subprocess.Popen(cmd, env=env)
    front = ThreadingHTTPServer(("", a.port), make_handler(upstream, secret))
    front.daemon_threads = True
    port = front.server_address[1]
    threading.Thread(target=front.serve_forever, daemon=True).start()

    def gw(path, **body):
        req = urllib.request.Request(gw_url + path, data=json.dumps({"model": model, "job": job, **body}).encode(),
                                     headers={"Content-Type": "application/json", "Authorization": "Bearer " + token})
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read() or b"{}")

    def stop(code):
        try:
            gw("/gw/unregister")
        except Exception:  # noqa: BLE001
            pass
        if child.poll() is None:
            child.terminate()
            try:
                child.wait(25)
            except subprocess.TimeoutExpired:
                child.kill()
        shutil.rmtree(tmp, ignore_errors=True)
        sys.exit(code)

    signal.signal(signal.SIGTERM, lambda *_: stop(143))
    signal.signal(signal.SIGINT, lambda *_: stop(130))
    say(f"job {job}: starting {model}; listening on port {port}")
    ready = announced = False
    refused = 0
    while True:
        if child.poll() is not None:
            say(f"the server ended with code {child.returncode}")
            stop(child.returncode or 1)
        ok = answers(upstream, a.ready_path)
        if not ready and not ok:
            time.sleep(2)
            continue
        if not ready:
            ready = True
            say("the server is ready; registering with the gateway")
        try:
            st = gw("/gw/register", port=port, secret=secret, ready=ok)
            refused = 0
            if not announced:
                announced = True
                say("registered")
            idle = st.get("idle_s", 0)
            if st.get("stop"):
                say("the gateway asked this job to stop")
                stop(0)
            if idle_s and idle > idle_s:
                say(f"idle {idle:.0f}s: stopping")
                stop(0)
            if handback_s and st.get("shared_ready") and idle > handback_s:
                say(f"the shared model is back and this job has been idle {idle:.0f}s: stopping")
                stop(0)
        except urllib.error.HTTPError as e:  # the gateway answered but refused
            try:
                why = json.loads(e.read())["error"]["message"]
            except Exception:  # noqa: BLE001
                why = str(e)
            refused += 1
            say(f"the gateway refused the registration ({refused}): {why}")
            if refused >= 6:  # do not hold a GPU for a job the gateway will never use
                stop(1)
        except Exception as e:  # noqa: BLE001
            say(f"gateway not reachable: {e}")
        time.sleep(beat_s)


if __name__ == "__main__":
    main()
