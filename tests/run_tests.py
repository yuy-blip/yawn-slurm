# SPDX-FileCopyrightText: 2026 The YAWN authors
# SPDX-License-Identifier: Apache-2.0
"""End-to-end tests for the gateway with a stand-in Slurm and model server (no GPU, no Slurm needed).

python3 tests/run_tests.py [WORKDIR]

Every model job runs the shipped examples/model.sbatch, so the template and yawn.worker are tested as they are.
The stand-ins behave like the real ones where it matters: the model server guards only /v1 with its key (like
vLLM), and the stand-in Slurm stops jobs with SIGTERM first (like Slurm).
"""
import getpass
import json
import os
import re
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, ".."))
W = sys.argv[1] if len(sys.argv) > 1 else tempfile.mkdtemp(prefix="yawn-")
os.makedirs(W, exist_ok=True)
BASE = int(os.environ.get("GW_TEST_BASE", "18180"))  # change it to run two test runs side by side
GWPORT, EDGEPORT = BASE, BASE + 5
FS = [sys.executable, os.path.join(HERE, "fakeslurm.py")]
SOCK = os.path.join(W, "key.sock")
ENV = {**os.environ, "FAKESLURM_DIR": os.path.join(W, "slurm"), "FAKESLURM_PEND": "1", "FAKESLURM_REQUEUE_DELAY": "2",
       "PATH": os.path.join(W, "bin") + os.pathsep + os.environ["PATH"],
       "PYTHONPATH": ROOT + (os.pathsep + os.environ["PYTHONPATH"] if os.environ.get("PYTHONPATH") else ""),
       "YAWN_SOCKET": SOCK, "YAWN_BEAT_S": "1", "YAWN_HANDBACK_S": "3",
       "YAWN_SBATCH": " ".join(FS + ["sbatch"]), "YAWN_SQUEUE": " ".join(FS + ["squeue"]), "YAWN_SCANCEL": " ".join(FS + ["scancel"])}
MOD = {"llm-up": "yawn.up", "llm-batch": "yawn.batch", "llm-key": "yawn.key"}
ME = getpass.getuser()
results = []


def check(name, ok, detail=""):
    ok = bool(ok)
    results.append(ok)
    print(("PASS " if ok else "FAIL ") + name + (f"   [{detail}]" if detail and not ok else ""), flush=True)


def fs(*a):
    return subprocess.run(FS + list(a), env=ENV, capture_output=True, text=True).stdout.strip()


def tool(name, *a):
    r = subprocess.run([sys.executable, "-m", MOD[name]] + list(a), env=ENV, capture_output=True, text=True)
    return r.stdout + r.stderr


def job_of(model, prefix="yawn-"):
    out = fs("squeue", "-h", "-n", prefix + model, "-o", "x")
    return out.split("|") if out else None


def req(model, key="k-alice", stream=False, extra=None, path="/v1/chat/completions", port=None):
    body = {"model": model, "messages": [{"role": "user", "content": "hello"}], "stream": stream, **(extra or {})}
    r = urllib.request.Request(f"http://127.0.0.1:{port or GWPORT}{path}", data=json.dumps(body).encode(),
                               headers={"Content-Type": "application/json", **({"Authorization": f"Bearer {key}"} if key else {})})
    try:
        with urllib.request.urlopen(r, timeout=120) as resp:
            return resp.status, dict(resp.headers), resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read().decode()
    except Exception as e:  # noqa: BLE001  a dropped connection is a failure the tests must see, not crash on
        return 0, {}, f"{type(e).__name__}: {e}"


def wait_ok(model, limit=30, **kw):
    t0 = time.time()
    while True:
        code, h, b = req(model, **kw)
        if code == 200 or time.time() - t0 > limit:
            return code, h, b
        time.sleep(1)


def raw(port_or_path, data, close_after=None):
    """Send raw bytes over TCP (port) or a Unix socket (path); return what comes back (b"" if the connection is closed first)."""
    s = socket.socket(socket.AF_UNIX if isinstance(port_or_path, str) else socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(30)
    s.connect(port_or_path if isinstance(port_or_path, str) else ("127.0.0.1", port_or_path))
    s.sendall(data)
    if close_after is not None:
        time.sleep(close_after)
        s.close()
        return b""
    out = b""
    while True:
        try:
            b = s.recv(65536)
        except OSError:
            break
        if not b:
            break
        out += b
    s.close()
    return out


def post_raw(path, body, key="k-alice"):
    b = body if isinstance(body, bytes) else json.dumps(body).encode()
    return (f"POST {path} HTTP/1.0\r\nAuthorization: Bearer {key}\r\nContent-Type: application/json\r\n"
            f"Content-Length: {len(b)}\r\n\r\n").encode() + b


def ledger(name="ledger.jsonl"):
    try:
        return [json.loads(line) for line in open(os.path.join(W, name))]
    except OSError:
        return []


def gwlog():
    return open(os.path.join(W, "gw.log")).read()


def worker_port(pattern):
    """The port of a job's worker, from the gateway's log (the last match)."""
    m = re.findall(pattern + r" at 127\.0\.0\.1:(\d+)", gwlog())
    return int(m[-1]) if m else None


def mode(name):
    return oct(os.stat(os.path.join(W, name)).st_mode & 0o777)


# model name -> how its stand-in server behaves: [startup s, chunks, chunk s], "crash" or "stuck"
PARAMS = {"m1": [3, 5, 0.05], "q": [1, 10, 0.1], "m2": [1, 40, 0.1], "o": [1, 5, 0.05], "idle": [1, 20, 0.1],
          "bad": "crash", "stuck": "stuck", "slow": [1, 40, 0.1], "sb": [1, 5, 0.05], "pc": [1, 5, 0.05]}
MODELS = {"m1": {}, "q": {"slots": 2}, "m2": {}, "o": {"batch_sbatch": os.path.join(ROOT, "examples", "batch.sbatch")},
          "idle": {"idle_stop_s": 2}, "bad": {"retry_after_failure_s": 60}, "stuck": {"start_timeout_s": 3}, "sb": {}, "pc": {}}
SLURM = {"sbatch": FS + ["sbatch"], "squeue": FS + ["squeue"], "scancel": FS + ["scancel"]}


def start_gateway(name="config.json", log="gw.log"):
    return subprocess.Popen([sys.executable, "-m", "yawn.gateway", os.path.join(W, name)], env=ENV, cwd=W,
                            stderr=open(os.path.join(W, log), "a"))


def setup():
    os.makedirs(os.path.join(W, "bin"), exist_ok=True)
    json.dump(PARAMS, open(os.path.join(W, "params.json"), "w"))
    stub = os.path.join(W, "bin", "vllm")  # stands in for the `vllm` command the shipped job scripts call
    open(stub, "w").write(f"""#!{sys.executable}
import json, os, sys, time
a = sys.argv[1:]
if a[0] == "serve":
    p = json.load(open({os.path.join(W, "params.json")!r})).get(a[1], [1, 5, 0.05])
    if p == "crash":
        time.sleep(1); sys.exit(1)
    if p == "stuck":
        time.sleep(3600)
    os.execv(sys.executable, [sys.executable, {os.path.join(HERE, "fake_vllm.py")!r}, "--uds", a[a.index("--uds") + 1]] + [str(x) for x in p])
elif a[0] == "run-batch":
    open(a[a.index("-o") + 1], "w").write("batch model=%s in=%s\\n" % (a[a.index("--model") + 1], a[a.index("-i") + 1]))
""")
    os.chmod(stub, 0o755)
    if not os.path.exists(os.path.join(W, "bin", "python3")):
        os.symlink(sys.executable, os.path.join(W, "bin", "python3"))  # the job script says `python3 -m yawn.worker`
    sb = os.path.join(ROOT, "examples", "model.sbatch")  # the shipped template, as it is
    json.dump({"k-alice": "alice", "k-bob": "bob"}, open(os.path.join(W, "keys.json"), "w"))
    cfg = {"listen": [f"127.0.0.1:{GWPORT}", f"unix:{W}/http.sock"], "key_socket": SOCK,
           "busy_hint": " To use it now as your own job, run: llm-up {model}",
           "hold_s": 3, "hold_loading_s": 30, "poll_s": 1, "beat_ttl_s": 5, "max_waiting_per_user": 8, "slurm": SLURM,
           "models": {n: {"sbatch": sb, **c} for n, c in MODELS.items()}}
    json.dump(cfg, open(os.path.join(W, "config.json"), "w"), indent=1)
    edge = {"listen": f"127.0.0.1:{EDGEPORT}", "keys": os.path.join(W, "keys.json"), "ledger": "edge-ledger.jsonl",
            "usage": "edge-usage.json", "state": "edge-jobs.json", "max_connections": 2, "client_timeout_s": 2,
            "max_body_mb": 1, "backend_timeout_s": 1, "poll_s": 1, "hold_loading_s": 30, "slurm": SLURM,
            "models": {"slow": {"sbatch": sb}}}
    json.dump(edge, open(os.path.join(W, "edge.json"), "w"))


def main():
    print(f"workdir {W}")
    setup()
    r = subprocess.run([sys.executable, "-m", "yawn.gateway", "--check", os.path.join(W, "config.json")], env=ENV, cwd=W,
                       capture_output=True, text=True)
    check("--check: reads the config and reports what it would do", r.returncode == 0 and "config looks usable" in r.stdout,
          r.stdout[-400:] + r.stderr[-200:])
    gw = start_gateway()
    time.sleep(1.5)
    try:
        # ---- who: the kernel vouches for the caller of llm-key ----
        k1, k2 = tool("llm-key").strip(), tool("llm-key").strip()
        keys = json.load(open(os.path.join(W, "keys.json")))
        check("llm-key: a key for the calling user, the same key on the second ask", k1.startswith("yawn-") and k1 == k2 and keys.get(k1) == ME, f"{k1} {k2}")
        check("no key -> 401", req("m1", key=None)[0] == 401)

        # ---- when: a cold start is held open and answered, not refused ----
        t0 = time.time()
        code, _, b = req("m1", key=k1)
        dt = time.time() - t0
        check("cold start: the request is held and answered (no 503)", code == 200 and dt >= 3, f"{code} {dt:.1f}s {b[:100]}")
        check("record: the request is logged under the user the key belongs to", ledger()[-1]["user"] == ME, str(ledger()[-1]))
        time.sleep(7)  # usage is saved every few polls
        check("keys, request log, usage and job state are readable by this account only",
              [mode(f) for f in ("keys.json", "ledger.jsonl", "usage.json", "jobs.json")] == ["0o600"] * 4,
              str([mode(f) for f in ("keys.json", "ledger.jsonl", "usage.json", "jobs.json")]))

        # ---- no way around the gateway, on any path (the model server itself only guards /v1) ----
        wp = worker_port("m1: job \\d+ is ready")
        direct = [req("m1", key=None, port=wp)[0], req("m1", key=None, port=wp, path="/invocations")[0],
                  req("m1", key="wrong", port=wp, path="/invocations")[0], req("m1", key=k1, port=wp)[0]]
        check("the job's worker refuses everything without its secret, /invocations included", wp and direct == [401] * 4, f"{wp} {direct}")

        # ---- Slurm trouble must not hurt a model that is up ----
        n0 = int(fs("count"))
        fs("failsqueue", "on")
        time.sleep(2.5)
        codes = [req("m1", key=k1)[0] for _ in range(3)]
        fs("failsqueue", "off")
        check("squeue fails: the running model still answers, no second sbatch", codes == [200, 200, 200] and int(fs("count")) == n0,
              f"{codes} sbatch+{int(fs('count')) - n0}")
        fs("slowsqueue", "4")
        time.sleep(1.5)
        t0 = time.time()
        codes = [req("m1")[0] for _ in range(5)]
        dt = time.time() - t0
        st = json.loads(urllib.request.urlopen(f"http://127.0.0.1:{GWPORT}/gw/status", timeout=5).read())
        fs("slowsqueue", "off")
        check("squeue is slow: requests to the running model and /gw/status are not slowed down",
              codes == [200] * 5 and dt < 4 and st["m1"]["state"] == "ready", f"{codes} {dt:.1f}s {st['m1']}")
        fs("failsbatch", "on")
        code, _, b = req("sb")
        fs("failsbatch", "off")
        check("sbatch fails: a clear 503, not a dropped connection", code == 503 and "model_failed" in b, f"{code} {b[:120]}")
        time.sleep(5)

        # ---- when: Slurm keeps the job waiting -> 503 with Slurm's reason and a short Retry-After ----
        fs("busy", "on")
        code, h, b = req("m2")
        check("cluster busy: 503, Retry-After <= 60, and the way out is named",
              code == 503 and 0 < int(h.get("Retry-After", 999)) <= 60 and "llm-up m2" in b, f"{code} {h.get('Retry-After')} {b[:160]}")
        jid = job_of("m2")[0]
        fs("reason", jid, "job", "requeued", "in", "held", "state")
        time.sleep(1.5)
        code, _, b = req("m2")
        check("held job: says it is on hold, not that the GPUs are busy", code == 503 and "model_held" in b and "in use" not in b, b[:200])
        fs("reason", jid, "ReqNodeNotAvail,", "Reserved", "for", "maintenance")
        time.sleep(1.5)
        code, _, b = req("m2")
        check("other reasons: Slurm's reason is passed on", code == 503 and "model_waiting" in b and "maintenance" in b, b[:200])
        fs("release", jid)
        fs("busy", "off")

        # ---- order: no caps, but the least-served waiting user goes next ----
        wait_ok("q", key=k1)  # warm up as someone else, so alice and bob start from zero
        t0 = time.time()
        done = []
        ts = [threading.Thread(target=lambda i=i: (req("q"), done.append(("alice", i, time.time() - t0)))) for i in range(6)]
        [t.start() for t in ts]
        time.sleep(0.3)
        tb = threading.Thread(target=lambda: (req("q", key="k-bob"), done.append(("bob", 0, time.time() - t0))))
        tb.start()
        [t.join() for t in ts + [tb]]
        a_times = sorted(d[2] for d in done if d[0] == "alice")
        b_time = [d[2] for d in done if d[0] == "bob"][0]
        check("alone, one user fills every slot (6 x 1 s on 2 slots in about 3-4 s)", a_times[-1] < 5.0, f"{a_times}")
        check("a newcomer is served before the heavy user's backlog", b_time < a_times[3] + 0.2 and b_time < a_times[-1] - 0.8,
              f"bob {b_time:.2f} alice {['%.2f' % x for x in a_times]}")
        time.sleep(3)  # idle: must not add usage
        recs = [r for r in ledger() if r["model"] == "q" and r.get("billed") is not None][-7:]
        billed = {u: sum(r["billed"] for r in recs if r["user"] == u) for u in ("alice", "bob")}
        check("usage: total booked = busy time, not slots x time; idle time is booked to nobody",
              abs(sum(billed.values()) - a_times[-1]) < 0.6, f"booked {sum(billed.values()):.2f} busy {a_times[-1]:.2f}")
        check("usage: the heavy user is booked more", billed["alice"] > 3 * billed["bob"] > 0, str(billed))

        # one user cannot fill the gateway: beyond max_waiting_per_user they get 429, others are still served
        flood = []
        ts = [threading.Thread(target=lambda: flood.append(req("q")[0])) for _ in range(14)]
        [t.start() for t in ts]
        time.sleep(0.5)
        bob = req("q", key="k-bob")[0]
        [t.join() for t in ts]
        check("one user's flood: the excess gets 429, another user is still served", flood.count(429) >= 3 and flood.count(200) >= 8 and bob == 200,
              f"{sorted(flood)} bob={bob}")

        # ---- yielding: preemption, requeue, and a job that goes away while requests wait ----
        wait_ok("m2")
        out = {}
        t = threading.Thread(target=lambda: out.update(r=req("m2", stream=True)))
        t.start()
        time.sleep(1.5)
        jid = job_of("m2")[0]
        fs("preempt", jid)
        t.join()
        last = [r for r in ledger() if r["model"] == "m2" and r.get("stream")][-1]
        check("preempted mid-stream -> model_interrupted, logged as interrupted, the part used is booked",
              "model_interrupted" in out["r"][2] and last["status"] == "interrupted" and last["billed"] > 0, str(last))
        t0 = time.time()
        code, _, b = req("m2")
        check("after preemption: the next request is held until the requeued job answers (same job)", code == 200 and job_of("m2")[0] == jid,
              f"{code} {time.time() - t0:.1f}s {b[:80]}")
        got = []
        ts = [threading.Thread(target=lambda: got.append(req("q")[0])) for _ in range(5)]  # 2 run, 3 wait for a slot
        [t.start() for t in ts]
        time.sleep(0.4)
        fs("preempt", job_of("q")[0])
        [t.join() for t in ts]
        check("the job is preempted while requests wait for a slot: every one of them gets an answer", 0 not in got and len(got) == 5, str(got))

        # ---- busy cluster: the caller's own job serves them behind the same entry ----
        fs("busy", "on")
        code, _, b = req("o", key=k1)
        check("busy: 503 names the way out (llm-up)", code == 503 and "llm-up o" in b, b[:200])
        out1, out2 = tool("llm-up", "o"), tool("llm-up", "o")
        check("llm-up: submits the user's own job once", "submitted job" in out1 and "already have" in out2, out1 + out2)
        code, _, b = wait_ok("o", key=k1, limit=25)
        last = ledger()[-1]
        check("own job: the user's request is answered by it, and nothing is booked by the gateway",
              code == 200 and last["backend"] == "own" and last["billed"] is None and last["user"] == ME, f"{code} {last}")
        check("own job: no cache salt is added (it serves one person)", code == 200 and json.loads(b).get("seen_cache_salt") is None, b[:200])
        check("own job: it serves only its owner", req("o")[0] == 503)
        op = worker_port(f"own job: {ME} serves o from job \\d+")
        check("own job: its worker refuses a direct call too", op and req("o", key=k1, port=op, path="/invocations")[0] == 401, str(op))
        fs("busy", "off")
        wait_ok("o", limit=25)  # alice: shared is back
        code, _, b = req("o", key=k1)
        check("shared back: the user's requests return to the shared model", code == 200 and ledger()[-1]["backend"] == "shared", str(ledger()[-1]))
        time.sleep(9)
        check("hand-back: the user's idle own job stops by itself once shared is back", job_of("o", "yawn-own-") is None, str(job_of("o", "yawn-own-")))
        open(os.path.join(W, "in.jsonl"), "w").write("{}\n")
        outp = os.path.join(W, "in.out.jsonl")
        msg = tool("llm-batch", os.path.join(W, "in.jsonl"), "--model", "o")
        time.sleep(3)
        check("llm-batch: the user's own job runs the shipped batch script with the model, input and output",
              os.path.exists(outp) and "batch model=o in=" in open(outp).read(), msg)

        # ---- registration: only with a token the gateway knows, only over TCP ----
        reg = {"model": "m1", "job": "1", "port": 9, "secret": "x"}
        r1 = raw(GWPORT, post_raw("/gw/register", reg, key="nobody"))
        r2 = raw(GWPORT, post_raw("/gw/register", {**reg, "port": "x"}))
        r3 = raw(os.path.join(W, "http.sock"), post_raw("/gw/register", reg))
        check("register: unknown token 401, bad port 400, over the socket file 400",
              b" 401 " in r1[:20] and b" 400 " in r2[:20] and b" 400 " in r3[:20], (r1[:30] + r2[:30] + r3[:30]).decode(errors="replace"))

        # ---- bodies, paths, connections ----
        out = raw(GWPORT, post_raw("/v1/chat/completions", b"[1, 2]"))
        check("a JSON body that is not an object -> 400", b" 400 " in out[:20], out[:200])
        out = raw(GWPORT, post_raw("/metrics", {"model": "m1"}))
        out2 = raw(GWPORT, post_raw("/invocations", {"model": "m1"}))
        check("paths: anything but the model API -> 404, nothing sent on", b" 404 " in out[:20] and b" 404 " in out2[:20], (out[:60] + out2[:60]).decode(errors="replace"))
        out = raw(os.path.join(W, "http.sock"), b"GET /v1/models HTTP/1.0\r\nAuthorization: Bearer k-alice\r\n\r\n")
        check("unix listener: the same API over a socket file", b" 200 " in out[:20] and b'"m1"' in out, out[:200])
        ep = start_gateway("edge.json", "edge.log")
        time.sleep(1.5)
        try:
            out = raw(EDGEPORT, f"POST /v1/chat/completions HTTP/1.0\r\nAuthorization: Bearer k-alice\r\nContent-Length: {2 * 2**20}\r\n\r\n".encode())
            check("a body over max_body_mb -> 413 without reading it", b" 413 " in out[:20], out[:200])
            idle = [socket.create_connection(("127.0.0.1", EDGEPORT)) for _ in range(2)]
            time.sleep(0.3)
            out = raw(EDGEPORT, b"GET /v1/models HTTP/1.0\r\nAuthorization: Bearer k-alice\r\n\r\n")
            check("connection cap: the next connection gets a quick 503, not a thread", b" 503 " in out[:20] and b"too_many" in out, out[:120])
            time.sleep(2.5)
            out = raw(EDGEPORT, b"GET /v1/models HTTP/1.0\r\nAuthorization: Bearer k-alice\r\n\r\n")
            check("silent connections are dropped after client_timeout_s, and service goes on", b" 200 " in out[:20], out[:120])
            [c.close() for c in idle]
            wait_ok("slow", port=EDGEPORT, stream=True)  # streams send bytes all along, so they pass the 1 s limit
            n0 = len(ledger("edge-ledger.jsonl"))
            t0 = time.time()
            code, _, b = req("slow", port=EDGEPORT)  # a 4 s answer in one piece
            dt = time.time() - t0
            sent = [r for r in ledger("edge-ledger.jsonl")[n0:] if r.get("backend")]
            check("a model slower than backend_timeout_s: one 504, the request is not sent again",
                  code == 504 and dt < 3 and len(sent) == 1 and sent[0]["status"] == "backend_timeout", f"{code} {dt:.1f}s {sent}")
        finally:
            ep.terminate()

        # ---- start-up checks ----
        def refused(cfg_change, word):
            c = {**json.load(open(os.path.join(W, "edge.json"))), "listen": f"127.0.0.1:{BASE + 6}", **cfg_change}
            json.dump(c, open(os.path.join(W, "bad.json"), "w"))
            try:
                r = subprocess.run([sys.executable, "-m", "yawn.gateway", os.path.join(W, "bad.json")], env=ENV, cwd=W,
                                   capture_output=True, text=True, timeout=10)
                return r.returncode != 0 and word in r.stderr and "Traceback" not in r.stderr, r.stderr[-200:]
            except subprocess.TimeoutExpired:
                return False, "it started and kept running"
        check("a key socket in /tmp is refused at start (it could be taken over first)", *refused({"key_socket": "/tmp/yawn-test-squat.sock"}, "refusing"))
        check("an unknown setting is refused at start, with its name", *refused({"idle_stop": 5}, "idle_stop"))
        check("a config without models is refused at start, without a traceback", *refused({"models": {}}, "models"))
        check("an IPv6 listen address is refused at start, saying so", *refused({"listen": f"::1:{BASE + 6}"}, "IPv6"))
        check("a port out of range is refused at start", *refused({"listen": "127.0.0.1:99999"}, "1-65535"))
        check("an address already in use is refused at start, with the address",
              *refused({"listen": f"127.0.0.1:{GWPORT}"}, f"127.0.0.1:{GWPORT}"))
        start = refused({"listen": [f"0.0.0.0:{BASE + 6}", f"127.0.0.1:{BASE + 6}"]}, f"cannot listen on 127.0.0.1:{BASE + 6}")
        r = subprocess.run([sys.executable, "-m", "yawn.gateway", os.path.join(W, "bad.json"), "--check"], env=ENV, cwd=W,
                           capture_output=True, text=True, timeout=10)
        check("two listen addresses on the same port: the start stops, and --check reports the same address",
              start[0] and r.returncode != 0 and f"ERROR listen on 127.0.0.1:{BASE + 6}" in r.stdout, f"{start} {r.stdout[-300:]}")
        check("a second gateway does not take over the socket file a running one listens on",
              *refused({"listen": [f"unix:{W}/http.sock", f"127.0.0.1:{BASE + 6}"]}, "listening on it"))
        try:
            out = raw(os.path.join(W, "http.sock"), b"GET /v1/models HTTP/1.0\r\nAuthorization: Bearer k-alice\r\n\r\n")
        except OSError as e:
            out = repr(e).encode()
        check("a second gateway refused: the running one still answers on its socket file", b" 200 " in out[:20], out[:120])
        kp = os.path.join(W, "keys.json")
        keys_before = open(kp).read()
        json.dump({k: u for k, u in json.loads(keys_before).items() if u != ME}, open(kp, "w"))  # this user has no key now
        refusal = refused({"key_socket": SOCK}, "listening on it")
        time.sleep(1)
        mine = [k for k, u in json.load(open(kp)).items() if u == ME]
        open(kp, "w").write(keys_before)
        check("a second gateway does not take over the key socket either, and looking there makes no key", refusal[0] and not mine,
              f"{refusal} {mine}")
        long_sock = os.path.join(W, "k" * 120 + ".sock")
        check("a key socket that cannot be opened (path too long) stops the start: no gateway that hands out no keys",
              *refused({"key_socket": long_sock}, "key_socket"))
        r = subprocess.run([sys.executable, "-m", "yawn.gateway", os.path.join(W, "bad.json"), "--check"], env=ENV, cwd=W,
                           capture_output=True, text=True, timeout=10)
        check("a key socket path too long: --check refuses it too", r.returncode != 0 and "at most 107" in r.stdout, r.stdout[-300:])

        # ---- the key socket keeps serving after running out of file descriptors for a while ----
        k2 = os.path.join(W, "k2.sock")
        c = {**json.load(open(os.path.join(W, "edge.json"))), "listen": f"127.0.0.1:{BASE + 6}", "key_socket": k2,
             "usage": "fd-usage.json", "state": "fd-jobs.json", "ledger": "fd-ledger.jsonl"}
        json.dump(c, open(os.path.join(W, "fd.json"), "w"))
        limited = ("import os, resource, sys; r = resource.RLIMIT_NOFILE; resource.setrlimit(r, (48, resource.getrlimit(r)[1]));"
                   " os.execv(sys.executable, [sys.executable, '-m', 'yawn.gateway', sys.argv[1]])")  # 48 file descriptors
        fdgw = subprocess.Popen([sys.executable, "-c", limited, os.path.join(W, "fd.json")], env=ENV, cwd=W,
                                stderr=subprocess.PIPE, text=True)
        held = []
        try:
            for _ in range(50):
                if os.path.exists(k2):
                    break
                time.sleep(0.1)
            time.sleep(0.5)
            for _ in range(80):  # more connections than it has descriptors for
                s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                s.settimeout(0.5)
                try:
                    s.connect(k2)
                except OSError:
                    s.close()
                    break
                held.append(s)
            time.sleep(2)
            for s in held:
                s.close()
            time.sleep(2)
            got = tool("llm-key", "--socket", k2).strip()
        finally:
            fdgw.terminate()
            try:
                err = fdgw.communicate(timeout=10)[1]
            except subprocess.TimeoutExpired:
                fdgw.kill()
                err = fdgw.communicate()[1]
        check("out of file descriptors for a while: the key socket says so in the log and serves keys again afterwards",
              "key socket:" in err and got.startswith("yawn-"), got[:120] + " | " + err[-200:])


        # ---- the keys file ----
        kp = os.path.join(W, "keys.json")
        good = open(kp).read()
        open(kp, "w").write("{broken")
        code, _, b = req("m1")
        newkey = tool("llm-key", "--new")
        still = open(kp).read()
        open(kp, "w").write(good)
        check("broken keys file: the keys read before still work", code == 200, f"{code} {b[:100]}")
        check("broken keys file: llm-key reports an error and nothing is overwritten", "yawn-" not in newkey and still == "{broken", newkey[:120])
        check("keys file restored: everyone's key is still valid", req("m1")[0] == 200 and req("m1", key=k1)[0] == 200)

        # ---- prefix-cache privacy: every user gets their own cache salt on the shared model ----
        def salt(key, extra=None):
            return json.loads(req("m1", key=key, extra=extra)[2]).get("seen_cache_salt")
        a1, a2, b1 = salt("k-alice"), salt("k-alice"), salt("k-bob")
        ac, bc = salt("k-alice", {"cache_salt": "mine"}), salt("k-bob", {"cache_salt": "mine"})
        check("cache salt: same for one user, different between users, set even if the client sends none", a1 and a1 == a2 and a1 != b1, f"{a1} {a2} {b1}")
        check("cache salt: a client-chosen salt is never passed on as is and cannot cross users", ac not in ("mine", a1) and ac != bc, f"{ac} {bc}")

        # ---- clients that hang up ----
        wait_ok("q")
        ts = [threading.Thread(target=req, args=("q",)) for _ in range(2)]  # fill both slots for about 1 s
        [t.start() for t in ts]
        time.sleep(0.2)
        raw(GWPORT, post_raw("/v1/chat/completions", {"model": "q", "messages": []}, key="k-bob"), close_after=0.3)
        [t.join() for t in ts]
        time.sleep(1.5)
        bob = [r for r in ledger() if r["user"] == "bob" and r["model"] == "q"][-1]
        check("client hangs up in the queue: logged as client_gone, never sent, nothing booked",
              bob["status"] == "client_gone" and bob.get("billed") is None, str(bob))
        wait_ok("m2")
        n0 = int(fs("count"))
        raw(GWPORT, post_raw("/v1/chat/completions", {"model": "m2", "messages": [], "stream": True}), close_after=1.0)
        time.sleep(1.5)
        last = [r for r in ledger() if r["model"] == "m2"][-1]
        code, _, _ = req("m2")
        check("client hangs up mid-stream: client_gone, partly booked, the model still answers at once",
              last["status"] == "client_gone" and 0 < last["billed"] < 3 and code == 200 and int(fs("count")) == n0, f"{last} {code}")

        # ---- usage is measured with a clock that does not jump ----
        sys.path.insert(0, ROOT)
        from unittest import mock
        from yawn import gateway as gwmod
        mt = gwmod.Meter(os.path.join(W, "clock.json"), 7 * 86400)
        mt.start(1, "zed", "m", 1.0)
        with mock.patch("time.time", return_value=time.time() + 3600):
            got1 = mt.end(1)
        check("usage: a wall-clock jump of an hour books nothing", got1 < 0.5, f"{got1:.1f}")
        mt.usage["zed"], mt.dirty = 100.0, True
        mt.save()
        d = json.load(open(os.path.join(W, "clock.json")))
        d["t"] -= 7 * 86400  # saved one half-life ago
        json.dump(d, open(os.path.join(W, "clock.json"), "w"))
        check("usage: across a restart the time down only decays usage",
              abs(gwmod.Meter(os.path.join(W, "clock.json"), 7 * 86400).usage["zed"] - 50.0) < 0.5)

        # ---- no GPU held for nothing ----
        code, _, _ = wait_ok("idle")
        code2, _, b2 = req("idle", stream=True)  # a 2 s answer: longer than the idle limit, must not be cut
        time.sleep(6)
        gone = job_of("idle") is None
        code3, _, b3 = req("idle")
        n_idle = gwlog().count("idle: submitted job")
        check("idle stop: a long answer is not cut, the idle job is cancelled, the next request starts one new job",
              code == 200 and code2 == 200 and "model_interrupted" not in b2 and gone and code3 == 200 and n_idle == 2,
              f"{code} {code2} gone={gone} {code3} {b3[:120]} submitted={n_idle}")
        time.sleep(6)  # let that job go idle and be cancelled
        fs("busy", "on")
        code, _, _ = req("idle")
        waiting = job_of("idle") is not None
        time.sleep(6)
        gone = job_of("idle") is None
        fs("busy", "off")
        code2, _, b2 = req("idle")
        check("a waiting job that nobody asks for any more is cancelled, and that is not counted as a failure",
              code == 503 and waiting and gone and code2 == 200, f"{code} waiting={waiting} gone={gone} {code2} {b2[:100]}")
        fs("busy", "on")
        n0 = int(fs("count"))
        code, _, _ = req("pc")
        jid = job_of("pc")[0]
        fs("scancel", jid)  # someone else cancels it while it waits
        time.sleep(2.5)
        code2, _, b2 = req("pc")
        jid2 = (job_of("pc") or [None])[0]
        check("a waiting job cancelled from outside is not a start-up failure: the next request submits a new one",
              code == 503 and code2 == 503 and "model_queued" in b2 and jid2 not in (None, jid) and int(fs("count")) - n0 == 2,
              f"{code} {code2} {b2[:100]} {jid}->{jid2} +{int(fs('count')) - n0}")
        if jid2:
            fs("scancel", jid2)
        time.sleep(2.5)
        code3, _, b3 = req("pc")
        check("... but two in a row are: no third job at once",
              code3 == 503 and "model_failed" in b3 and int(fs("count")) - n0 == 2, f"{code3} {b3[:100]} +{int(fs('count')) - n0}")
        fs("busy", "off")
        code, _, _ = req("stuck")
        time.sleep(9)
        gone = job_of("stuck") is None
        code2, _, b2 = req("stuck")
        check("a job that runs but never becomes ready is cancelled after start_timeout_s, and not started again at once",
              gone and code2 == 503 and "model_failed" in b2, f"{code} gone={gone} {code2} {b2[:100]}")
        n0 = int(fs("count"))
        req("bad")
        time.sleep(3)
        codes = [req("bad") for _ in range(3)]
        check("a job that dies while starting -> model_failed, one sbatch only",
              all(c[0] == 503 and "model_failed" in c[2] for c in codes) and int(fs("count")) - n0 == 1, f"{[c[0] for c in codes]} +{int(fs('count')) - n0}")

        # ---- restart: the gateway finds its running job again ----
        wait_ok("m1")
        jid, n0 = job_of("m1")[0], int(fs("count"))
        gw.terminate()
        gw.wait(10)
        gw = start_gateway()
        time.sleep(1.5)
        code, _, b = wait_ok("m1", limit=15)
        check("gateway restart: the running job is used again, no new sbatch", code == 200 and job_of("m1")[0] == jid and int(fs("count")) == n0,
              f"{code} {job_of('m1')} +{int(fs('count')) - n0} {b[:80]}")
    finally:
        gw.terminate()
        for f in os.listdir(ENV["FAKESLURM_DIR"]):
            if f.startswith("job.") and not f.endswith(".tmp"):
                fs("scancel", f[4:])
        time.sleep(4)
    print(f"\n{sum(results)}/{len(results)} passed")
    sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    main()
