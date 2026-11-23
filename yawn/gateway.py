# SPDX-FileCopyrightText: 2026 The YAWN authors
# SPDX-License-Identifier: Apache-2.0
"""YAWN gateway: an OpenAI-compatible gateway for model servers that run as Slurm jobs.

  who     `llm-key` asks this gateway over a Unix socket; the kernel reports who is asking (SO_PEERCRED), so each
          cluster user gets their own key with no list to maintain. Key -> user name.
  when    a request for a model that is not running submits its job (sbatch). While the job loads the request is
          held open and answered when it is ready; while Slurm keeps the job waiting the reply is 503 with Slurm's
          reason and Retry-After. A job nobody uses is cancelled.
  where   the job runs `yawn.worker`, which guards the model server and registers here; its address is taken from
          that connection. Nothing is shared through files, and with `{socket}` the model server is never on the network.
  order   at most `slots` requests reach the model at a time; the rest wait here, and the next free slot goes to
          the waiting user with the fewest requests in flight, then the least use. Alone, one user may use every slot.
  record  every request is written to a per-user log; busy GPU time is split among the requests in flight.
Which job gets a GPU, and when it is preempted, stays Slurm's decision (the QoS in the job script).

Run:  yawn-gateway config.json  (or: python3 -m yawn.gateway config.json)
"""
import sys

if sys.version_info < (3, 8):
    sys.exit("the gateway needs Python 3.8 or later (this is %d.%d)" % sys.version_info[:2])

import argparse  # noqa: E402
import getpass  # noqa: E402
import hashlib  # noqa: E402
import hmac  # noqa: E402
import itertools  # noqa: E402
import json  # noqa: E402
import math  # noqa: E402
import os  # noqa: E402
import pwd  # noqa: E402
import re  # noqa: E402
import secrets  # noqa: E402
import select  # noqa: E402
import signal  # noqa: E402
import shutil  # noqa: E402
import socket  # noqa: E402
import socketserver  # noqa: E402
import stat  # noqa: E402
import struct  # noqa: E402
import subprocess  # noqa: E402
import threading  # noqa: E402
import time  # noqa: E402
import traceback  # noqa: E402
from datetime import datetime  # noqa: E402
from http.client import HTTPConnection, HTTPException  # noqa: E402
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer  # noqa: E402

try:
    from yawn import __version__ as VERSION
except ImportError:  # run as a plain file
    VERSION = "unknown"

DEFAULT_MSG = {
    "queued": "All GPUs are in use by other jobs. The model starts when one is free{at}; try again later.",
    "loading": "The model is loading. Try again in about {s} seconds.",
    "restarting": "The model's job is restarting (preempted, requeued or stopping). Try again in about {s} seconds.",
    "held": "The model's job is on hold (set by an administrator or its owner). It will not start until it is released.",
    "waiting": "The model's job is waiting in Slurm (reason: {reason}). Try again later.",
    "starting": "The model was not running and is being started now. Try again in about {s} seconds.",
    "failed": "The model's last job ended before it could answer, so it is not restarted automatically for now."
              " It will be tried again in about {s} seconds. Please tell the administrators if this repeats.",
}
INTERRUPTED = {"error": {"message": "The model stopped while answering (preempted or restarted). Try again.",
                         "type": "model_interrupted", "code": "model_interrupted"}}
DEFAULT_PATHS = ["/v1/chat/completions", "/v1/completions", "/v1/embeddings"]
HOP = {"connection", "keep-alive", "transfer-encoding", "content-length", "host", "authorization", "te", "upgrade"}
TOP_KEYS = {"listen", "url", "key_socket", "keys", "ledger", "usage", "state", "halflife_days", "hold_s", "hold_loading_s",
            "queue_wait_s", "poll_s", "models", "slurm", "messages", "busy_status", "busy_hint", "paths", "max_body_mb",
            "client_timeout_s", "max_connections", "backend_timeout_s", "max_waiting_per_user", "own_jobs", "beat_ttl_s"}
MODEL_KEYS = {"sbatch", "batch_sbatch", "job_name", "slots", "idle_stop_s", "startup_s", "start_timeout_s",
              "retry_after_failure_s", "billing_weight", "cache_salt"}


def log(*a):
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), *a, file=sys.stderr, flush=True)


def expected(j):
    """' (Slurm expects about 11-13 00:42)' for a pending job with an estimate, else ''."""
    try:
        t = datetime.strptime(j["start"], "%Y-%m-%dT%H:%M:%S")
    except (TypeError, ValueError, KeyError):
        return ""
    return f" (Slurm expects about {t:%m-%d %H:%M})" if j["state"] == "PENDING" and t > datetime.now() else ""


def write_private(path, text):
    """Replace a file atomically; only this account can read it."""
    tmp = path + ".tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(text)
    os.replace(tmp, path)


class Meter:
    """Books busy GPU time to the users whose requests were in flight (GPU-seconds times the model's weight)."""

    def __init__(self, path, halflife_s):
        self.path, self.hl = path, halflife_s
        self.lock = threading.Lock()
        self.usage, self.active, self.dirty = {}, {}, False
        self.t = time.monotonic()  # time is measured with the monotonic clock: a wall-clock jump books nothing
        try:
            with open(path) as f:
                d = json.load(f)
            usage, saved = d["usage"], d["t"]

            def number(v):
                return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)

            if not number(saved) or not isinstance(usage, dict) or not all(number(v) and v >= 0 for v in usage.values()):
                raise ValueError("not the form this gateway writes")
        except FileNotFoundError:
            return
        except (OSError, ValueError, KeyError, TypeError, OverflowError, RecursionError) as e:
            log(f"usage from {path} not used: {e!r}")
            return
        down = time.time() - saved  # across a restart only the wall clock is known: decay, never book
        k = 0.5 ** (down / self.hl) if down > 0 else 1.0
        self.usage = {u: v * k for u, v in usage.items()}

    def _tick(self):
        now = time.monotonic()
        dt, self.t = now - self.t, now
        if dt <= 0:
            return
        k = 0.5 ** (dt / self.hl)
        for u in self.usage:
            self.usage[u] *= k
        groups = {}
        for r in self.active.values():
            groups.setdefault(r[1], []).append(r)
        for reqs in groups.values():  # one busy GPU-second is shared by everything in flight on that model
            for r in reqs:
                share = dt * r[2] / len(reqs)
                self.usage[r[0]] = self.usage.get(r[0], 0.0) + share
                r[3] += share
                self.dirty = True

    def start(self, rid, user, model, weight):
        with self.lock:
            self._tick()
            self.active[rid] = [user, model, weight, 0.0]

    def end(self, rid):
        with self.lock:
            self._tick()
            r = self.active.pop(rid, None)
            return r[3] if r else 0.0

    def peek(self):  # no tick: good enough for ordering
        return self.usage

    def save(self):
        with self.lock:
            self._tick()
            if not self.dirty:
                return
            self.dirty = False
            data = json.dumps({"t": time.time(), "usage": self.usage})
        write_private(self.path, data)


class FairQueue:
    """At most `slots` requests reach the model; the next slot goes to the waiting user with the fewest requests in
    flight, then the least use."""

    def __init__(self, slots, meter, max_waiting_per_user):
        self.slots, self.meter, self.max_waiting = slots, meter, max_waiting_per_user
        self.cv = threading.Condition()
        self.running, self.waiting, self.seq = {}, [], itertools.count()

    def _grant(self):
        while self.waiting and (not self.slots or sum(self.running.values()) < self.slots):
            usage = self.meter.peek()
            w = min(self.waiting, key=lambda w: (self.running.get(w[0], 0), usage.get(w[0], 0.0), w[1]))
            self.waiting.remove(w)
            w[2] = True
            self.running[w[0]] = self.running.get(w[0], 0) + 1
        self.cv.notify_all()

    def acquire(self, user, timeout, gone=lambda: False):
        """-> "ok" (a slot was granted), "timeout", "gone" (the caller hung up) or "full" (this user already waits a lot)."""
        with self.cv:
            if sum(1 for w in self.waiting if w[0] == user) >= self.max_waiting:
                return "full"  # one user must not fill the gateway's connections
            me = [user, next(self.seq), False]
            self.waiting.append(me)
            self._grant()
            end = time.monotonic() + timeout
            while not me[2]:
                left = end - time.monotonic()
                if left <= 0 or gone():
                    self.waiting.remove(me)
                    self._grant()
                    return "timeout" if left <= 0 else "gone"
                self.cv.wait(min(left, 0.5))
            return "ok"

    def release(self, user):
        with self.cv:
            self.running[user] -= 1
            self._grant()

    def depth(self):
        with self.cv:
            return sum(self.running.values()), len(self.waiting)


class Model:
    """One model = one Slurm job, found by its name. All state here is changed under `lock`; nothing in here waits
    on Slurm except submit() and cancel(), which run one command each."""

    def __init__(self, name, c, gw):
        unknown = set(c) - MODEL_KEYS
        if unknown:
            sys.exit(f"model {name!r}: unknown setting(s) {', '.join(sorted(unknown))}")
        if "sbatch" not in c:
            sys.exit(f"model {name!r}: `sbatch` (the job script) is missing")
        self.name, self.gw = name, gw
        self.sbatch = os.path.expanduser(c["sbatch"])
        self.batch_sbatch = os.path.expanduser(c["batch_sbatch"]) if c.get("batch_sbatch") else None
        self.job_name = c.get("job_name", "yawn-" + re.sub(r"[^A-Za-z0-9._-]+", "-", name))
        self.queue = FairQueue(int(c.get("slots", 32)), gw.meter, gw.max_waiting)  # 0 = no limit (and no fair order)
        self.weight = float(c.get("billing_weight", 1.0))
        # vLLM reuses computed prefixes between requests; timing then tells one user what another sent. A per-user
        # `cache_salt` keeps each user's cache apart. Turn off for a server that rejects the field.
        self.cache_salt = bool(c.get("cache_salt", True))
        self.startup_s = int(c.get("startup_s", 300))  # how long loading usually takes (for messages)
        self.start_timeout_s = int(c.get("start_timeout_s", 1800))  # a running job that is not ready by then is cancelled
        self.idle_stop_s = int(c.get("idle_stop_s", 1800))  # cancel the job after this long without a request; 0 = never
        self.retry_s = int(c.get("retry_after_failure_s", 600))  # crash-loop guard
        self.lock = threading.Lock()
        self.job = None  # dict(id, state, reason, node, start) as last seen in squeue, or None
        self.running_since = None  # (job id, monotonic time it was first seen RUNNING)
        self.submitted = None  # {"job", "token", "t"}: the last job submitted (token: how its worker proves itself)
        self.backend = None  # {"job", "host", "port", "secret", "beat", "ready"}: where the job's worker listens
        self.ready_job = None  # id of the job last seen ready
        self.ready_t = 0.0
        self.stopping = None  # id of a job being cancelled
        self.failure = None  # (job id, time): that job ended without (or right after) becoming ready
        self.ran = None  # id of the job last seen RUNNING
        self.unstarted = 0  # jobs in a row that left the queue before they were seen running
        now = time.monotonic()
        self.last_used = now  # last request started or finished, or the job became ready
        self.last_wanted = now  # last request that asked for this model
        self.inflight = 0

    # ---- what Slurm says (called by the poller) ----
    def observe(self, j, t_query):
        with self.lock:
            if self.submitted and self.submitted["t"] > t_query:
                return  # this squeue started before the sbatch: it cannot know the new job
            if not j and self.job and self.job["reason"] == "submitted" and time.monotonic() - self.submitted["t"] < 15:
                return  # just submitted: give Slurm a moment to list it
            prev, self.job = self.job, j
            if j and j["state"] == "RUNNING":
                if not self.running_since or self.running_since[0] != j["id"]:
                    self.running_since = (j["id"], time.monotonic())
                self.ran, self.unstarted = j["id"], 0
            else:
                self.running_since = None
            if prev and (not j or j["id"] != prev["id"]):  # that job is gone
                if self.backend and self.backend["job"] == prev["id"]:
                    self.backend = None
                if self.stopping == prev["id"]:  # cancelled by the gateway itself: not a failure
                    self.stopping = None
                    if self.submitted and self.submitted["job"] == prev["id"]:
                        self.submitted = None
                        self.gw.save_state()
                elif self.ready_job == prev["id"] and time.monotonic() - self.ready_t < 60:
                    # it answered and died within a minute: treat like a job that never answered
                    self.failure = (prev["id"], time.monotonic())
                    log(f"{self.name}: job {prev['id']} ended right after becoming ready; no new job for {self.retry_s}s")

    def set_backend(self, b):
        """The job's worker registered (called with the lock held)."""
        if b["ready"] and self.ready_job != b["job"]:  # newly ready (first start, or back after a requeue)
            now = time.monotonic()
            # the idle clock starts now, not at the last request to an earlier job
            self.ready_job, self.ready_t, self.last_used = b["job"], now, now
            log(f"{self.name}: job {b['job']} is ready at {b['host']}:{b['port']}")
        self.backend = b

    def fresh_backend(self):
        b = self.backend
        return b if b and time.monotonic() - b["beat"] < self.gw.beat_ttl_s else None

    def status(self, wake=True):
        """-> ("ready", job, backend) or (kind, seconds_to_wait, job) with kind in
        queued/held/waiting/restarting/loading/starting/failed/stopped. Reads what the poller saw; may submit."""
        with self.lock:
            now = time.monotonic()
            if wake:
                self.last_wanted = now
            j, b = self.job, self.fresh_backend()
            if j and b and b["job"] == j["id"] and b["ready"] and j["state"] == "RUNNING" and self.stopping != j["id"]:
                return ("ready", j, dict(b))
            if not self.gw.slurm_fresh():
                return ("waiting", 15, dict(id=j and j["id"], state="UNKNOWN", reason="Slurm is not answering",
                                            node="", start="N/A"))
            if j is None:
                if not wake:
                    return ("stopped", 0, None)
                if self.failure and (not self.submitted or self.failure[0] == self.submitted["job"]):
                    left = int(self.failure[1] + self.retry_s - now)
                    if left > 0:
                        return ("failed", left, None)
                elif self.submitted and self.submitted["job"] != self.ready_job:
                    # the last job submitted is gone and never became ready
                    if self.ran == self.submitted["job"] or self.unstarted:
                        # it crashed or was cancelled while starting, or it is the second in a row that never
                        # started. Do not resubmit on every request (a crash loop takes a GPU again and again).
                        self.failure = (self.submitted["job"], now)
                        log(f"{self.name}: job {self.submitted['job']} ended before it was ready; no new job for {self.retry_s}s")
                        return ("failed", self.retry_s, None)
                    # it left the queue before it ran (someone cancelled it while it waited): submit again, once
                    self.unstarted += 1
                    log(f"{self.name}: job {self.submitted['job']} left the queue before it started")
                return ("starting", self.startup_s, None) if self._submit() else ("failed", 60, None)
            if j["id"] == self.stopping or j["state"] == "COMPLETING":
                return ("restarting", 30, j)
            if j["state"] == "RUNNING":
                up = now - self.running_since[1] if self.running_since else 0
                return ("loading", max(10, int(self.startup_s - up)), j)
            if j["state"] in ("PENDING", "REQUEUED", "CONFIGURING"):
                r = j["reason"]  # say what Slurm says: only Resources/Priority mean "the GPUs are busy"
                if "held" in r.lower() or j["state"] == "REQUEUE_HOLD":
                    return ("held", 60, j)
                if j["state"] == "CONFIGURING":
                    return ("restarting", 30, j)
                if r in ("Resources", "Priority", "None", "submitted", ""):
                    return ("queued", 60, j)  # j["start"] holds Slurm's estimate while pending
                if r == "BeginTime":  # e.g. the pause Slurm inserts after a requeue
                    return ("restarting", 30, j)
                return ("waiting", 60, j)  # a reservation (maintenance), a limit, a dependency ...
            return ("queued", 30, j)  # SUSPENDED and the like: reported as queued

    def _submit(self):
        """sbatch the model's job (called with the lock held). The worker in the job gets a token to prove itself."""
        token = secrets.token_hex(24)
        env = {**os.environ, "YAWN_MODEL": self.name, "YAWN_GATEWAY": self.gw.url, "YAWN_TOKEN": token}
        try:
            r = subprocess.run(self.gw.slurm["sbatch"] + ["--parsable", "--export=ALL", "-J", self.job_name, self.sbatch],
                               capture_output=True, text=True, timeout=60, env=env)
            jid = r.stdout.strip().split(";")[0]
            if r.returncode != 0 or not jid:
                raise OSError(r.stderr.strip() or f"exit {r.returncode}")
        except (OSError, subprocess.SubprocessError) as e:
            log(f"{self.name}: sbatch failed: {e}")
            return None
        log(f"{self.name}: submitted job {jid}")
        self.submitted = {"job": jid, "token": token, "t": time.monotonic()}
        self.job = dict(id=jid, state="PENDING", reason="submitted", node="", start="N/A")
        self.failure = None
        self.gw.save_state()
        return jid

    def begin(self, backend):
        """A request is about to use `backend`: count it, unless the job is being stopped meanwhile."""
        with self.lock:
            b = self.backend
            if not b or b["job"] != backend["job"] or self.stopping == b["job"]:
                return False
            self.inflight += 1
            self.last_used = time.monotonic()
            return True

    def end(self):
        with self.lock:
            self.inflight -= 1
            self.last_used = time.monotonic()

    def drop_backend(self, job):
        """The worker of `job` failed during a request: do not use it until it registers again."""
        with self.lock:
            if self.backend and self.backend["job"] == job:
                self.backend = None

    def housekeeping(self):
        """Called by the poller: cancel a job that is idle, stuck while starting, or waiting for nobody."""
        with self.lock:
            j, now = self.job, time.monotonic()
            if not j or self.stopping == j["id"] or self.inflight:
                return
            b = self.fresh_backend()
            why = None
            if j["state"] == "RUNNING":
                ready = b and b["job"] == j["id"] and b["ready"]
                # idle = nobody used it and nobody asked for it (a request held while it loads counts as asking)
                if ready and self.idle_stop_s and now - max(self.last_used, self.last_wanted) > self.idle_stop_s:
                    why = f"idle {self.idle_stop_s}s"
                elif not ready and self.running_since and now - self.running_since[1] > self.start_timeout_s:
                    why = f"not ready after {self.start_timeout_s}s"
                    self.failure = (j["id"], now)
            elif j["state"] == "PENDING" and self.idle_stop_s and now - self.last_wanted > self.idle_stop_s:
                why = "nobody is waiting for it any more"
            if not why:
                return
            self.stopping, self.backend = j["id"], None  # from here on no request is sent to it
        try:
            r = subprocess.run(self.gw.slurm["scancel"] + [j["id"]], capture_output=True, text=True, timeout=30)
            if r.returncode != 0:
                raise OSError(r.stderr.strip() or f"exit {r.returncode}")
            log(f"{self.name}: cancelled job {j['id']} ({why})")
        except (OSError, subprocess.SubprocessError) as e:
            log(f"{self.name}: scancel {j['id']} failed: {e}")
            with self.lock:
                self.stopping = None


class KeysUnavailable(Exception):
    """The keys file could not be read and no earlier copy is known: not the caller's fault (503, not 401)."""


def private_place(path, remove=True):
    """Refuse a socket path in a directory others can write to (e.g. /tmp): they could create it first and collect keys.

    The directory must belong to this account (or root) and not be writable by group or others. A stale socket of this
    account is removed.
    """
    d = os.path.dirname(os.path.abspath(path))
    try:
        st = os.stat(d)
    except OSError as e:
        sys.exit(f"cannot use {path}: its directory is not there or not readable ({e})")
    if st.st_uid not in (os.getuid(), 0) or st.st_mode & 0o022:
        sys.exit(f"refusing to use {path}: its directory {d} can be written by others (use a directory only this account can"
                 " write to)")
    try:
        lst = os.lstat(path)
    except FileNotFoundError:
        return
    if not stat.S_ISSOCK(lst.st_mode) or lst.st_uid != os.getuid():
        sys.exit(f"refusing to replace {path}: it is not a socket owned by this account")
    if remove:
        os.remove(path)


class Gateway:
    def __init__(self, cfg):
        unknown = set(cfg) - TOP_KEYS
        if unknown:
            sys.exit(f"unknown setting(s) in the config: {', '.join(sorted(unknown))}")
        if not isinstance(cfg.get("models"), dict) or not cfg["models"]:
            sys.exit("the config needs `models`: {\"MODEL NAME\": {\"sbatch\": \"/path/to/job/script\"}}")
        self.cfg = cfg
        self.slurm = {k: (v if isinstance(v, list) else [v]) for k, v in
                      {"sbatch": "sbatch", "squeue": "squeue", "scancel": "scancel", **cfg.get("slurm", {})}.items()}
        listen = cfg.get("listen", "127.0.0.1:8080")
        self.listen = listen if isinstance(listen, list) else [listen]
        for a in self.listen:
            if not a.startswith("unix:") and not re.fullmatch(r".+:\d+", a):
                sys.exit(f"listen: {a!r} is neither HOST:PORT nor unix:/path")
        tcp = [a for a in self.listen if not a.startswith("unix:")]
        self.url = (cfg.get("url") or ("http://" + tcp[0] if tcp else "")).rstrip("/")  # where jobs and users reach the gateway
        self.poll_s = cfg.get("poll_s", 5)
        self.beat_ttl_s = cfg.get("beat_ttl_s", 45)  # a worker that has not registered for this long is not used
        self.max_waiting = int(cfg.get("max_waiting_per_user", 64))
        self.meter = Meter(cfg.get("usage", "usage.json"), float(cfg.get("halflife_days", 7)) * 86400)
        self.models = {n: Model(n, c, self) for n, c in cfg["models"].items()}
        names = [m.job_name for m in self.models.values()]
        if len(set(names)) != len(names):
            sys.exit("two models have the same job name; set `job_name` for one of them")
        self.keys_path = cfg.get("keys", "keys.json")
        self.ledger_path = cfg.get("ledger", "ledger.jsonl")
        self.state_path = cfg.get("state", "jobs.json")
        self.hold_s = cfg.get("hold_s", 20)  # how long to keep a request open while Slurm keeps the job waiting
        self.hold_loading_s = cfg.get("hold_loading_s", 300)  # ... while the job runs and the model loads
        self.queue_wait_s = cfg.get("queue_wait_s", 600)  # ... while waiting for a slot behind other requests
        self.backend_timeout_s = cfg.get("backend_timeout_s", 600)  # ... for the model to send the next bytes
        self.own_jobs = bool(cfg.get("own_jobs", True))
        self.busy_status = cfg.get("busy_status", 503)
        self.busy_hint = cfg.get("busy_hint", "")  # e.g. " To use it now as your own job, run: llm-up {model}"
        self.msg = {**DEFAULT_MSG, **cfg.get("messages", {})}
        self.paths = set(cfg.get("paths", DEFAULT_PATHS))  # only the model API is passed on
        self.max_body = int(cfg.get("max_body_mb", 32)) * 2 ** 20
        self.client_timeout_s = cfg.get("client_timeout_s", 60)  # a client that sends nothing for this long is dropped
        self.conn_slots = threading.BoundedSemaphore(int(cfg.get("max_connections", 512)))
        self.keys_lock = threading.Lock()
        self.ledger_lock = threading.Lock()
        self.state_lock = threading.Lock()
        self.rid = itertools.count()
        self.keys = None  # ((mtime_ns, size, inode), {key: user}): the keys file as last read
        self.salt_key = secrets.token_bytes(32)  # new at every start: cached prefixes simply miss once
        self.own = {}  # (user, model name) -> backend of that user's own job
        self.own_lock = threading.Lock()
        self.slurm_ok_t = None  # monotonic time of the last squeue that worked
        self.load_state()

    # ---- state that must survive a restart: which job was submitted for a model, and its token ----
    def load_state(self):
        try:
            with open(self.state_path) as f:
                d = json.load(f)
            if not isinstance(d, dict):
                raise ValueError("not the form this gateway writes")
        except FileNotFoundError:
            return
        except (OSError, ValueError, RecursionError) as e:
            return log(f"job state from {self.state_path} not used: {e!r}")
        for name, s in d.items():
            m = self.models.get(name)
            if m and isinstance(s, dict) and s.get("job") and isinstance(s.get("token"), str) and s["token"]:
                m.submitted = {"job": str(s["job"]), "token": s["token"], "t": 0.0}

    def save_state(self):
        with self.state_lock:
            d = {n: {"job": m.submitted["job"], "token": m.submitted["token"]} for n, m in self.models.items() if m.submitted}
            try:
                write_private(self.state_path, json.dumps(d))
            except OSError as e:
                log(f"could not save {self.state_path}: {e}")

    # ---- Slurm: one squeue for everything, in the background ----
    def slurm_fresh(self):
        return self.slurm_ok_t is not None and time.monotonic() - self.slurm_ok_t < max(30, 6 * self.poll_s)

    def poll_once(self):
        t0 = time.monotonic()
        try:
            r = subprocess.run(self.slurm["squeue"] + ["-h", "-u", getpass.getuser(), "-o", "%i|%j|%T|%r|%N|%S"],
                               capture_output=True, text=True, timeout=60)
            if r.returncode != 0:
                raise OSError(r.stderr.strip() or f"exit {r.returncode}")
        except (OSError, subprocess.SubprocessError) as e:
            # "squeue failed" is not "there is no job": keep the last known state, and never submit on a transient Slurm error
            return log(f"squeue failed ({e}); keeping the last known state")
        seen = {}
        for line in r.stdout.splitlines():
            f = line.strip().split("|")
            if len(f) == 6:
                seen.setdefault(f[1], []).append(dict(id=f[0], state=f[2], reason=f[3], node=f[4], start=f[5]))
        for m in self.models.values():
            jobs = sorted(seen.get(m.job_name, []), key=lambda j: j["state"] != "RUNNING")  # prefer a running one
            m.observe(jobs[0] if jobs else None, t0)
        self.slurm_ok_t = time.monotonic()

    def poller(self):
        n = 0
        while True:
            try:
                self.poll_once()
                for m in self.models.values():
                    m.housekeeping()
                n += 1
                if n % 6 == 0:
                    self.meter.save()
            except Exception:  # noqa: BLE001
                log("poller: " + traceback.format_exc())
            time.sleep(self.poll_s)

    # ---- who ----
    def salt(self, user, client_salt=None):
        """The cache salt sent to a shared server for `user`: the same for all of that user's requests, different
        between users, not guessable by them. A salt the client sent keeps its effect, but only within that user."""
        msg = user.encode() + b"\0" + (client_salt.encode() if isinstance(client_salt, str) else b"")
        return hmac.new(self.salt_key, msg, hashlib.sha256).hexdigest()[:32]

    def read_keys(self):
        """The keys file as a dict. A missing file is an empty set; a file that cannot be read or parsed is an error."""
        try:
            st = os.stat(self.keys_path)
        except FileNotFoundError:
            return {}
        sig = (st.st_mtime_ns, st.st_size, st.st_ino)
        if self.keys and self.keys[0] == sig:
            return self.keys[1]
        with open(self.keys_path) as f:
            keys = json.load(f)
        if not isinstance(keys, dict):
            raise ValueError("not a JSON object")
        self.keys = (sig, keys)
        return keys

    def load_keys(self):
        try:
            return self.read_keys()
        except (OSError, ValueError) as e:
            if self.keys:  # keep using the last good copy, and say so
                log(f"cannot read {self.keys_path} ({e}); using the keys read before")
                return self.keys[1]
            raise KeysUnavailable(str(e)) from e

    def user(self, auth):
        if not auth or not auth.startswith("Bearer "):
            return None
        return self.load_keys().get(auth[7:].strip())

    def key_for(self, user, new=False):
        """The user's key (made on first ask). Called only for a user the kernel vouched for.
        If the keys file cannot be read right now, nothing is written: one bad read must not wipe everyone's keys."""
        with self.keys_lock:
            keys = dict(self.read_keys())  # raises on an unreadable or broken file
            mine = [k for k, u in keys.items() if u == user]
            if mine and not new:
                return mine[0]
            for k in mine:
                del keys[k]
            key = "yawn-" + secrets.token_hex(20)
            keys[key] = user
            write_private(self.keys_path, json.dumps(keys))
            self.keys = None
            return key

    def info(self, user):
        """What the user tools need: the user's key, where the gateway is, and the job scripts for their own jobs."""
        models = {n: {"sbatch": os.path.abspath(m.sbatch),
                      "batch_sbatch": os.path.abspath(m.batch_sbatch) if m.batch_sbatch else None}
                  for n, m in self.models.items()}
        return {"key": self.key_for(user), "url": self.url, "models": models, "own_jobs": self.own_jobs}

    def key_server(self, path):
        """Unix socket on the gateway's host: `llm-key` connects, the kernel says who it is, and that user's key is handed out."""
        private_place(path)
        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        srv.bind(path)
        os.chmod(path, 0o666)
        srv.listen(64)

        def serve(c):
            try:
                c.settimeout(5)
                _pid, uid, _gid = struct.unpack("3i", c.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i")))
                user = pwd.getpwuid(uid).pw_name
                cmd = c.recv(64).decode(errors="replace").strip()
                out = json.dumps(self.info(user)) if cmd == "info" else self.key_for(user, new=(cmd == "new"))
                c.sendall((out + "\n").encode())
            except Exception as e:  # noqa: BLE001
                log(f"key request failed: {e}")
                try:
                    c.sendall(b"ERROR the gateway could not read or write its keys; tell the administrators\n")
                except OSError:
                    pass
            finally:
                c.close()

        while True:
            c, _ = srv.accept()
            threading.Thread(target=serve, args=(c,), daemon=True).start()

    # ---- where: workers register ----
    def register(self, auth, peer, d):
        """A worker says where its job serves a model. -> (reply, None) or (None, (http status, why)).

        The address is the one the registration came from, never one named in the request, so the gateway only
        ever connects to the machine that registered. The token decides whose job it is: the token given to the
        shared job at submission, or a user's key (their own job, which then serves only them)."""
        if not peer or peer == "unix":
            return None, (400, "register over the gateway's TCP address")
        m = self.models.get(d.get("model"))
        token = auth[7:].strip() if auth and auth.startswith("Bearer ") else ""
        try:
            port, job = int(d.get("port")), str(d.get("job"))
            if not 0 < port < 65536 or not isinstance(d.get("secret"), str):
                raise ValueError
        except (TypeError, ValueError):
            return None, (400, "port and secret are required")
        if not m or not token:
            return None, (400 if token else 401, "unknown model" if token else "missing token")
        now = time.monotonic()
        b = {"job": job, "host": peer, "port": port, "secret": d["secret"], "beat": now, "ready": bool(d.get("ready", True))}
        with m.lock:
            shared = m.submitted and hmac.compare_digest(m.submitted["token"], token)
            if shared:
                if m.submitted["job"] != job:
                    return None, (403, "this token belongs to another job")
                if m.stopping == job:
                    return {"stop": True, "idle_s": 0}, None
                m.set_backend(b)
                return {"stop": False, "idle_s": 0}, None
        user = self.user(auth)
        if not user:
            return None, (401, "unknown token")
        if not self.own_jobs:
            return None, (403, "own jobs are turned off on this gateway")
        with self.own_lock:
            old = self.own.get((user, m.name))
            if not old or old["job"] != job:
                log(f"own job: {user} serves {m.name} from job {job} at {peer}:{port}")
                b["last_used"] = now
            else:
                b["last_used"] = old["last_used"]
            self.own[(user, m.name)] = b
            idle = now - b["last_used"]
        return {"idle_s": round(idle, 1), "shared_ready": m.status(wake=False)[0] == "ready", "stop": False}, None

    def unregister(self, auth, d):
        m = self.models.get(d.get("model"))
        token = auth[7:].strip() if auth and auth.startswith("Bearer ") else ""
        if not m or not token:
            return
        job = str(d.get("job"))
        with m.lock:
            if m.submitted and hmac.compare_digest(m.submitted["token"], token):
                if m.backend and m.backend["job"] == job:
                    m.backend = None
                return
        try:
            user = self.user(auth)
        except KeysUnavailable:
            return
        if user:
            self.drop_own(user, m.name, job)

    def drop_own(self, user, model, job):
        with self.own_lock:
            b = self.own.get((user, model))
            if b and b["job"] == job:
                del self.own[(user, model)]
                log(f"own job gone: {user} {model} job {job}")

    def own_backend(self, user, model):
        with self.own_lock:
            b = self.own.get((user, model))
            if b and time.monotonic() - b["beat"] > self.beat_ttl_s:
                del self.own[(user, model)]
                b = None
            return b if b and b["ready"] else None

    def record(self, **rec):
        rec = {"t": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"), **rec}
        with self.ledger_lock, open(self.ledger_path, "a") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def make_handler(gw):
    class H(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.0"  # one request per connection; the body ends when the connection closes
        timeout = gw.client_timeout_s  # applies to every read and write on the client's socket

        def log_message(self, fmt, *a):
            pass

        def send_json(self, code, obj, headers=None):
            body = json.dumps(obj, ensure_ascii=False).encode()
            try:
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                for k, v in (headers or {}).items():
                    self.send_header(k, v)
                self.end_headers()
                self.wfile.write(body)
            except OSError:  # the client is gone; nothing to tell
                pass

        def error(self, code, kind, message, headers=None):
            self.send_json(code, {"error": {"message": message, "type": kind, "code": kind}}, headers)

        def client_gone(self):
            """Has the client closed its side? (It sends nothing after its request, so readable means closed.)"""
            try:
                r, _, _ = select.select([self.connection], [], [], 0)
                return bool(r) and self.connection.recv(1, socket.MSG_PEEK) == b""
            except (OSError, ValueError):
                return True

        def who(self):
            """-> user, or None after an error reply has been sent."""
            try:
                user = gw.user(self.headers.get("Authorization"))
            except KeysUnavailable as e:
                log(f"keys unavailable: {e}")
                self.error(503, "gateway_error", "The gateway cannot read its keys right now. Try again later.",
                           {"Retry-After": "30"})
                return None
            if not user:
                self.error(401, "invalid_api_key", "Unknown or missing API key. Get yours with: llm-key")
            return user

        def read_json(self):
            """-> the request body as a dict, or None after an error reply has been sent."""
            try:
                n = int(self.headers.get("Content-Length") or 0)
            except ValueError:
                n = -1
            if n < 0 or self.headers.get("Transfer-Encoding"):
                return self.error(400, "invalid_request", "Send the body with a Content-Length.")
            if n > gw.max_body:
                return self.error(413, "request_too_large", f"The request body is larger than {gw.max_body >> 20} MB.")
            try:
                body = json.loads(self.rfile.read(n) or b"{}")
            except (ValueError, OSError):
                return self.error(400, "invalid_json", "The request body is not JSON.")
            if not isinstance(body, dict):
                return self.error(400, "invalid_json", "The request body must be a JSON object.")
            return body

        def do_GET(self):
            if self.path == "/gw/status":  # what the poller last saw; asks nothing of Slurm
                out = {}
                for n, m in gw.models.items():
                    st = m.status(wake=False)
                    j = st[1] if st[0] == "ready" else st[2]
                    running, waiting = m.queue.depth()
                    out[n] = {"state": st[0], "job": j and j["id"], "slurm_state": j and j["state"],
                              "reason": j and j["reason"], "in_flight": running, "waiting": waiting}
                return self.send_json(200, out)
            if self.path.rstrip("/") == "/v1/models":
                if not self.who():
                    return
                data = [{"id": n, "object": "model", "owned_by": "yawn"} for n in gw.models]
                return self.send_json(200, {"object": "list", "data": data})
            return self.error(404, "not_found", "Not found.")

        def do_POST(self):
            try:
                self.handle_post()
            except Exception:  # noqa: BLE001  never leave a client without an answer
                log("request failed: " + traceback.format_exc())
                self.error(500, "gateway_error", "The gateway hit an internal error. Please tell the administrators.")

        def handle_post(self):
            t0 = time.monotonic()
            path = self.path.split("?", 1)[0]
            auth = self.headers.get("Authorization")
            if path in ("/gw/register", "/gw/unregister"):  # workers: their own tokens, not user keys
                body = self.read_json()
                if body is None:
                    return
                if path == "/gw/unregister":
                    gw.unregister(auth, body)
                    return self.send_json(200, {})
                try:
                    out, err = gw.register(auth, self.client_address[0], body)
                except KeysUnavailable:
                    out, err = None, (503, "the gateway cannot read its keys right now")
                return self.send_json(200, out) if out else self.error(err[0], "bad_registration", err[1])
            user = self.who()
            if not user:
                return
            body = self.read_json()
            if body is None:
                return
            if path not in gw.paths:  # only the model API is passed on, nothing else the server may have
                return self.error(404, "not_found", f"Not found: {path}")
            name = body.get("model")
            m = gw.models.get(name) if isinstance(name, str) else None
            if not m:
                return self.error(404, "model_not_found", f"Unknown model {name!r}. See GET /v1/models.")
            st = None
            for _attempt in range(3):  # a backend that vanishes before its first byte is retried without the client noticing
                ta = time.monotonic()
                # when: keep the request open while the model loads; say why only if Slurm keeps the job waiting.
                # The shared model is used whenever it is ready; otherwise the caller's own job, if they started one.
                st = m.status()
                own = gw.own_backend(user, name)
                while st[0] not in ("ready", "failed") and not own:
                    limit = gw.hold_loading_s if st[0] in ("loading", "restarting") else gw.hold_s
                    if time.monotonic() - ta >= limit:
                        break
                    if self.client_gone():
                        return gw.record(user=user, model=name, path=path, status="client_gone", http=None,
                                         s=round(time.monotonic() - t0, 1))
                    time.sleep(1)
                    was, st = st[0], m.status()
                    if st[0] != was:  # each state gets its own patience (e.g. stopping -> starting -> queued -> loading)
                        ta = time.monotonic()
                    own = gw.own_backend(user, name)
                if st[0] == "ready":
                    # order: wait here for a slot; the least-served waiting user goes next
                    got = m.queue.acquire(user, gw.queue_wait_s, self.client_gone)
                    if got == "ok" and self.client_gone():  # left just as its turn came
                        m.queue.release(user)
                        got = "gone"
                    if got != "ok":
                        gw.record(user=user, model=name, path=path, http=None if got == "gone" else 429 if got == "full" else gw.busy_status,
                                  status={"gone": "client_gone", "full": "too_many_waiting"}.get(got, "queue_timeout"),
                                  s=round(time.monotonic() - t0, 1))
                        if got == "gone":
                            return
                        if got == "full":
                            return self.error(429, "too_many_requests", "You already have many requests waiting for this"
                                              " model. Wait for some to finish.", {"Retry-After": "10"})
                        return self.error(gw.busy_status, "queue_timeout", "Too many requests are waiting for this model."
                                          " Try again later.", {"Retry-After": "60"})
                    res = "retry"
                    try:
                        if m.begin(st[2]):  # False: the job started stopping in the meantime; look again
                            rid = next(gw.rid)
                            gw.meter.start(rid, user, m.name, m.weight)
                            try:
                                res = self.forward(user, m, path, st[2], body, t0, rid)
                            finally:
                                gw.meter.end(rid)
                                m.end()
                    finally:
                        m.queue.release(user)
                elif own:  # their own job: Slurm already charges them for it; nothing is booked here
                    own["last_used"] = time.monotonic()
                    res = self.forward(user, m, path, own, body, t0, None, own=True)
                    own["last_used"] = time.monotonic()
                else:
                    break
                if res != "retry":
                    return
                time.sleep(1)
            kind, wait = (st[0], max(1, min(60, st[1]))) if st[0] != "ready" else ("restarting", 30)
            job = st[2] if st[0] != "ready" else st[1]
            gw.record(user=user, model=name, path=path, status=kind, http=gw.busy_status,
                      job=job and job["id"] or None, s=round(time.monotonic() - t0, 1))
            hint = gw.busy_hint.format(model=name) if kind in ("queued", "starting", "failed", "held", "waiting") else ""
            return self.error(gw.busy_status, "model_" + kind,
                              gw.msg[kind].format(s=st[1] if st[0] != "ready" else 30, at=expected(job),
                                                  reason=job["reason"] if job else "") + hint,
                              {"Retry-After": str(wait)})

        def forward(self, user, m, path, b, body, t0, rid, own=False):
            """Send the request to the worker at `b`. Returns "retry" if nothing was sent back and trying again makes sense.

            Errors on the client's side (it hung up) and on the server's side (the model went away) are kept apart:
            only the second stops the gateway from using that worker until it registers again.
            """
            stream = bool(body.get("stream"))
            opts = body.get("stream_options") if isinstance(body.get("stream_options"), dict) else {}
            asked_usage = bool(opts.get("include_usage"))
            send = dict(body)
            if stream and not asked_usage:  # the token counts are needed; the extra chunk is hidden again below
                send["stream_options"] = {**opts, "include_usage": True}
            if not own and m.cache_salt:  # a user's own job serves only them: nothing to keep apart
                send["cache_salt"] = gw.salt(user, body.get("cache_salt"))
            data = json.dumps(send).encode()
            hdr = {k: v for k, v in self.headers.items() if k.lower() not in HOP and k.lower() not in ("expect", "accept-encoding")}
            hdr["Content-Length"] = str(len(data))
            hdr["Authorization"] = "Bearer " + b["secret"]  # the worker only answers the gateway
            usage, status, code, retry = {}, "ok", 0, False
            client = [True]  # still there?

            def out(buf, flush=False):
                if client[0]:
                    try:
                        self.wfile.write(buf)
                        if flush:
                            self.wfile.flush()
                    except OSError:
                        client[0] = False
                return client[0]

            c = None
            try:
                c = HTTPConnection(b["host"], b["port"], timeout=gw.backend_timeout_s)
                c.request("POST", path, body=data, headers=hdr)
                r = c.getresponse()
                if r.status == 502:  # the worker is there but its model server is not: same as a vanished backend
                    raise HTTPException("the worker says its model server is not answering")
                code = r.status
                try:
                    self.send_response(r.status)
                    for k, v in r.getheaders():
                        if k.lower() not in HOP and k.lower() not in ("date", "server"):  # the gateway sends its own
                            self.send_header(k, v)
                    self.end_headers()
                except OSError:
                    client[0] = False
                if stream and r.status == 200:
                    done = False
                    while client[0]:
                        line = r.readline()
                        if not line:
                            break
                        if line.strip() == b"data: [DONE]":
                            done = True
                        elif line.startswith(b"data: {"):
                            try:
                                ev = json.loads(line[6:])
                            except ValueError:
                                ev = {}
                            if isinstance(ev, dict) and ev.get("usage"):
                                usage = ev["usage"]
                                if not asked_usage and not ev.get("choices"):
                                    r.readline()  # the blank line after the event
                                    continue
                        out(line, flush=line in (b"\n", b"\r\n"))
                    if not client[0]:
                        status = "client_gone"
                    elif not done:  # the stream ended without its last line: the model went away
                        status = "interrupted"
                        out(b"data: " + json.dumps(INTERRUPTED).encode() + b"\n\n")
                else:
                    payload = r.read()
                    out(payload)
                    try:
                        usage = json.loads(payload).get("usage") or {}
                    except (ValueError, AttributeError):
                        pass
                    status = "client_gone" if not client[0] else "ok" if r.status == 200 else f"backend_{r.status}"
            except socket.timeout:  # the model is there but too slow: do not send the same work again
                status = "backend_timeout"
                log(f"{m.name}: no answer from job {b['job']} within {gw.backend_timeout_s}s")
                if code and stream:
                    out(b"data: " + json.dumps(INTERRUPTED).encode() + b"\n\n")
            except (OSError, ValueError, HTTPException) as e:  # the server's side: client writes never raise
                status, retry = "interrupted", True
                log(f"{m.name}: forwarding to job {b['job']} failed: {e}")
                if code and stream:
                    out(b"data: " + json.dumps(INTERRUPTED).encode() + b"\n\n")
            finally:
                if c:
                    c.close()  # if the client left mid-answer, this tells the server to stop generating
            if status == "interrupted":  # do not use this worker until it registers again
                if own:
                    gw.drop_own(user, m.name, b["job"])
                else:
                    m.drop_backend(b["job"])
            billed = gw.meter.end(rid) if rid is not None else None
            if code == 0:  # nothing reached the client
                status = "retried" if retry else status
            usage = usage if isinstance(usage, dict) else {}
            gw.record(user=user, model=m.name, path=path, status=status, http=code or None,
                      backend="own" if own else "shared", billed=None if billed is None else round(billed, 3),
                      job=b["job"], stream=stream, prompt_tokens=usage.get("prompt_tokens"),
                      completion_tokens=usage.get("completion_tokens"), s=round(time.monotonic() - t0, 1))
            if code == 0 and client[0]:
                if retry:
                    return "retry"
                self.error(504, "model_timeout", "The model did not answer in time.")
            return "done"

    return H


class Capped:
    """At most N connections at a time across all listeners; the next one gets a short 503 instead of a thread."""

    daemon_threads = True
    request_queue_size = 128  # the default backlog of 5 resets connections when many requests arrive at once
    slots = None

    def process_request(self, request, client_address):
        if not self.slots.acquire(blocking=False):
            try:
                body = json.dumps({"error": {"message": "The gateway is handling too many connections. Try again shortly.",
                                             "type": "too_many_connections", "code": "too_many_connections"}}).encode()
                request.sendall(b"HTTP/1.0 503 Service Unavailable\r\nContent-Type: application/json\r\nRetry-After: 5\r\n"
                                b"Content-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body)
            except OSError:
                pass
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except Exception:
            self.slots.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self.slots.release()


class TCPServer(Capped, ThreadingHTTPServer):
    pass


class UnixServer(Capped, socketserver.ThreadingUnixStreamServer):
    """`"listen": "unix:/path/http.sock"`: users reach it with `ssh -L 8080:/path/http.sock HOST`.

    A file in a directory only this account can write to cannot be taken over while the gateway is down, unlike a TCP port.
    """

    def get_request(self):
        request, _ = super().get_request()
        return request, ("unix", 0)


def check(cfg, gw):
    """--check: say what the gateway would do with this config, and what is missing. Changes nothing."""
    bad = 0

    def say(ok, text):
        nonlocal bad
        bad += not ok
        print(("ok    " if ok else "ERROR ") + text)

    def place(path):  # -> None if a socket may live there, else why not
        try:
            private_place(os.path.expanduser(path), remove=False)
        except SystemExit as e:
            return str(e)

    for addr in gw.listen:
        if addr.startswith("unix:"):
            why = place(addr[5:])
        else:
            host, port = addr.rsplit(":", 1)
            why = None
            try:
                with socket.socket(socket.AF_INET6 if ":" in host else socket.AF_INET) as s:
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    s.bind((host, int(port)))
            except OSError as e:
                why = f"cannot listen there ({e})"
        say(not why, f"listen on {addr}" + (f": {why}" if why else ""))
    say(bool(gw.url), f"jobs and user tools are told to reach the gateway at {gw.url or '(nothing: set `url` or a TCP `listen`)'}")
    if cfg.get("key_socket"):
        why = place(cfg["key_socket"])
        say(not why, f"keys handed out at {cfg['key_socket']}" + (f": {why}" if why else ""))
    else:
        say(True, "no key_socket: keys must be put into the keys file by hand")
    for name in ("sbatch", "squeue", "scancel"):
        exe = gw.slurm[name][0]
        say(bool(shutil.which(exe)), f"Slurm command `{exe}`" + ("" if shutil.which(exe) else " not found"))
    for name, m in gw.models.items():
        say(os.path.isfile(m.sbatch), f"model {name}: job script {m.sbatch}" + ("" if os.path.isfile(m.sbatch) else " not found"))
        say(True, f"model {name}: job name {m.job_name}, "
            f"{'no limit' if not m.queue.slots else m.queue.slots} requests at a time, "
            f"{'cancelled after ' + str(m.idle_stop_s) + ' s idle' if m.idle_stop_s else 'never cancelled by the gateway'}")
    say(os.access(".", os.W_OK), f"keys, the request log and usage are kept in {os.getcwd()}")
    print("config looks usable" if not bad else f"{bad} problem(s)")
    return 1 if bad else 0


def main():
    ap = argparse.ArgumentParser(prog="yawn-gateway", description="An OpenAI-compatible gateway for model servers that run as Slurm jobs.")
    ap.add_argument("config", help="the gateway's JSON config (see examples/config.json)")
    ap.add_argument("--check", action="store_true", help="read the config, say what would be done and what is missing, and exit")
    ap.add_argument("--version", action="version", version=VERSION)
    args = ap.parse_args()
    try:
        with open(args.config) as f:
            cfg = json.load(f)
    except (OSError, ValueError) as e:
        sys.exit(f"cannot read the config {args.config}: {e}")
    if not isinstance(cfg, dict):
        sys.exit(f"the config {args.config} must be a JSON object")
    os.umask(0o077)  # keys, the request log and usage are for this account only (jobs inherit this, too)
    gw = Gateway(cfg)
    if args.check:
        sys.exit(check(cfg, gw))
    if not gw.url:
        sys.exit("set `url` (or a TCP `listen` address): the jobs need an address to register at")
    handler = make_handler(gw)
    servers = []
    for addr in gw.listen:  # e.g. the cluster's internal address, and optionally a socket file for users
        if addr.startswith("unix:"):
            path = os.path.expanduser(addr[5:])
            private_place(path)
            srv = UnixServer(path, handler)
            os.chmod(path, 0o666)
        else:
            host, port = addr.rsplit(":", 1)
            srv = TCPServer((host, int(port)), handler)
        srv.slots = gw.conn_slots
        servers.append(srv)
    if cfg.get("key_socket"):
        path = os.path.expanduser(cfg["key_socket"])
        private_place(path)  # checked here too, so a bad place stops the start instead of a background thread
        threading.Thread(target=gw.key_server, args=(path,), daemon=True).start()
    threading.Thread(target=gw.poller, daemon=True).start()
    signal_stop = threading.Event()

    def bye(*_):
        signal_stop.set()

    signal.signal(signal.SIGTERM, bye)
    signal.signal(signal.SIGINT, bye)
    log(f"YAWN gateway {VERSION} listening on {', '.join(gw.listen)}; models: {', '.join(gw.models)}")
    for srv in servers:
        threading.Thread(target=srv.serve_forever, daemon=True).start()
    signal_stop.wait()
    try:
        gw.meter.save()  # the model's jobs go on; the next start finds them again by name
    except OSError:
        pass
    log("stopped")


if __name__ == "__main__":
    main()
