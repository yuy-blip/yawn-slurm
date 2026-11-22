# SPDX-FileCopyrightText: 2026 The YAWN authors
# SPDX-License-Identifier: Apache-2.0
"""Shared bits of the user tools (llm-key, llm-up, llm-batch). Everything they need comes from the gateway itself,
over the key socket: the caller's key, the gateway's address, and the job scripts. There is no site file."""
import json
import os
import re
import socket
import subprocess
import sys

if sys.version_info < (3, 8):
    sys.exit("these tools need Python 3.8 or later (this is %d.%d)" % sys.version_info[:2])

SOCKET = os.path.expanduser(os.environ.get("YAWN_SOCKET", "/run/yawn/key.sock"))


def ask(cmd, path=None):
    """One line from the gateway's key socket. The kernel tells the gateway who is asking."""
    path = path or SOCKET
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        s.connect(path)
        s.sendall(cmd.encode() + b"\n")
        line = s.makefile().readline().strip()
    except OSError as e:
        sys.exit(f"the gateway is not answering at {path}: {e}\n(run this on the host where the gateway runs, or set YAWN_SOCKET)")
    finally:
        s.close()
    if not line or line.startswith("ERROR"):
        sys.exit(line[6:] or "the gateway gave no answer")
    return line


def info():
    """{"key", "url", "models": {NAME: {"sbatch", "batch_sbatch"}}, "own_jobs"} for the calling user."""
    return json.loads(ask("info"))


def cmd(name):
    """A Slurm command; YAWN_SBATCH etc. replace it (used by the tests)."""
    return os.environ.get("YAWN_" + name.upper(), name).split()


def slug(model):
    return re.sub(r"[^A-Za-z0-9._-]+", "-", model)


def model_entry(i, model):
    if model not in i["models"]:
        sys.exit(f"unknown model {model!r}. Available: {', '.join(i['models']) or '(none)'}")
    return i["models"][model]


def my_jobs(name):
    out = subprocess.run(cmd("squeue") + ["-h", "-u", os.environ.get("USER", ""), "-n", name, "-o", "%i|%T|%r|%N|%S"],
                         capture_output=True, text=True).stdout
    return [line.split("|") for line in out.splitlines() if line.strip()]


def submit(name, script, env, qos=None, time_limit=None):
    c = cmd("sbatch") + ["--parsable", "--export=ALL", "-J", name]
    if qos:
        c.append(f"--qos={qos}")
    if time_limit:
        c.append(f"--time={time_limit}")
    r = subprocess.run(c + [script], capture_output=True, text=True, env={**os.environ, **env})
    jid = r.stdout.strip().split(";")[0]
    if r.returncode != 0 or not jid:
        sys.exit(f"sbatch failed: {r.stderr.strip() or r.stdout.strip()}")
    return jid
