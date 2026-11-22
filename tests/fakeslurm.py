# SPDX-FileCopyrightText: 2026 The YAWN authors
# SPDX-License-Identifier: Apache-2.0
"""A tiny stand-in for Slurm in tests. State lives in $FAKESLURM_DIR.

  fakeslurm.py sbatch --parsable -J NAME FILE   -> job id; a supervisor runs FILE (bash) when the job "starts"
  fakeslurm.py squeue -h -u USER -o FMT        -> "%i|%j|%T|%r|%N|%S" lines (every job; what the gateway asks)
  fakeslurm.py squeue -h -n NAME -o FMT        -> "%i|%T|%r|%N|%S" lines (jobs with that name; used by the tests)
  fakeslurm.py scancel ID
  fakeslurm.py preempt ID      (test) kill it, PENDING for FAKESLURM_REQUEUE_DELAY s, then queue again (--requeue)
  fakeslurm.py busy on|off     (test) others hold every GPU: pending jobs stay pending (reason Resources)
  fakeslurm.py count           (test) how many sbatch calls so far
Env: FAKESLURM_PEND (s pending before start, default 1), FAKESLURM_REQUEUE_DELAY (default 2).
"""
import json
import os
import signal
import subprocess
import sys
import time

D = os.environ["FAKESLURM_DIR"]
PEND = float(os.environ.get("FAKESLURM_PEND", "1"))
DELAY = float(os.environ.get("FAKESLURM_REQUEUE_DELAY", "2"))
GRACE = float(os.environ.get("FAKESLURM_KILL_WAIT", "3"))
os.makedirs(D, exist_ok=True)


def path(*p):
    return os.path.join(D, *p)


def save(job):
    tmp = path(f"job.{job['id']}.tmp")
    json.dump(job, open(tmp, "w"))
    os.replace(tmp, path(f"job.{job['id']}"))


def load(jid):
    try:
        return json.load(open(path(f"job.{jid}")))
    except (OSError, ValueError):
        return None


def now():
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def stop(proc):
    """Like Slurm: SIGTERM to the whole job, then SIGKILL after a grace period."""
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    end = time.time() + GRACE
    while time.time() < end and proc.poll() is None:
        time.sleep(0.05)
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def supervise(jid):
    proc = None
    while True:
        job = load(jid)
        if job is None:
            return
        if os.path.exists(path(f"cancel.{jid}")):
            if proc:
                stop(proc)
            os.remove(path(f"job.{jid}"))
            return
        if os.path.exists(path(f"preempt.{jid}")):
            os.remove(path(f"preempt.{jid}"))
            if proc:
                stop(proc)
                proc = None
            job.update(state="PENDING", reason="BeginTime", node="", start="N/A", eligible=time.time() + DELAY)
            save(job)
        if job["state"] == "PENDING":
            own = job["name"].startswith(("yawn-own-", "yawn-batch-"))  # tests: the user's own QoS still gets a GPU
            if time.time() >= job["eligible"] and (own or not os.path.exists(path("busy"))):
                proc = subprocess.Popen(["bash", job["file"]], start_new_session=True, env={**os.environ, "SLURM_JOB_ID": jid},
                                        stdout=open(path(f"out.{jid}"), "a"), stderr=subprocess.STDOUT)
                job.update(state="RUNNING", reason="None", node="127.0.0.1", start=now())
                save(job)
            elif time.time() >= job["eligible"] and job["reason"] != "Resources":
                job.update(reason="Resources")
                save(job)
        elif proc and proc.poll() is not None:
            os.remove(path(f"job.{jid}"))  # finished on its own
            return
        time.sleep(0.2)


def main():
    cmd, args = sys.argv[1], sys.argv[2:]
    if cmd == "sbatch":
        if os.path.exists(path("sbatch_fails")):  # (test)
            sys.exit("sbatch: error: Batch job submission failed: Socket timed out on send/recv operation")
        name = args[args.index("-J") + 1]
        jid = str(int(open(path("next")).read()) if os.path.exists(path("next")) else 1000)
        open(path("next"), "w").write(str(int(jid) + 1))
        save(dict(id=jid, name=name, file=os.path.abspath(args[-1]), state="PENDING", reason="Priority",
                  node="", start="N/A", eligible=time.time() + PEND))
        subprocess.Popen([sys.executable, __file__, "_supervise", jid], start_new_session=True,
                         stdout=subprocess.DEVNULL, stderr=open(path(f"sup.{jid}"), "a"))
        print(jid)
    elif cmd == "_supervise":
        supervise(args[0])
    elif cmd == "squeue":
        if os.path.exists(path("squeue_fails")):  # (test) a transient Slurm error
            sys.exit("squeue: error: Unable to contact slurm controller")
        if os.path.exists(path("squeue_slow")):  # (test) a slow controller
            time.sleep(float(open(path("squeue_slow")).read() or 0))
        name = args[args.index("-n") + 1] if "-n" in args else None
        for f in sorted(os.listdir(D)):
            if f.startswith("job.") and not f.endswith(".tmp"):
                j = load(f[4:])
                if j and name is None:  # the gateway's one query: every job, with its name
                    print("|".join([j["id"], j["name"], j["state"], j["reason"], j["node"], j["start"]]))
                elif j and j["name"] == name:
                    print("|".join([j["id"], j["state"], j["reason"], j["node"], j["start"]]))
    elif cmd == "scancel":
        open(path(f"cancel.{args[0]}"), "w").close()
    elif cmd == "preempt":
        open(path(f"preempt.{args[0]}"), "w").close()
    elif cmd == "reason":  # (test) set a pending job's reason and keep it pending: reason ID TEXT...
        j = load(args[0])
        j.update(reason=" ".join(args[1:]), eligible=time.time() + 3600)
        save(j)
    elif cmd == "release":  # (test) undo `reason`: the job may start again
        j = load(args[0])
        j.update(reason="Priority", eligible=time.time())
        save(j)
    elif cmd == "failsqueue":
        if args[0] == "on":
            open(path("squeue_fails"), "w").close()
        elif os.path.exists(path("squeue_fails")):
            os.remove(path("squeue_fails"))
    elif cmd == "slowsqueue":  # (test) slowsqueue SECONDS | slowsqueue off
        if args[0] == "off":
            if os.path.exists(path("squeue_slow")):
                os.remove(path("squeue_slow"))
        else:
            open(path("squeue_slow"), "w").write(args[0])
    elif cmd == "failsbatch":
        if args[0] == "on":
            open(path("sbatch_fails"), "w").close()
        elif os.path.exists(path("sbatch_fails")):
            os.remove(path("sbatch_fails"))
    elif cmd == "busy":
        if args[0] == "on":
            open(path("busy"), "w").close()
        elif os.path.exists(path("busy")):
            os.remove(path("busy"))
    elif cmd == "count":
        print(int(open(path("next")).read()) - 1000 if os.path.exists(path("next")) else 0)


if __name__ == "__main__":
    main()
