# SPDX-FileCopyrightText: 2026 The YAWN authors
# SPDX-License-Identifier: Apache-2.0
"""llm-up: serve a model from your own Slurm job (your account, your QoS, your place in the queue) behind the usual endpoint.

Use it when the shared model is waiting for a free GPU and you do not want to wait with it.
  llm-up MODEL [--qos QOS] [--time T] [--idle-min N]     start (or show) your job for MODEL
  llm-up MODEL --stop                                    cancel it
  llm-up --list                                          models and your jobs
Nothing else changes: same URL, same key. While your job runs, the gateway sends your requests to it whenever the
shared model cannot answer. It stops by itself after N idle minutes (and sooner once the shared model is back).
"""
import argparse
import os
import subprocess
import sys

from yawn import common


def main():
    ap = argparse.ArgumentParser(prog="llm-up", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("model", nargs="?")
    ap.add_argument("--qos")
    ap.add_argument("--time")
    ap.add_argument("--idle-min", type=float, default=30)
    ap.add_argument("--stop", action="store_true")
    ap.add_argument("--list", action="store_true")
    a = ap.parse_args()
    i = common.info()
    if a.list or not a.model:
        for m in i["models"]:
            jobs = common.my_jobs("yawn-own-" + common.slug(m))
            print(m, "-", ", ".join(f"job {j[0]} {j[1]}" for j in jobs) or "no job of yours")
        sys.exit(0)
    entry = common.model_entry(i, a.model)
    name = "yawn-own-" + common.slug(a.model)
    jobs = common.my_jobs(name)
    if a.stop:
        for j in jobs:
            subprocess.run(common.cmd("scancel") + [j[0]])
            print(f"cancelled job {j[0]}")
        sys.exit(0)
    if not i.get("own_jobs", True):
        sys.exit("own jobs are turned off on this gateway")
    if jobs:
        print(f"you already have job {jobs[0][0]} ({jobs[0][1]}) for {a.model}")
        sys.exit(0)
    env = {"YAWN_GATEWAY": i["url"], "YAWN_TOKEN": i["key"], "YAWN_MODEL": a.model,
           "YAWN_IDLE_S": str(int(a.idle_min * 60)), "YAWN_HANDBACK_S": os.environ.get("YAWN_HANDBACK_S", "120")}
    jid = common.submit(name, entry["sbatch"], env, a.qos, a.time)
    print(f"submitted job {jid} for {a.model} as your own job.")
    print(f"Slurm charges it to you like any job. It stops after {a.idle_min:g} idle minutes. Stop it now: llm-up {a.model} --stop")


if __name__ == "__main__":
    main()
