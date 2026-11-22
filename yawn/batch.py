# SPDX-FileCopyrightText: 2026 The YAWN authors
# SPDX-License-Identifier: Apache-2.0
"""llm-batch: run a file of requests as your own Slurm job (OpenAI batch format, one JSON request per line).

  llm-batch INPUT.jsonl [-o OUTPUT.jsonl] --model MODEL [--qos QOS] [--time T]
For bulk work: it goes through the queue as an ordinary job of yours, not through the shared service.
"""
import argparse
import os
import sys

from yawn import common


def main():
    ap = argparse.ArgumentParser(prog="llm-batch", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("input")
    ap.add_argument("-o", "--output")
    ap.add_argument("--model", required=True)
    ap.add_argument("--qos")
    ap.add_argument("--time")
    a = ap.parse_args()
    entry = common.model_entry(common.info(), a.model)
    if not entry.get("batch_sbatch"):
        sys.exit(f"no batch job script is set up for {a.model} on this gateway")
    src = os.path.abspath(a.input)
    if not os.path.isfile(src):
        sys.exit(f"no such file: {src}")
    out = os.path.abspath(a.output or (os.path.splitext(src)[0] + ".out.jsonl"))
    env = {"YAWN_MODEL": a.model, "YAWN_BATCH_IN": src, "YAWN_BATCH_OUT": out}
    jid = common.submit("yawn-batch-" + common.slug(a.model), entry["batch_sbatch"], env, a.qos, a.time)
    print(f"submitted job {jid}: {src} -> {out}")
    print(f"watch: squeue -j {jid}")


if __name__ == "__main__":
    main()
