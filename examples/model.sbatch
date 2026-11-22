#!/bin/bash
# SPDX-FileCopyrightText: 2026 The YAWN authors
# SPDX-License-Identifier: Apache-2.0
#
# The job that serves a model. The gateway submits it on the first request; `llm-up` submits the same script as
# a user's own job. Edit the lines marked EDIT.
#SBATCH --partition=gpu            # EDIT
#SBATCH --gres=gpu:1               # EDIT
#SBATCH --time=1-00:00:00          # EDIT
#SBATCH --qos=normal               # EDIT: the QoS for the shared model. One that yields to other jobs keeps the
#                                    service from jumping the queue. (llm-up overrides it with the user's own.)
#SBATCH --requeue
#SBATCH --open-mode=append
#SBATCH --output=yawn-%j.out       # EDIT if the directory the gateway runs in does not exist on the compute nodes

# yawn.worker starts the server on a private Unix socket ({socket}), lets only the gateway through, and tells the
# gateway where this job is. YAWN_MODEL is the model name from the gateway's config.
# EDIT: how the server is started at your site (a module, a virtualenv, a container). The `yawn` package must be
# importable here; for a server that cannot listen on a Unix socket use `--port {port}` instead.
exec python3 -m yawn.worker -- vllm serve "$YAWN_MODEL" --uds {socket}
