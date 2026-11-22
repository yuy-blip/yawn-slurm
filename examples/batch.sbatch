#!/bin/bash
# SPDX-FileCopyrightText: 2026 The YAWN authors
# SPDX-License-Identifier: Apache-2.0
#
# A user's own batch job, submitted by `llm-batch INPUT.jsonl --model MODEL`: bulk work goes through the queue as
# the person who asked. Input and output are in the OpenAI batch format. Edit the lines marked EDIT.
#SBATCH --partition=gpu            # EDIT
#SBATCH --gres=gpu:1               # EDIT
#SBATCH --time=08:00:00            # EDIT
#SBATCH --output=yawn-batch-%j.out

set -eu
mkdir -p "$(dirname "$YAWN_BATCH_OUT")"
# EDIT: how vLLM is started at your site.
vllm run-batch -i "$YAWN_BATCH_IN" -o "$YAWN_BATCH_OUT" --model "$YAWN_MODEL"
