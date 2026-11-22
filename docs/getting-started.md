# Getting started

You need a Slurm cluster, Python 3.8 or later, and an account that will own the model's jobs (called `llm` here).
The gateway runs on any host that can run `sbatch`, for example a login node. It needs no GPU and no root.

## 1. Install

```bash
pip install git+https://github.com/yuy-blip/yawn-slurm
```

There are no dependencies. The `yawn` package must also be importable on the compute nodes, because the job runs
a small part of it: install it on a filesystem the nodes share (or in your container image). Without pip, copy the
`yawn` directory and set `PYTHONPATH`.

## 2. The job script

Copy [`examples/model.sbatch`](../examples/model.sbatch) next to your config and edit the lines marked `EDIT`:
the partition, the GPU request, the QoS, and how the model server is started at your site.

The last line starts the server through `yawn.worker`. The worker puts the server on a private Unix socket, lets
only the gateway through, and tells the gateway where the job is, so there is nothing to configure about hosts
and ports.

The QoS is your decision. A QoS that yields to other jobs keeps the service from jumping the queue.

## 3. The config

```json
{
  "listen": "192.0.2.10:8080",
  "key_socket": "/run/yawn/key.sock",
  "models": {
    "Qwen/Qwen2.5-7B-Instruct": {"sbatch": "model.sbatch"}
  }
}
```

- `listen`: an address that users and compute nodes can reach.
- `key_socket`: where users get their key. Its directory must be writable only by the `llm` account.
- `models`: the model name users will send, and the job script that serves it.

Run the gateway in a directory of the `llm` account. It keeps its keys and its request log there, readable only by
that account.

## 4. Start

```bash
yawn-gateway --check config.json    # says what it would do and what is missing
yawn-gateway config.json
```

To keep it running, use [`examples/yawn-gateway.service`](../examples/yawn-gateway.service). Installing the unit
is the one step that needs root; it also creates `/run/yawn` for the key socket. Without root, put the socket in
a directory of the `llm` account that others can enter but not write to, and have users set `YAWN_SOCKET` to it.

## 5. Use

On the host where the gateway runs:

```bash
export OPENAI_API_KEY=$(llm-key)
export OPENAI_BASE_URL=http://192.0.2.10:8080/v1
```

Any OpenAI-compatible client works. From a laptop, forward the port first: `ssh -L 8080:192.0.2.10:8080 LOGIN_NODE`.

The first request starts the job and waits while the model loads. If the cluster is full, the reply is a `503`
that says why, with a `Retry-After`; the job stays in the queue while requests keep coming, and starts when Slurm
gives it a GPU. A job, running or waiting, that nobody asks for is cancelled after 30 minutes.

Who used how much is in `ledger.jsonl` (one line per request, with the user and the tokens) and `usage.json`
(each user's share of the busy GPU time) in the gateway's working directory.

## Options

Per model:

| key | default | meaning |
|---|---|---|
| `idle_stop_s` | `1800` | cancel the job after this many seconds without a request; `0` never cancels it |
| `slots` | `32` | requests sent to the model at a time; the rest wait, and the least-served user goes next. Set it near what your server handles at once. `0` = no limit and no ordering |
| `startup_s` | `300` | how long the model usually takes to load (used in messages) |
| `start_timeout_s` | `1800` | a job that runs this long without becoming ready is cancelled |
| `batch_sbatch` | none | job script for `llm-batch` (see below) |

Top level:

| key | default | meaning |
|---|---|---|
| `url` | from `listen` | the address jobs and user tools are told to use, if it differs from `listen` |
| `hold_loading_s` | `300` | how long a request is held while the model loads |
| `hold_s` | `20` | how long a request is held while the job waits in the queue, before the `503` |
| `busy_hint` | none | text added to that `503`, e.g. `" To use it now as your own job, run: llm-up {model}"` |
| `own_jobs` | `true` | allow `llm-up` |

## When the shared job is waiting: `llm-up` and `llm-batch`

A user who does not want to wait for the shared job can run the same model as their own job, with their own
priority and place in the queue, behind the same endpoint and key. On a full cluster that job waits too.

```bash
llm-up Qwen/Qwen2.5-7B-Instruct            # the user's own job serves the user's requests; it stops when idle
llm-batch requests.jsonl --model Qwen/Qwen2.5-7B-Instruct   # bulk work as the user's own job
```

`llm-up` submits the model's job script as the user, so the script must be readable by users. For `llm-batch`,
copy [`examples/batch.sbatch`](../examples/batch.sbatch) and name it in the model's `batch_sbatch`.

## What the gateway assumes

- Jobs can reach the gateway's address, and the gateway can reach the compute nodes on the port the worker opens.
- `sbatch` passes the environment to the job (the gateway asks for it with `--export=ALL`).
- Users have accounts on the host where the gateway runs. That is how they get a key.
- The model server speaks the OpenAI API and can listen on a Unix socket (vLLM: `--uds`). For others, see the
  comment in the job script.
