<h1 align="center">YAWN</h1>

<h3 align="center">
An LLM for everyone on your Slurm cluster that is there when they ask and out of the way when they don't
</h3>

<p align="center">
| <a href="docs/getting-started.md"><b>Getting started</b></a> | <a href="examples"><b>Examples</b></a> | <a href="SECURITY.md"><b>Security</b></a> |
</p>

<p align="center">
<a href="https://github.com/yuy-blip/yawn-slurm/actions/workflows/tests.yml"><img alt="tests" src="https://github.com/yuy-blip/yawn-slurm/actions/workflows/tests.yml/badge.svg"></a>
<a href="LICENSE"><img alt="license: Apache-2.0" src="https://img.shields.io/badge/license-Apache--2.0-blue"></a>
<img alt="python: 3.8+" src="https://img.shields.io/badge/python-3.8%2B-blue">
</p>

---

## About

YAWN is a small gateway between people who want an OpenAI-compatible endpoint and a Slurm cluster whose GPUs are shared. The model server (for example vLLM) runs as an ordinary Slurm job. YAWN starts that job when someone sends a request, holds the request while the model loads, and stops the job when nobody is using it. Which job gets a GPU, and when, stays Slurm's decision.

It is for you if any of these sounds familiar:

- The shared LLM is used a few hours a day but holds a GPU around the clock, while other people's jobs wait.
- When you stop it, everyone starts their own copy of the same model and they compete for the same GPUs.
- Everything runs under one service account, so nobody can tell who used how much.

## Features

- **Starts on request, stops when idle.** The first request submits the model's job; requests are held while it loads.
- **Knows the queue.** While Slurm keeps the job pending, clients get a clear `503` with Slurm's reason and a `Retry-After`, and the job keeps its place in line while clients keep asking.
- **Leaves GPU decisions to Slurm.** The job runs in the QoS your site chooses; preemption and requeue are followed.
- **Knows who is asking.** Every cluster user gets their own key with no list for an administrator to maintain, and every request is recorded per user.
- **Shares one model fairly.** No per-user quotas; the next free slot goes to the waiting user with the fewest requests in flight, then the least use.
- **No way around the gateway.** With vLLM's Unix socket (as in the example job script) the model server is never on the network: a small worker in the job guards every path.
- **Your own job when you need it.** If the shared job is waiting, `llm-up` runs the same model as the user's own job, at the user's own priority, behind the same endpoint; `llm-batch` sends bulk work through the queue.
- **Small.** One Python package, standard library only. No database, no root, no changes to Slurm.

## Getting started

```bash
pip install git+https://github.com/yuy-blip/yawn-slurm
```

`config.json` (the job script is [`examples/model.sbatch`](examples/model.sbatch) with your partition filled in):

```json
{
  "listen": "192.0.2.10:8080",
  "key_socket": "/run/yawn/key.sock",
  "models": {
    "Qwen/Qwen2.5-7B-Instruct": {"sbatch": "model.sbatch"}
  }
}
```

```bash
yawn-gateway --check config.json    # says what it would do and what is missing
yawn-gateway config.json
```

A user on the cluster:

```bash
export OPENAI_API_KEY=$(llm-key)
curl http://192.0.2.10:8080/v1/chat/completions -H "Authorization: Bearer $OPENAI_API_KEY" \
     -H 'Content-Type: application/json' \
     -d '{"model": "Qwen/Qwen2.5-7B-Instruct", "messages": [{"role": "user", "content": "hello"}]}'
```

The first request waits while the job starts and the model loads; later ones are answered at once. See [Getting started](docs/getting-started.md) for the job script and the options.

## Status

Alpha. Tested end to end against a stand-in Slurm and model server (`python3 tests/run_tests.py`, no GPU needed) on Python 3.8 to 3.13. Tried on one real Slurm cluster with vLLM: start on request, waiting in the queue, loading, streaming, idle stop, gateway restart, and refusing requests that bypass the gateway. Preemption, `llm-up` and `llm-batch` have not been tried on a real cluster yet. Not yet used in production. Read [SECURITY.md](SECURITY.md) before opening it to users.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Apache-2.0. The optional add-on in [`contrib/slurm-settle`](contrib/slurm-settle) runs inside or next to Slurm and is GPL-2.0-or-later, like Slurm; see [NOTICE](NOTICE).

Slurm is a registered trademark of SchedMD LLC. This project is not affiliated with or endorsed by SchedMD.
