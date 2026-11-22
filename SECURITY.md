# Security

## Reporting a vulnerability

Please do not open a public issue. Use GitHub's private reporting instead: on the repository page, open the
**Security** tab and choose **Report a vulnerability**. Say what an attacker needs (an account on the cluster? a
valid key?), what they gain, and how to reproduce it.

## What the gateway is meant to guarantee

- A request is attributed to the cluster user who obtained the key. Keys are handed out only over a local socket on
  which the kernel identifies the caller.
- A model server started through `yawn.worker` with `{socket}` cannot be used without going through the gateway,
  on any path. The server listens on a Unix socket in a directory only the job's account can open, and the worker in
  front of it passes a request on only if it carries the job's secret. (A model server's own API key is not enough:
  vLLM, for example, applies its key only to paths under `/v1` and a few other prefixes.)
- One user cannot read another user's requests through the gateway. When the model server honours `cache_salt`
  (vLLM does) and the model's `cache_salt` setting is on (the default), the shared prefix cache is kept apart per user.
- A user's own job only ever serves that user.
- The gateway connects only to machines that registered with it, at the address the registration came from.

## Known limits

- The gateway speaks plain HTTP. Keep its TCP address on the cluster's internal network. Users who connect from
  elsewhere should use an SSH tunnel, or put a TLS proxy in front.
- With `{port}` instead of `{socket}` the model server listens on the node's loopback address, where other jobs on
  the same node can reach it directly.
- Everyone who can log in to the host where the gateway runs can get a key. There is no allow-list, expiry or
  quota yet.
- While the gateway is down, another user on the same host could bind its TCP port. The key socket cannot be taken
  over when it lives in a directory only the service account can write to.
- The gateway cannot verify that a job registering with a user's key really is that user's Slurm job; such a
  registration therefore only ever redirects that user's own requests.
- The token a job uses to register is passed to it through its environment (`sbatch --export=ALL`), which Slurm
  stores for the job. Other users cannot read it, but administrators can.
