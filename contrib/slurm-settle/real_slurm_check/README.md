# Ground truth inside a real Slurm

Purpose: verify that "priority + the settle delta (SiteFactor)" equals the priority Slurm computes once the same
usage is real.

Setup (a single-account, four-user sandbox built from source): Slurm 26.05.1, slurmdbd + MariaDB, one node,
priority/multifactor with PriorityWeightFairshare=10000, Fair Tree (default), PriorityCalcPeriod=1, the
`site_factor/table` plugin. Each user has one pending "probe" job (begin time in the future, so it never starts).

`ground_truth.sh` reads `sshare`, computes outside Slurm the table for "bob uses 300 more", then
(A) gives the table to the plugin and reads the probe jobs' priorities, empties the table, and
(B) lets bob run a 300 s job and reads the priorities again.

Result: A and B are identical for all four jobs, and the fair-share values predicted outside Slurm equal Slurm's own
after the real usage. Slurm's usage records were never written to by the settle step.

| user | before | A (table, usage unchanged) | B (real usage, no table) | fair-share predicted / Slurm |
|---|---|---|---|---|
| alice | 2000 | 4000 | 4000 | 0.40 / 0.40 |
| bob | 8000 | 2000 | 2000 | 0.20 / 0.20 |
| carol | 4000 | 6000 | 6000 | 0.60 / 0.60 |
| dave | 8000 | 8000 | 8000 | 0.80 / 0.80 |

Notes: `sprio` does not list jobs that wait for a begin time, so priorities are read with `squeue -o %Q`. Jobs
submitted with `--uid` need a spool directory that user can access.
