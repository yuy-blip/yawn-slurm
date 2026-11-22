# slurm-settle (experimental, off by default)

Optional. Makes the use of a shared model count in Slurm's fair-share priority, without changing Slurm's usage
records. License: GPL-2.0-or-later (see `COPYING`), because these files run inside Slurm or follow its code.

Status: in a sandbox Slurm 26.05.1 with slurmdbd, the calculation reproduces the fair-share values of `sshare`, and
the resulting job priorities equal those after real usage (`real_slurm_check/`). It has not been used in production.
Treat it as an experiment.

## The idea

Slurm adds usage only to the owner of a running job. A shared model job serves many people, so their use never
reaches their fair-share factor. But usage only matters when it becomes the fair-share term of a job's priority,
and Slurm lets a site add its own term, SiteFactor:

    SiteFactor(user) = round(PriorityWeightFairshare * (F'(user) - F(user)))

where F is the fair-share factor from `sshare` and F' is the same with the service use added. The job's priority is
then what it would have been had the service use been ordinary job usage.

## Parts

| file | what it does |
|---|---|
| `settle.py` | reads `sshare -a -l -P` and the service use per user (the gateway's `usage.json`); prints `user account delta` lines. `settle.py check` only compares its fair-share values with Slurm's own (read-only). |
| `site_factor_table/` | a `site_factor` plugin for slurmctld: applies such a table at submission and to pending jobs every PriorityCalcPeriod. Copy the directory into `src/plugins/site_factor/` of the same Slurm version and build. |
| `job_submit_site_factor.lua` | the same at submission only, for sites that already use `JobSubmitPlugins=lua`. |
| `real_slurm_check/` | the sandbox check: script and result. |

## Use with the gateway

The gateway keeps each user's share of the busy GPU time in `usage.json` in its working directory, decayed with
the gateway setting `halflife_days` (default 7, Slurm's default PriorityDecayHalfLife; set it to the site's value).
The per-model setting `billing_weight` (default 1) should equal the partition's TRES billing weight. Run this every
few minutes as the service account, for example from cron:

    sshare -a -l -P | python3 settle.py table --usage /path/to/usage.json --weight 10000 > site_factor.txt.tmp \
      && mv site_factor.txt.tmp /path/to/site_factor.txt

`--weight` is the site's PriorityWeightFairshare. The gateway itself knows nothing about this.

## Known limits

- Only Fair Tree (the default) is reproduced. Run `sshare -a -l -P | python3 settle.py check` first.
- A user with associations in several accounts: the service use is booked to the first one.
- With the Lua snippet the value is fixed at submission; only the plugin refreshes pending jobs.
- Whoever can write the table can move anyone's priority within the limit; protect the file.
