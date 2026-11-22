# Contributing

Bug reports, questions and reports from your own cluster are welcome as issues. The most useful reports say what
your site looks like (scheduler version, how many GPUs, how users reach the cluster) and what happened.

## Pull requests

- Keep each pull request to one logical change, and run `python3 tests/run_tests.py` before sending it.
- Sign off your commits (Developer Certificate of Origin, https://developercertificate.org/): `git commit -s`.
- Your contribution is licensed under the license of the files it changes: Apache-2.0 for everything except
  `contrib/slurm-settle/`, which is GPL-2.0-or-later. Please do not move code between the two.
- New files carry the two SPDX lines used throughout the repository.
