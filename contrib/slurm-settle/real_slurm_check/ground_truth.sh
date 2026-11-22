#!/bin/bash
# SPDX-FileCopyrightText: 2026 The YAWN authors
# SPDX-License-Identifier: GPL-2.0-or-later
# Ground-truth check inside a real Slurm. Priorities are read with squeue (sprio hides jobs waiting for a begin time).
# Set: I = Slurm install prefix, R = the directory holding site_factor.txt (as in slurm.conf), X = a scratch directory.
: "${I:?} ${R:?} ${X:?}"
G="$(cd "$(dirname "$0")/.." && pwd)"; export PATH=$I/bin:$I/sbin:$PATH; WHO=bob; USE=300
quiet() { while squeue -h -t R -o %i | grep -q .; do sleep 3; done; sleep 75; }
prio() { for j in $(squeue -h -t PD -n probe -o %i | sort -n); do scontrol show job $j | tr ' ' '\n' | grep -E "^(UserId|Priority|SiteFactor)=" | tr '\n' ' ' | sed -E 's/UserId=([a-z]+)\([0-9]+\)/\1/; s/Priority=//; s/SiteFactor=//'; echo; done | sort; }
quiet
sshare -a -l -P > $X/s0.txt; echo "== before: fair-share (real Slurm)   account|user|usage|fairshare"; cut -d'|' -f1,2,5,8 $X/s0.txt
python3 "$G/settle.py" check < $X/s0.txt
echo "== before: probe jobs   user priority site"; prio | tee $X/p0.txt
python3 - "$G" $X $WHO $USE <<'PY'
import sys; sys.path.insert(0, sys.argv[1]); import settle
X, who, use = sys.argv[2], sys.argv[3], float(sys.argv[4])
tab = settle.table(settle.parse_sshare(open(X + "/s0.txt").read()), {who: use}, 10000)
open(X + "/table.txt", "w").write("".join(f"{u} {a} {d}\n" for (u, a), d in sorted(tab.items())))
print(f"== settle table if {who} uses {use:g} more (computed outside Slurm from sshare)"); print(open(X + "/table.txt").read(), end="")
PY
cp $X/table.txt $R/site_factor.txt.tmp && mv $R/site_factor.txt.tmp $R/site_factor.txt; sleep 75
echo "== A: usage unchanged, table applied by the site_factor/table plugin   user priority site"; prio | tee $X/pA.txt
: > $R/site_factor.txt
sbatch --parsable --uid=$WHO -A hpc -J real -o /dev/null -D /tmp --wrap "sleep $USE" > /dev/null; sleep 5; quiet
sshare -a -l -P > $X/s1.txt; echo "== after $WHO ran a $USE s job: fair-share (real Slurm)"; cut -d'|' -f1,2,5,8 $X/s1.txt
echo "== B: usage real, table empty   user priority site"; prio | tee $X/pB.txt
python3 - "$G" $X $WHO <<'PY'
import sys; sys.path.insert(0, sys.argv[1]); import settle
X, who = sys.argv[2], sys.argv[3]
def rd(f): return {l.split()[0]: [int(v) for v in l.split()[1:]] + [0] for l in open(X + "/" + f) if l.strip()}  # no SiteFactor printed = 0
def usage(f): return {l.split("|")[1].strip(): (float(l.split("|")[4]), float(l.split("|")[7])) for l in open(X + "/" + f).read().splitlines()[1:] if l.split("|")[1].strip()}
pA, pB, u0, u1 = rd("pA.txt"), rd("pB.txt"), usage("s0.txt"), usage("s1.txt")
added = u1[who][0] - u0[who][0]
pred = {k[2]: v for k, v in settle.fair_tree(settle.parse_sshare(open(X + "/s0.txt").read()), {who: added}).items()}
print(f"== verdict (Slurm itself added {added:g} to {who}'s usage)")
ok = bool(pA) and set(pA) == set(pB)
for u in sorted(pA):
    same = pA[u][0] == pB[u][0]; fs = abs(pred[u] - u1[u][1]) < 1e-6; ok &= same and fs
    print(f"{u:6} A: priority {pA[u][0]:>6} (site {pA[u][1]:>6})   B: priority {pB[u][0]:>6} (site {pB[u][1]:>2})   {'SAME' if same else 'DIFFERENT'}"
          f"   fair-share predicted {pred[u]:.2f} / Slurm {u1[u][1]:.2f} {'ok' if fs else 'WRONG'}")
print("RESULT:", f"MATCH for {len(pA)} jobs: settling by SiteFactor gives exactly the priorities that real usage gives" if ok else "MISMATCH (or nothing compared)")
PY
