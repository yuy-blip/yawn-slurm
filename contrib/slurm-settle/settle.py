# SPDX-FileCopyrightText: 2026 The YAWN authors
# SPDX-License-Identifier: GPL-2.0-or-later
"""settle: charge shared-service use to Slurm fair-share without touching Slurm's usage numbers.

Slurm adds usage only to the owner of a running job. A shared model job serves many people, so their use never
reaches their fair-share factor. But usage matters only when it turns into the fair-share term of a job's priority:
    priority = ... + PriorityWeightFairshare * F(usage) + SiteFactor + ...
settle.py computes, outside Slurm, F with the service use added (F') and hands the difference to Slurm as SiteFactor:
    SiteFactor(user) = round(PriorityWeightFairshare * (F'(user) - F(user)))
The job's priority is then what it would be if the service use had been ordinary job usage.
Adding use to one person also moves the people they fall behind up, so the table covers every user.

F is Slurm's Fair Tree (the default). The computation follows Slurm 26.05.1,
src/plugins/priority/multifactor/fair_tree.c (Copyright (C) 2014 Brigham Young University,
GPL-2.0-or-later); this file is under the same license.

  sshare -a -l -P | python3 settle.py check
      recompute FairShare from the same output and compare with Slurm's own column (prints counts only, no names)
  sshare -a -l -P | python3 settle.py table --usage usage.json [--weight 10000]
      usage.json: the gateway's usage file, or {"user": billed_seconds_already_decayed, ...}
      ->  "user account delta" lines (only non-zero)
  python3 settle.py selftest
"""
import json
import math
import sys


class Assoc:
    def __init__(self, account, user, shares_norm, raw, reported=None, use_parent=False):
        self.account, self.user = account, user
        self.shares_norm, self.raw, self.reported, self.use_parent = shares_norm, raw, reported, use_parent
        self.parent, self.children = None, []
        self.level_fs = self.fs = None


def parse_sshare(text):
    """`sshare -a -l -P` (with header). Depth = leading spaces of the Account column. Returns the root."""
    lines = [l for l in text.splitlines() if l.strip()]
    head = [h.strip() for h in lines[0].split("|")]
    col = {h: i for i, h in enumerate(head)}
    root, stack = None, []  # stack of (indent, account assoc)
    for line in lines[1:]:
        f = line.split("|")
        acct_raw = f[col["Account"]]
        indent, account, user = len(acct_raw) - len(acct_raw.lstrip(" ")), acct_raw.strip(), f[col["User"]].strip()
        rs = f[col["RawShares"]].strip()

        def num(name):
            try:
                return float(f[col[name]])
            except (ValueError, KeyError):
                return 0.0
        a = Assoc(account, user or None, num("NormShares"), num("RawUsage"),
                  num("FairShare") if user else None, use_parent=(rs == "parent"))
        while stack and stack[-1][0] >= indent:  # parent = nearest account row above with a smaller indent
            stack.pop()
        if not stack:
            if root is None and not user:
                root = a
                stack.append((indent, a))
                continue
            raise ValueError("sshare output does not start with the root account")
        a.parent = stack[-1][1]
        a.parent.children.append(a)
        if not user:
            stack.append((indent, a))
    return root


def users(root):
    out, todo = [], [root]
    while todo:
        a = todo.pop()
        if a.user:
            out.append(a)
        todo += a.children
    return out


def fair_tree(root, extra=None):
    """Set .fs on every user assoc. extra: {user: usage to add} (added to the user and every ancestor, like a job)."""
    us = users(root)
    add = {}
    for u in us:
        x = (extra or {}).get(u.user, 0.0)
        if x and u.user not in add:  # one association per user gets the charge (the first found)
            add[u.user] = True
            a = u
            while a:
                a._raw = getattr(a, "_raw", a.raw) + x
                a = a.parent
    raw = lambda a: getattr(a, "_raw", a.raw)  # noqa: E731
    n = len(us)
    state = {"rank": n, "rnt": n}

    def level_fs(a):
        if a.use_parent:
            return math.inf if a.user else -1.0  # accounts with Fairshare=parent: NO_VAL in Slurm (not ranked by it)
        if a.shares_norm == 0:
            return 0.0
        pr = raw(a.parent) if a.parent else 0.0
        u = raw(a) / pr if pr else 0.0
        return math.inf if u == 0 else a.shares_norm / u

    def calc(siblings, account_tied):
        for a in siblings:
            a.level_fs = level_fs(a)
        siblings = sorted(siblings, key=lambda a: (-a.level_fs, 0 if a.user else 1))
        prev, i = None, 0
        while i < len(siblings):
            a = siblings[i]
            tied = account_tied if i == 0 and account_tied else (prev == a.level_fs)
            if a.user:
                if not tied:
                    state["rank"] = state["rnt"]
                a.fs = state["rank"] / n
                state["rnt"] -= 1
            else:
                j = i
                while j + 1 < len(siblings) and not siblings[j + 1].user and siblings[j + 1].level_fs == a.level_fs:
                    j += 1  # tied sibling accounts: merge their children
                kids = [c for s in siblings[i:j + 1] for c in s.children]
                if kids:
                    calc(kids, tied)
                i = j
            prev = a.level_fs
            i += 1

    calc(list(root.children), False)
    todo = [root]
    while todo:  # drop the temporary usage
        a = todo.pop()
        a.__dict__.pop("_raw", None)
        todo += a.children
    return {(0, u.account, u.user): u.fs for u in us}


def table(root, ledger, weight):
    base = dict(fair_tree(root))
    new = fair_tree(root, ledger)
    out = {}
    for k, f0 in base.items():  # one line per association: a user in two accounts has two fair-share values
        d = int(round(weight * (new[k] - f0)))
        if d:
            out[(k[2], k[1])] = d
    return out


def selftest():
    t = "Account|User|RawShares|NormShares|RawUsage|FairShare\n"
    flat = t + "root|||0|60|\n hpc||1|1|60|\n  hpc|u0|1|0.25|0|1.0\n  hpc|u10|1|0.25|10|0.75\n  hpc|u20|1|0.25|20|0.5\n  hpc|u30|1|0.25|30|0.25\n"
    r = parse_sshare(flat)
    f = {k[2]: v for k, v in fair_tree(r).items()}
    assert f == {"u0": 1.0, "u10": 0.75, "u20": 0.5, "u30": 0.25}, f
    f2 = {k[2]: v for k, v in fair_tree(r, {"u10": 15}).items()}
    assert f2 == {"u0": 1.0, "u20": 0.75, "u10": 0.5, "u30": 0.25}, f2
    assert table(r, {"u10": 15}, 10000) == {("u10", "hpc"): -2500, ("u20", "hpc"): 2500}
    assert {k[2]: v for k, v in fair_tree(r).items()} == f  # no state left behind
    # invariant: base priority + delta == priority with the use counted as usage
    W = 10000
    d = table(r, {"u10": 15, "u0": 40}, W)
    for u, fnew in {k[2]: v for k, v in fair_tree(r, {"u10": 15, "u0": 40}).items()}.items():
        assert round(W * f[u]) + d.get((u, "hpc"), 0) == round(W * fnew), u
    ties = t + "root|||0|30|\n hpc||1|1|30|\n  hpc|a|1|0.25|0|\n  hpc|b|1|0.25|0|\n  hpc|c|1|0.25|10|\n  hpc|d|1|0.25|20|\n"
    f = {k[2]: v for k, v in fair_tree(parse_sshare(ties)).items()}
    assert f == {"a": 1.0, "b": 1.0, "c": 0.5, "d": 0.25}, f
    two = t + ("root|||0|150|\n A||1|0.5|100|\n  A|a1|1|0.5|80|\n  A|a2|1|0.5|20|\n B||1|0.5|50|\n  B|b1|1|1|50|\n")
    f = {k[2]: v for k, v in fair_tree(parse_sshare(two)).items()}
    assert f == {"b1": 1.0, "a2": 2 / 3, "a1": 1 / 3}, f
    # enough service use by b1 flips the accounts: A's users move ahead of b1
    f = {k[2]: v for k, v in fair_tree(parse_sshare(two), {"b1": 200}).items()}
    assert f == {"a2": 1.0, "a1": 2 / 3, "b1": 1 / 3}, f
    # a user in two accounts: each association gets its own delta (the service use is booked to the first one)
    both = t + ("root|||0|100|\n A||1|0.5|60|\n  A|x|1|0.5|10|\n  A|y|1|0.5|50|\n B||1|0.5|40|\n  B|x|1|0.5|30|\n  B|z|1|0.5|10|\n")
    d = table(parse_sshare(both), {"x": 100}, 10000)
    assert set(k for k in d if k[0] == "x") <= {("x", "A"), ("x", "B")} and d.get(("x", "A")) != d.get(("x", "B")), d
    print("selftest ok")


def main():
    if len(sys.argv) < 2 or sys.argv[1] == "selftest":
        return selftest()
    root = parse_sshare(sys.stdin.read())
    if sys.argv[1] == "check":
        fair_tree(root)
        us = [u for u in users(root) if u.reported is not None]
        diffs = [abs(u.fs - u.reported) for u in us]
        bad = sum(d > 2e-6 for d in diffs)
        print(f"user associations: {len(us)}  max |recomputed - sshare FairShare|: {max(diffs, default=0):.6f}  differing: {bad}")
        print("MATCH: this cluster's fair-share values are reproduced outside Slurm" if not bad else
              "NO MATCH: different algorithm or flags (classic fair-share? DEPTH_OBLIVIOUS?) or sshare layout not parsed")
    elif sys.argv[1] == "table":
        ledger = json.load(open(sys.argv[sys.argv.index("--usage") + 1]))
        if isinstance(ledger.get("usage"), dict):  # the gateway's usage.json: {"t": ..., "usage": {user: value}}
            ledger = ledger["usage"]
        weight = int(sys.argv[sys.argv.index("--weight") + 1]) if "--weight" in sys.argv else 10000
        for (u, acct), d in sorted(table(root, ledger, weight).items()):
            print(u, acct, d)


if __name__ == "__main__":
    main()
