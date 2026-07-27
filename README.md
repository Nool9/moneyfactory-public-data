# PIT ledger v1

Local, offline implementation stage for the frozen PIT ledger v1 r4 contract.

Checks:

```text
python -m unittest -v test_pit_ledger.py
python pit_ledger.py self-check
python pit_ledger.py e2e-self-check
git diff --check
```

The workflow is a thin `schedule` wrapper installed on the default branch. It
checks out and writes only `pit-ledger-v1` and fails closed
before network access unless every separately governed activation, target-write,
secret, writer, implementation, epoch and permission boundary is present.
Each slot has an immutable namespace, real log/slot manifest, checkpoint and
continuous append-only ledger index; overdue slots are recovered before capture.

This child commit is not an activation: no live download, collector-v4 read,
target mutation, remote branch, push, dispatch, `H0`, `S0`, killtest or trading
is authorized.
