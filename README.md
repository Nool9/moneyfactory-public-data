# PIT ledger public-only v1

Local, offline implementation stage for the composed PIT ledger public-only v1 contract.

Checks:

```text
python -m unittest -v test_pit_ledger.py
python pit_ledger.py self-check
python pit_ledger.py e2e-self-check
git diff --check
```

The workflow is a thin `schedule` wrapper installed on the default branch. It
checks out and writes only `pit-ledger-public-v1` and fails closed
before network access unless every separately governed activation, target-write,
writer, implementation and epoch boundary is present.
Each slot has an immutable namespace, real log/slot manifest, checkpoint and
continuous append-only ledger index. The outcome, slot manifest, index record
and checkpoint share one atomic terminal commit; duplicate acceptance replays
all eight public sources and every Bybit page from raw bytes against the pinned
implementation, and overdue recovery precedes the current-window gate.

This child commit is not an activation: no live download, collector-v4 read,
target mutation, remote branch, push, dispatch, `H0`, `S0`, killtest or trading
is authorized.
