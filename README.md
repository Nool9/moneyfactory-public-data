# PIT ledger public-only v9

Candidate Cloud Run implementation for the PIT ledger public-only v9 contract.
It accepts zero-sided raw Binance bookTicker rows while preserving positive
quotes for any active perpetual candidate. V8 history remains immutable.

Checks:

```text
python -m unittest -v test_pit_ledger.py
python pit_ledger.py self-check
python pit_ledger.py e2e-self-check
git diff --check
```

The container bloblessly and sparsely clones only `pit-ledger-public-v9` and fails closed
before market access unless every separately governed activation, target-write,
writer, implementation, image, credential and epoch boundary is present.
Each slot has an immutable namespace, real log/slot manifest, checkpoint and
continuous append-only ledger index. The outcome, slot manifest, index record
and checkpoint share one atomic terminal commit; duplicate acceptance replays
all eight public sources and every Bybit page from raw bytes against the pinned
implementation, and overdue recovery precedes the current-window gate.
History is proved from one metadata stream plus the current index prefix chain;
raw pages remain separate pre-parse commits while derived evidence is batched.

This local commit is not an activation: it performs no live download, remote
mutation, provisioning, dispatch, `S0`, killtest or trading.
