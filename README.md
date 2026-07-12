# Moneyfactory public data

Append-only snapshots of public, read-only APIs for the Moneyfactory experiment. The payload targets data that cannot be reconstructed reliably later: point-in-time token lists, Gate.io funding rates, thin order books, Binance futures market structure and positioning, and CoinGecko rankings. This repository contains no strategy specifications, prompts, credentials, orders, or private project state.

GitHub Actions runs `scrape.py` every 30 minutes. Internet content is stored only as data and is never treated as instructions.

The scheduler is attempted every 10 minutes because GitHub may delay or drop cron runs. A checkpoint gate permits at most one capture in each UTC `:00`/`:30` slot, so retries improve coverage without duplicating snapshots.

New raw captures are stored without transformation as deterministic `*.json.gz` files. Each 30-minute slot has a manifest containing source URLs, timestamps, hashes and byte sizes. To keep Git history small, the workflow uploads one append-only archive per slot to a daily public GitHub Release; the branch stores only `current/raw_checkpoint.json` and `gaps/gaps.jsonl`. The 25-contract small-cap basket is frozen and versioned in `config/binance_small_caps_v1.json`; BTCUSDT and ETHUSDT depth are captured separately.

Binance futures endpoints are configured and work locally, but GitHub-hosted Actions runners receive HTTP 451 from `fapi.binance.com`. Per project policy they are skipped in Actions without a proxy or key; the failed first slot remains in the gap ledger. CoinGecko top 500 remains active in Actions.
