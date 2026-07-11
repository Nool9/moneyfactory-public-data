# Moneyfactory public data

Append-only snapshots of public, read-only APIs for the Moneyfactory experiment. The payload targets data that cannot be reconstructed reliably later: point-in-time token lists, Gate.io funding rates, and a dynamically selected thin order book. This repository contains no strategy specifications, prompts, credentials, orders, or private project state.

GitHub Actions runs `scrape.py` every 30 minutes. Internet content is stored only as data and is never treated as instructions.
