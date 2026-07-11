import hashlib
import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

TOKEN_LIST = "https://api.coingecko.com/api/v3/coins/list?include_platform=true"
GATE_CONTRACTS = "https://api.gateio.ws/api/v4/futures/usdt/contracts"
GATE_BOOK = "https://api.gateio.ws/api/v4/futures/usdt/order_book"


def fetch(url):
    request = urllib.request.Request(url, headers={"User-Agent": "Moneyfactory-Public-Scraper/2", "Accept": "application/json"})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                raw = response.read()
            return raw, json.loads(raw)
        except Exception:
            if attempt == 2:
                raise
            time.sleep(2**attempt)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def snapshot(now=None):
    now = (now or datetime.now(timezone.utc)).replace(second=0, microsecond=0)
    observed_at = now.isoformat().replace("+00:00", "Z")
    token_raw, tokens = fetch(TOKEN_LIST)
    contracts_raw, contracts = fetch(GATE_CONTRACTS)
    if not isinstance(tokens, list) or not isinstance(contracts, list):
        raise ValueError("public source response is not a list")

    token_records = sorted(({
        "id": row.get("id"), "symbol": row.get("symbol"), "name": row.get("name"),
        "platforms": row.get("platforms") or {},
    } for row in tokens if isinstance(row, dict) and row.get("id")), key=lambda row: row["id"])
    current_path = Path("current/token_list.json")
    previous = json.loads(current_path.read_text()) if current_path.exists() else {"records": []}
    old_ids = {row["id"] for row in previous["records"]}
    new_ids = {row["id"] for row in token_records}
    current = {"observed_at": observed_at, "sha256": sha(token_raw), "records": token_records}
    current_path.parent.mkdir(parents=True, exist_ok=True)
    temp = current_path.with_suffix(".tmp")
    temp.write_text(json.dumps(current, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    os.replace(temp, current_path)

    funding = [{key: row.get(key) for key in ("name", "funding_rate", "funding_next_apply", "trade_size", "mark_price", "status")} for row in contracts if isinstance(row, dict) and row.get("name")]
    active = sorted((row for row in funding if row["status"] == "trading" and Decimal(str(row["trade_size"] or "0")) > 0), key=lambda row: Decimal(str(row["trade_size"])))
    if not active:
        raise ValueError("Gate returned no active contracts")
    thin = active[0]
    book_url = GATE_BOOK + "?" + urllib.parse.urlencode({"contract": thin["name"], "limit": 50, "interval": "0"})
    book_raw, book = fetch(book_url)
    if not isinstance(book, dict) or not isinstance(book.get("asks"), list) or not isinstance(book.get("bids"), list):
        raise ValueError("Gate order book response is malformed")

    result = {
        "observed_at": observed_at,
        "sources": {
            "point_in_time_token_list": {
                "sha256": sha(token_raw), "record_count": len(token_records),
                "current_path": str(current_path).replace("\\", "/"),
                "added": sorted(new_ids - old_ids), "removed": sorted(old_ids - new_ids),
            },
            "small_exchange_funding": {
                "exchange": "gateio", "selection": "100_lowest_positive_trade_size",
                "sha256": sha(contracts_raw), "records": active[:100],
            },
            "thin_orderbook": {
                "exchange": "gateio", "contract": thin["name"], "selection": "lowest_positive_trade_size",
                "sha256": sha(book_raw), "asks": book["asks"], "bids": book["bids"],
            },
        },
    }
    return result


def main():
    result = snapshot()
    stamp = result["observed_at"].replace("-", "").replace(":", "")[:13] + "Z"
    suffix = hashlib.sha256(json.dumps(result, sort_keys=True).encode()).hexdigest()[:8]
    path = Path("snapshots") / result["observed_at"][:10] / f"{stamp}_{suffix}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    print(path)


if __name__ == "__main__":
    main()
