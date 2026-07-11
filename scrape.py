import hashlib
import json
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


def fetch(url):
    request = urllib.request.Request(url, headers={"User-Agent": "Moneyfactory-Public-Scraper/1", "Accept": "application/json"})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                raw = response.read()
            return raw, json.loads(raw)
        except Exception:
            if attempt == 2:
                raise
            time.sleep(2**attempt)


def main():
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    end_ms = int(now.timestamp() * 1000) - 1
    sources = {
        "binance": "https://data-api.binance.vision/api/v3/klines?" + urllib.parse.urlencode({"symbol": "BTCUSDT", "interval": "1m", "limit": 2, "endTime": end_ms}),
        "coingecko": "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1&sparkline=false",
        "github": "https://api.github.com/repos/bitcoin/bitcoin/events?per_page=100",
    }
    snapshot = {"observed_at": now.isoformat().replace("+00:00", "Z"), "sources": {}}
    for name, url in sources.items():
        raw, payload = fetch(url)
        if not isinstance(payload, list):
            raise ValueError(f"{name} response is not a list")
        if name == "coingecko":
            payload = [{key: row.get(key) for key in ("id", "symbol", "market_cap_rank", "current_price", "market_cap", "total_volume", "last_updated")} for row in payload]
        elif name == "github":
            payload = [{"id": row.get("id"), "type": row.get("type"), "created_at": row.get("created_at"), "repo": (row.get("repo") or {}).get("name")} for row in payload]
        snapshot["sources"][name] = {"sha256": hashlib.sha256(raw).hexdigest(), "records": payload}
    path = Path("snapshots") / now.strftime("%Y-%m-%d") / f"{now.strftime('%Y%m%dT%H%MZ')}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(snapshot, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    print(path)


if __name__ == "__main__":
    main()
