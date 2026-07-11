import gzip
import hashlib
import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

BINANCE = "https://fapi.binance.com"
COINGECKO = "https://api.coingecko.com/api/v3/coins/markets"
BASKET = Path("config/binance_small_caps_v1.json")


def fetch_raw(url):
    request = urllib.request.Request(url, headers={"User-Agent": "Moneyfactory-Public-Scraper/3", "Accept": "application/json"})
    error = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                raw = response.read()
            json.loads(raw)
            return raw
        except Exception as caught:
            error = caught
            if attempt < 2:
                time.sleep(2**attempt)
    raise RuntimeError(str(error))


def urls(symbols):
    result = {
        "binance_futures_exchange_info": f"{BINANCE}/fapi/v1/exchangeInfo",
        "binance_futures_premium_index": f"{BINANCE}/fapi/v1/premiumIndex",
        "coingecko_markets_001_250": COINGECKO + "?" + urllib.parse.urlencode({"vs_currency": "usd", "order": "market_cap_desc", "per_page": 250, "page": 1, "sparkline": "false"}),
        "coingecko_markets_251_500": COINGECKO + "?" + urllib.parse.urlencode({"vs_currency": "usd", "order": "market_cap_desc", "per_page": 250, "page": 2, "sparkline": "false"}),
    }
    for symbol in ["BTCUSDT", "ETHUSDT", *symbols]:
        result[f"binance_depth_{symbol}"] = f"{BINANCE}/fapi/v1/depth?" + urllib.parse.urlencode({"symbol": symbol, "limit": 50})
    for symbol in symbols:
        result[f"binance_open_interest_{symbol}"] = f"{BINANCE}/fapi/v1/openInterest?" + urllib.parse.urlencode({"symbol": symbol})
        result[f"binance_long_short_{symbol}"] = f"{BINANCE}/futures/data/globalLongShortAccountRatio?" + urllib.parse.urlencode({"symbol": symbol, "period": "30m", "limit": 1})
        result[f"binance_taker_volume_{symbol}"] = f"{BINANCE}/futures/data/takerlongshortRatio?" + urllib.parse.urlencode({"symbol": symbol, "period": "30m", "limit": 1})
    return result


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"


def append_gaps(path, rows):
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    known = set()
    if path.exists():
        known = {json.loads(line)["gap_id"] for line in path.read_text(encoding="utf-8").splitlines() if line}
    with path.open("a", encoding="utf-8", newline="\n") as output:
        for row in rows:
            if row["gap_id"] not in known:
                output.write(canonical(row))


def atomic_write(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_bytes(data)
    os.replace(temporary, path)


def capture(now=None, source_urls=None):
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    slot = now.replace(minute=30 if now.minute >= 30 else 0, second=0, microsecond=0)
    stamp = slot.strftime("%Y%m%dT%H%MZ")
    manifest_path = Path("raw/manifests") / slot.date().isoformat() / f"{stamp}.json"
    if manifest_path.exists():
        return manifest_path

    basket = json.loads(BASKET.read_text(encoding="utf-8"))
    symbols = [row["symbol"] for row in basket["symbols"]]
    skipped = None
    if source_urls is None:
        source_urls = urls(symbols)
        if os.environ.get("GITHUB_ACTIONS") == "true":
            blocked = [name for name in source_urls if name.startswith("binance_")]
            source_urls = {name: url for name, url in source_urls.items() if name not in blocked}
            skipped = {"count": len(blocked), "prefix": "binance_", "reason": "HTTP 451 from GitHub-hosted runner; see changes.log"}
    gaps = []
    checkpoint_path = Path("current/raw_checkpoint.json")
    if checkpoint_path.exists():
        prior = datetime.fromisoformat(json.loads(checkpoint_path.read_text(encoding="utf-8"))["slot"].replace("Z", "+00:00"))
        missed = prior + timedelta(minutes=30)
        while missed < slot:
            gap_id = hashlib.sha256(f"schedule:{missed.isoformat()}".encode()).hexdigest()[:16]
            gaps.append({"gap_id": gap_id, "kind": "schedule_gap", "slot": missed.isoformat().replace("+00:00", "Z"), "expected_sources": sorted(source_urls)})
            missed += timedelta(minutes=30)

    records = {}
    directory = Path("raw/data") / slot.date().isoformat() / stamp
    for name, url in source_urls.items():
        try:
            raw = fetch_raw(url)
            path = directory / f"{name}.json.gz"
            compressed = gzip.compress(raw, compresslevel=9, mtime=0)
            atomic_write(path, compressed)
            records[name] = {"status": "ok", "url": url, "path": path.as_posix(), "sha256": hashlib.sha256(raw).hexdigest(), "raw_bytes": len(raw), "gzip_bytes": len(compressed)}
        except Exception as error:
            gap_id = hashlib.sha256(f"source:{slot.isoformat()}:{name}".encode()).hexdigest()[:16]
            gaps.append({"gap_id": gap_id, "kind": "source_gap", "slot": slot.isoformat().replace("+00:00", "Z"), "source": name, "url": url, "error": str(error)})
            records[name] = {"status": "gap", "url": url, "error": str(error)}

    append_gaps(Path("gaps/gaps.jsonl"), gaps)
    manifest = {
        "slot": slot.isoformat().replace("+00:00", "Z"),
        "fetched_at": now.isoformat().replace("+00:00", "Z"),
        "basket": str(BASKET).replace("\\", "/"),
        "basket_version": basket["version"],
        "skipped": skipped,
        "sources": records,
    }
    atomic_write(manifest_path, canonical(manifest).encode())
    ok = sum(row["status"] == "ok" for row in records.values())
    checkpoint = {"slot": manifest["slot"], "fetched_at": manifest["fetched_at"], "sources_ok": ok, "sources_total": len(records), "sources_skipped": skipped["count"] if skipped else 0, "gaps": len(records) - ok, "manifest_sha256": hashlib.sha256(canonical(manifest).encode()).hexdigest()}
    atomic_write(checkpoint_path, canonical(checkpoint).encode())
    return manifest_path


if __name__ == "__main__":
    print(capture())
