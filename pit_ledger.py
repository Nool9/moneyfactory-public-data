"""PIT ledger v1 r4: deterministic producer/validator core.

Live capture is fail-closed until a separately authorized activation supplies
every boundary value.  Offline fixtures exercise the same code paths.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Iterable

CONTRACT_ID = "PIT_LEDGER_PUBLIC_ONLY_V8"
EPOCH_ID = "BASKET_PIT_LEDGER_TOP250_BINANCE_BYBIT_PUBLIC_V8"
PREFIX = f"pit_ledger/{EPOCH_ID}/"
CONCURRENCY_GROUP = f"pit-ledger-{EPOCH_ID}"
GITHUB_REPO = "git@github.com:Nool9/moneyfactory-public-data.git"
GITHUB_BRANCH = "pit-ledger-public-v8"
AUTHORIZED_WRITER = "PIT Ledger Writer <pit-ledger@users.noreply.github.com>"
SECRET_MOUNT = "/secrets/github/id_ed25519"
KNOWN_HOSTS = "github.com ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl\n"
VENUES = ("BINANCE_USDM", "BYBIT_LINEAR")
SOURCE_ORDER = (
    "CG_TOP250",
    "BN_FUT_EXCHANGE_INFO",
    "BN_FUT_PREMIUM_INDEX",
    "BN_FUT_BOOK_TICKER",
    "BY_LINEAR_INSTRUMENTS",
    "BY_LINEAR_TICKERS",
    "BY_SPOT_INSTRUMENTS",
    "BY_MARGIN_BORROWABLE",
)
URLS = {
    "CG_TOP250": "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=volume_desc&per_page=250&page=1&sparkline=false",
    "BN_FUT_EXCHANGE_INFO": "https://fapi.binance.com/fapi/v1/exchangeInfo",
    "BN_FUT_PREMIUM_INDEX": "https://fapi.binance.com/fapi/v1/premiumIndex",
    "BN_FUT_BOOK_TICKER": "https://fapi.binance.com/fapi/v1/ticker/bookTicker",
    "BY_LINEAR_INSTRUMENTS": "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000",
    "BY_LINEAR_TICKERS": "https://api.bybit.com/v5/market/tickers?category=linear",
    "BY_SPOT_INSTRUMENTS": "https://api.bybit.com/v5/market/instruments-info?category=spot",
    "BY_MARGIN_BORROWABLE": "https://api.bybit.com/v5/spot-margin-trade/data?vipLevel=No%20VIP",
}
MARGIN_TRADING = ("none", "both", "utaOnly", "normalSpotOnly")
MISSING_REASON_ORDER = (
    "NOT_APPLICABLE_NO_PERP",
    "NOT_OBSERVED_PUBLIC_ONLY",
    "INVALID_SYMBOL_FORMAT",
    "AMBIGUOUS_SYMBOL",
    "AMBIGUOUS_EXCHANGE_PRODUCT",
    "SOURCE_FAILURE",
    "PARSE_FAILURE",
    "SCHEMA_FAILURE",
    "DERIVATION_FAILURE",
    "QA_FAILURE",
)
REASON_CODE_ORDER = (
    "NO_CLAIM_BEFORE_DEADLINE",
    "ATTEMPT_ABORTED",
    "NO_RAW_DURABLY_PUBLISHED",
    "SOURCE_FAILURE",
    "PARSE_FAILURE",
    "SCHEMA_FAILURE",
    "DERIVATION_FAILURE",
    "QA_FAILURE",
)
ENUMS = {
    "method": {"GET"},
    "source_id": set(SOURCE_ORDER),
    "venue": set(VENUES),
    "auth_class": {"PUBLIC"},
    "mapping_status": {"MAPPED", "UNMAPPABLE", "AMBIGUOUS"},
    "claim_status": {"CLAIMED"},
    "source_status": {"SOURCE_NOT_RUN", "SOURCE_OK", "SOURCE_FAILURE"},
    "parse_status": {"PARSE_NOT_RUN", "PARSE_OK", "PARSE_FAILURE", "SCHEMA_FAILURE"},
    "qa_status": {"QA_NOT_RUN", "QA_OK", "DERIVATION_FAILURE", "QA_FAILURE"},
    "error_class": {
        "SOURCE_FAILURE",
        "PARSE_FAILURE",
        "SCHEMA_FAILURE",
        "DERIVATION_FAILURE",
        "QA_FAILURE",
    },
    "outcome_kind": {
        "SNAPSHOT_COMPLETE",
        "SNAPSHOT_PARTIAL",
        "SNAPSHOT_INVALID",
        "GAP_UNIVERSE",
        "GAP_NO_RUN",
        "ABORTED_ATTEMPT",
        "INVALID_ATTEMPT",
    },
    "slot_status": {
        "COMPLETE",
        "PARTIAL",
        "INVALID",
        "GAP_UNIVERSE",
        "GAP_NO_RUN",
        "ABORTED_ATTEMPT",
        "INVALID_ATTEMPT",
    },
    "run_status": {
        "CLAIM_NOT_PUBLISHED_NO_REQUEST",
        "PUBLISHED_SLOT_OUTCOME",
        "DUPLICATE_NO_WRITE",
        "CLAIM_HELD_NO_WRITE",
        "ATTEMPT_INTERRUPTED_NO_OUTCOME",
        "EPOCH_WRITER_STOP",
    },
    "marginTrading": set(MARGIN_TRADING),
}
OUTCOME_STATUS = {
    "SNAPSHOT_COMPLETE": "COMPLETE",
    "SNAPSHOT_PARTIAL": "PARTIAL",
    "SNAPSHOT_INVALID": "INVALID",
    "GAP_UNIVERSE": "GAP_UNIVERSE",
    "GAP_NO_RUN": "GAP_NO_RUN",
    "ABORTED_ATTEMPT": "ABORTED_ATTEMPT",
    "INVALID_ATTEMPT": "INVALID_ATTEMPT",
}
UTC_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")
SYMBOL_RE = re.compile(r"^[A-Za-z0-9]+$")
DECIMAL_RE = re.compile(r"^-?(?:0|[1-9]\d*)(?:\.\d+)?$")
FORBIDDEN_FIELDS = {"reason", "message", "detail", "diagnostic"}
MAX_UNIX_MS = 253402300799999
SHA256_RE = re.compile(r"^[0-9A-F]{64}$")
IMPLEMENTATION_FILES = ("Dockerfile", "pit_ledger.py")
LEDGER_INDEX_PATH = PREFIX + "ledger/index.jsonl"


class PitError(ValueError):
    """Fail-closed contract violation."""


def _array_key(item: Any) -> bytes:
    return json.dumps(item, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


ARRAY_KEYS = {
    "assets", "available_raw_relative_paths", "available_raw_sha256s", "events",
    "files", "missing_reasons", "raw_references", "reason_codes",
    "source_manifests", "source_manifest_sha256s", "source_raw_sha256s", "venues",
}


def _validate_derived(value: Any, path: tuple[str, ...] = ()) -> None:
    if value is None or isinstance(value, (bool, str)):
        return
    if isinstance(value, int) and not isinstance(value, bool):
        return
    if isinstance(value, list):
        key = path[-1] if path else None
        if key not in ARRAY_KEYS and not (isinstance(key, str) and key.endswith("_sha256s")):
            raise PitError("UNCLASSIFIED_ARRAY")
        if key in {"missing_reasons", "reason_codes"}:
            validate_ordered(value, MISSING_REASON_ORDER if key == "missing_reasons" else REASON_CODE_ORDER)
        elif key.endswith("_sha256s"):
            if value != sorted(set(value)) or any(not isinstance(x, str) or not SHA256_RE.fullmatch(x) for x in value):
                raise PitError("NON_CANONICAL_HASH_ORDER")
        elif key == "available_raw_relative_paths":
            def raw_key(path: str) -> tuple[int, int]:
                name = path.rsplit("/", 1)[-1]
                matches = [(SOURCE_ORDER.index(source), name[len(source) + 1:].split(".", 1)[0]) for source in SOURCE_ORDER if name.startswith(source + "-")]
                if len(matches) != 1 or not matches[0][1].isdigit():
                    raise PitError("NON_CANONICAL_RAW_ORDER")
                return matches[0][0], int(matches[0][1])
            if any(not isinstance(x, str) for x in value) or value != sorted(value, key=raw_key):
                raise PitError("NON_CANONICAL_RAW_ORDER")
        elif key == "files":
            if any(not isinstance(x, dict) or not isinstance(x.get("path"), str) for x in value) or value != sorted(value, key=lambda x: x["path"]):
                raise PitError("NON_CANONICAL_FILE_ORDER")
        elif key == "assets":
            if [x.get("universe_rank") for x in value if isinstance(x, dict)] != list(range(1, len(value) + 1)):
                raise PitError("NON_CANONICAL_ASSET_ORDER")
        elif key == "venues":
            if [x.get("venue") for x in value if isinstance(x, dict)] != list(VENUES):
                raise PitError("NON_CANONICAL_VENUE_ORDER")
        elif key in {"source_manifests", "raw_references"}:
            def source_key(x: dict[str, Any]) -> tuple[int, int]:
                return SOURCE_ORDER.index(x["source_id"]), x.get("page_ordinal", 0)
            if any(not isinstance(x, dict) or x.get("source_id") not in SOURCE_ORDER for x in value) or value != sorted(value, key=source_key):
                raise PitError("NON_CANONICAL_SOURCE_ORDER")
        elif key is not None:
            ordered_values = sorted(value, key=_array_key)
            if value != ordered_values:
                raise PitError("AMBIGUOUS_ARRAY_ORDER")
        for item in value:
            _validate_derived(item, path + ("[]",))
        return
    if isinstance(value, dict) and all(isinstance(key, str) for key in value):
        for key, item in value.items():
            if key in FORBIDDEN_FIELDS:
                raise PitError("FREE_FORM_FIELD")
            if key == "I_impl" or key.endswith("_sha256"):
                if item is not None and (not isinstance(item, str) or not SHA256_RE.fullmatch(item)):
                    raise PitError("INVALID_SHA256")
            _validate_derived(item, path + (key,))
        return
    raise PitError("NON_CANONICAL_SCALAR")


def canonical_bytes(value: Any) -> bytes:
    _validate_derived(value)
    validate_normative(value)
    return (
        json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def canonical_jsonl(records: Iterable[dict[str, Any]]) -> bytes:
    rows = list(records)
    if not rows:
        raise PitError("EMPTY_JSONL")
    seq = [row.get("ledger_seq") for row in rows]
    if any(type(item) is not int for item in seq) or seq != sorted(seq) or len(seq) != len(set(seq)):
        raise PitError("NON_CANONICAL_LEDGER_ORDER")
    return b"".join(canonical_bytes(row) for row in rows)


def validate_ledger_index(data: bytes) -> list[dict[str, Any]]:
    if not data or not data.endswith(b"\n") or b"\r" in data:
        raise PitError("NON_CANONICAL_JSONL")
    records = [validate_canonical(line + b"\n") for line in data.splitlines()]
    if any(not isinstance(row, dict) for row in records):
        raise PitError("NON_CANONICAL_JSONL")
    if [row.get("ledger_seq") for row in records] != list(range(1, len(records) + 1)):
        raise PitError("NON_CANONICAL_LEDGER_ORDER")
    if canonical_jsonl(records) != data:
        raise PitError("NON_CANONICAL_JSONL")
    return records


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest().upper()


def validate_canonical(data: bytes) -> Any:
    if data.startswith(b"\xef\xbb\xbf") or b"\r" in data or not data.endswith(b"\n"):
        raise PitError("NON_CANONICAL_JSON")
    value = json.loads(data)
    if canonical_bytes(value) != data:
        raise PitError("NON_CANONICAL_JSON")
    validate_normative(value)
    return value


def ordered(values: Iterable[str], order: tuple[str, ...]) -> list[str]:
    values = list(values)
    if any(value not in order for value in values):
        raise PitError("UNKNOWN_REASON")
    return [value for value in order if value in set(values)]


def validate_ordered(values: Any, order: tuple[str, ...]) -> None:
    if not isinstance(values, list) or values != ordered(values, order) or len(values) != len(set(values)):
        raise PitError("NON_CANONICAL_REASON_ORDER")


def validate_normative(value: Any, key: str | None = None) -> None:
    if isinstance(value, list):
        _validate_derived(value, (key,) if key else ())
        return
    if not isinstance(value, dict):
        return
    for key, item in value.items():
        if key in FORBIDDEN_FIELDS:
            raise PitError("FREE_FORM_FIELD")
        if key == "I_impl" or key.endswith("_sha256"):
            if item is not None and (not isinstance(item, str) or not SHA256_RE.fullmatch(item)):
                raise PitError("INVALID_SHA256")
        if key in ENUMS and item not in ENUMS[key]:
            raise PitError("UNKNOWN_ENUM")
        if key == "http_status" and not (item is None or type(item) is int and 100 <= item <= 599):
            raise PitError("UNKNOWN_HTTP_STATUS")
        if key in {"perp_exists", "borrowable"} and item is not None and type(item) is not bool:
            raise PitError("NON_TRISTATE")
        if key == "missing_reasons":
            validate_ordered(item, MISSING_REASON_ORDER)
        if key == "reason_codes":
            validate_ordered(item, REASON_CODE_ORDER)
        if key.endswith("_utc") and item is not None:
            validate_utc(item)
        validate_normative(item, key)
    if "outcome_kind" in value and value.get("slot_status") != OUTCOME_STATUS[value["outcome_kind"]]:
        raise PitError("OUTCOME_STATUS_MISMATCH")
    if (
        value.get("mapping_status") == "MAPPED"
        and value.get("perp_exists") is None
        and value.get("qa_status") == "QA_FAILURE"
        and value.get("source_id") in {"BN_FUT_EXCHANGE_INFO", "BY_LINEAR_INSTRUMENTS"}
        and value.get("missing_reasons") != ["AMBIGUOUS_EXCHANGE_PRODUCT", "QA_FAILURE"]
    ):
        raise PitError("R3_01_ORACLE_MISMATCH")


def validate_utc(value: str) -> None:
    if not isinstance(value, str) or not UTC_RE.fullmatch(value):
        raise PitError("INVALID_UTC")
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError as exc:
        raise PitError("INVALID_UTC") from exc


def utc_from_ms(value: Any) -> str:
    if type(value) is not int or not 0 <= value <= MAX_UNIX_MS:
        raise PitError("SCHEMA_FAILURE")
    try:
        observed = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=value)
    except OverflowError as exc:
        raise PitError("SCHEMA_FAILURE") from exc
    return observed.strftime("%Y-%m-%dT%H:%M:%S.") + f"{value % 1000:03d}Z"


def ascii_upper(value: str) -> str:
    return "".join(chr(ord(char) - 32) if "a" <= char <= "z" else char for char in value)


def _reject_constant(_: str) -> None:
    raise PitError("SCHEMA_FAILURE")


def universe_gap(code: str) -> dict[str, Any]:
    parse_status = "PARSE_FAILURE" if code == "PARSE_FAILURE" else "SCHEMA_FAILURE"
    qa_status = "QA_NOT_RUN"
    if code in {"DERIVATION_FAILURE", "QA_FAILURE"}:
        parse_status = "PARSE_OK"
        qa_status = code
    return {
        "error_class": code,
        "missing_reasons": [] if code == "QA_FAILURE" else [code],
        "outcome_kind": "GAP_UNIVERSE",
        "parse_status": parse_status,
        "qa_status": qa_status,
        "reason_codes": [code],
        "slot_status": "GAP_UNIVERSE",
        "source_id": "CG_TOP250",
        "source_status": "SOURCE_OK" if code != "SOURCE_FAILURE" else "SOURCE_FAILURE",
    }


def parse_universe(raw: bytes) -> tuple[list[dict[str, Any]] | None, dict[str, Any] | None]:
    try:
        rows = json.loads(
            raw,
            parse_int=Decimal,
            parse_float=Decimal,
            parse_constant=_reject_constant,
        )
    except (json.JSONDecodeError, InvalidOperation):
        return None, universe_gap("PARSE_FAILURE")
    except PitError:
        return None, universe_gap("SCHEMA_FAILURE")
    if not isinstance(rows, list) or len(rows) != 250:
        return None, universe_gap("SCHEMA_FAILURE")
    ids: set[str] = set()
    previous: Decimal | None = None
    for row in rows:
        if not isinstance(row, dict) or any(
            key not in row for key in ("id", "symbol", "current_price", "total_volume", "market_cap_rank")
        ):
            return None, universe_gap("SCHEMA_FAILURE")
        if not isinstance(row["id"], str) or not row["id"] or row["id"] in ids:
            return None, universe_gap("SCHEMA_FAILURE")
        ids.add(row["id"])
        volume = row["total_volume"]
        if volume is not None and not isinstance(volume, Decimal):
            return None, universe_gap("SCHEMA_FAILURE")
        if volume is not None and (not volume.is_finite()):
            return None, universe_gap("SCHEMA_FAILURE")
        if volume is not None and previous is not None and previous < volume:
            return None, universe_gap("QA_FAILURE")
        if volume is not None:
            previous = volume
    return rows, None


def symbol_decisions(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for row in rows:
        symbol = row.get("symbol")
        if isinstance(symbol, str) and SYMBOL_RE.fullmatch(symbol):
            base = ascii_upper(symbol)
            counts[base] = counts.get(base, 0) + 1
    result = []
    for rank, row in enumerate(rows, 1):
        symbol = row.get("symbol")
        if not isinstance(symbol, str) or not SYMBOL_RE.fullmatch(symbol):
            result.append({"universe_rank": rank, "mapping_status": "UNMAPPABLE", "base": None, "exchange_symbol": None, "missing_reasons": ["INVALID_SYMBOL_FORMAT"]})
            continue
        base = ascii_upper(symbol)
        if counts[base] > 1:
            result.append({"universe_rank": rank, "mapping_status": "AMBIGUOUS", "base": None, "exchange_symbol": None, "missing_reasons": ["AMBIGUOUS_SYMBOL"]})
            continue
        result.append({"universe_rank": rank, "mapping_status": "MAPPED", "base": base, "exchange_symbol": base + "USDT", "missing_reasons": []})
    return result


def perp_decision(venue: str, base: str, instruments: Any, source_complete: bool = True) -> dict[str, Any]:
    if venue not in VENUES or not source_complete or not isinstance(instruments, list):
        return {"perp_exists": None, "missing_reasons": ["SCHEMA_FAILURE"]}
    candidate = base + "USDT"
    if venue == "BINANCE_USDM":
        wanted = {
            "symbol": candidate,
            "baseAsset": base,
            "quoteAsset": "USDT",
            "contractType": "PERPETUAL",
            "status": "TRADING",
        }
    else:
        wanted = {
            "symbol": candidate,
            "baseCoin": base,
            "quoteCoin": "USDT",
            "settleCoin": "USDT",
            "contractType": "LinearPerpetual",
            "status": "Trading",
        }
    matches = [row for row in instruments if isinstance(row, dict) and all(row.get(k) == v for k, v in wanted.items())]
    if len(matches) == 1:
        return {"perp_exists": True, "missing_reasons": []}
    if not matches:
        return {"perp_exists": False, "missing_reasons": ["NOT_APPLICABLE_NO_PERP"]}
    return {
        "mapping_status": "MAPPED",
        "missing_reasons": ["AMBIGUOUS_EXCHANGE_PRODUCT", "QA_FAILURE"],
        "outcome_kind": "SNAPSHOT_PARTIAL",
        "perp_exists": None,
        "qa_status": "QA_FAILURE",
        "reason_codes": ["QA_FAILURE"],
        "slot_status": "PARTIAL",
        "source_id": "BN_FUT_EXCHANGE_INFO" if venue == "BINANCE_USDM" else "BY_LINEAR_INSTRUMENTS",
    }


def _unique(rows: Any, predicate: Callable[[dict[str, Any]], bool]) -> dict[str, Any] | None:
    if not isinstance(rows, list):
        raise PitError("SCHEMA_FAILURE")
    matches = [row for row in rows if isinstance(row, dict) and predicate(row)]
    if len(matches) > 1:
        raise PitError("SCHEMA_FAILURE")
    return matches[0] if matches else None


def borrowable_bybit(base: str, currencies: Any, spots: Any) -> bool | None:
    try:
        if not isinstance(spots, list) or any(
            not isinstance(row, dict) or row.get("marginTrading") not in MARGIN_TRADING for row in spots
        ):
            raise PitError("SCHEMA_FAILURE")
        currency = _unique(currencies, lambda row: row.get("currency") == base)
        spot = _unique(spots, lambda row: row.get("baseCoin") == base and row.get("quoteCoin") == "USDT")
        if currency is not None and type(currency.get("borrowable")) is not bool:
            raise PitError("SCHEMA_FAILURE")
        return bool(currency and spot and currency["borrowable"] and spot["marginTrading"] in {"utaOnly", "both"})
    except PitError:
        return None


def parse_decimal_string(value: Any, positive: bool = False) -> Decimal:
    if not isinstance(value, str) or not DECIMAL_RE.fullmatch(value):
        raise PitError("SCHEMA_FAILURE")
    number = Decimal(value)
    if not number.is_finite() or positive and number <= 0:
        raise PitError("SCHEMA_FAILURE")
    return number


def decimal_coefficient(value: Any) -> tuple[int, int]:
    if not isinstance(value, str) or not DECIMAL_RE.fullmatch(value):
        raise PitError("SCHEMA_FAILURE")
    negative = value.startswith("-")
    whole, dot, fraction = value.lstrip("-").partition(".")
    coefficient = int(whole + fraction)
    return (-coefficient if negative else coefficient), len(fraction) if dot else 0


def spread_bps(bid: Any, ask: Any) -> str:
    b, bs = decimal_coefficient(bid)
    a, ass = decimal_coefficient(ask)
    scale = max(bs, ass)
    B, A = b * 10 ** (scale - bs), a * 10 ** (scale - ass)
    if B <= 0 or A <= 0 or B > A:
        raise PitError("SCHEMA_FAILURE")
    numerator, denominator = 20000 * (A - B), A + B
    q, remainder = divmod(numerator * 10**8, denominator)
    if 2 * remainder > denominator or 2 * remainder == denominator and q % 2:
        q += 1
    return f"{q // 10**8}.{q % 10**8:08d}"


def funding_observation(source_id: str, row: dict[str, Any], body: dict[str, Any]) -> tuple[str, str]:
    if source_id == "BN_FUT_PREMIUM_INDEX":
        stamp, rate = row.get("time"), row.get("lastFundingRate")
    elif source_id == "BY_LINEAR_TICKERS":
        stamp, rate = body.get("time"), row.get("fundingRate")
    else:
        raise PitError("UNKNOWN_SOURCE")
    parse_decimal_string(rate)
    return rate, utc_from_ms(stamp)


def funding_schema_failure(source_id: str) -> dict[str, Any]:
    return {
        "error_class": "SCHEMA_FAILURE",
        "funding_observed_at_utc": None,
        "funding_rate": None,
        "missing_reasons": ["SCHEMA_FAILURE"],
        "outcome_kind": "SNAPSHOT_PARTIAL",
        "parse_status": "SCHEMA_FAILURE",
        "qa_status": "QA_NOT_RUN",
        "reason_codes": ["SCHEMA_FAILURE"],
        "slot_status": "PARTIAL",
        "source_id": source_id,
        "source_status": "SOURCE_OK",
    }


def _rows(value: Any, *path: str) -> list[dict[str, Any]]:
    for key in path:
        if not isinstance(value, dict) or key not in value:
            raise PitError("SCHEMA_FAILURE")
        value = value[key]
    if not isinstance(value, list) or any(not isinstance(row, dict) for row in value):
        raise PitError("SCHEMA_FAILURE")
    return value


def _bybit_borrowable_rows(body: Any) -> list[dict[str, Any]]:
    envelopes = _rows(body, "result", "vipCoinList")
    if len(envelopes) != 1 or envelopes[0].get("vipLevel") != "No VIP":
        raise PitError("SCHEMA_FAILURE")
    return _rows(envelopes[0], "list")


def _ticker_record(
    venue: str,
    candidate: str,
    premium: Any,
    books: Any,
    bybit_body: Any,
) -> dict[str, Any]:
    if venue == "BINANCE_USDM":
        premium_row = _unique(premium, lambda row: row.get("symbol") == candidate)
        book_row = _unique(books, lambda row: row.get("symbol") == candidate)
        if premium_row is None or book_row is None:
            raise PitError("SCHEMA_FAILURE")
        rate, observed = funding_observation("BN_FUT_PREMIUM_INDEX", premium_row, {})
        bid, ask = book_row.get("bidPrice"), book_row.get("askPrice")
        mark, index = premium_row.get("markPrice"), premium_row.get("indexPrice")
    else:
        tickers = _rows(bybit_body, "result", "list")
        row = _unique(tickers, lambda item: item.get("symbol") == candidate)
        if row is None:
            raise PitError("SCHEMA_FAILURE")
        rate, observed = funding_observation("BY_LINEAR_TICKERS", row, bybit_body)
        bid, ask = row.get("bid1Price"), row.get("ask1Price")
        mark, index = row.get("markPrice"), row.get("indexPrice")
    for value in (bid, ask, mark, index):
        parse_decimal_string(value, positive=True)
    return {
        "ask_price": ask,
        "bid_price": bid,
        "funding_observed_at_utc": observed,
        "funding_rate": rate,
        "index_price": index,
        "mark_price": mark,
        "spread_bps": spread_bps(bid, ask),
    }


def build_snapshot(
    raw_universe: bytes,
    sources: dict[str, Any],
    claim: dict[str, Any],
) -> dict[str, Any]:
    rows, gap = parse_universe(raw_universe)
    if gap is not None:
        return gap
    source_manifests = sources.get("__source_manifests__", [])
    source_errors: dict[str, str] = {}
    for manifest in source_manifests:
        statuses = (
            manifest.get("source_status"),
            manifest.get("parse_status"),
            manifest.get("qa_status"),
        )
        code = {
            ("SOURCE_FAILURE", "PARSE_NOT_RUN", "QA_NOT_RUN"): "SOURCE_FAILURE",
            ("SOURCE_OK", "PARSE_FAILURE", "QA_NOT_RUN"): "PARSE_FAILURE",
            ("SOURCE_OK", "SCHEMA_FAILURE", "QA_NOT_RUN"): "SCHEMA_FAILURE",
            ("SOURCE_OK", "PARSE_OK", "DERIVATION_FAILURE"): "DERIVATION_FAILURE",
            ("SOURCE_OK", "PARSE_OK", "QA_FAILURE"): "QA_FAILURE",
        }.get(statuses)
        if code is not None:
            source_errors[manifest["source_id"]] = code
    primary_error = next(
        (source_errors[source_id] for source_id in SOURCE_ORDER if source_id in source_errors),
        None,
    )
    for source_id in SOURCE_ORDER[1:]:
        if source_id not in sources:
            source_errors.setdefault(source_id, primary_error or "SCHEMA_FAILURE")

    def source_rows(source_id: str, *path: str) -> list[dict[str, Any]] | None:
        nonlocal primary_error
        if source_id in source_errors:
            return None
        try:
            return _bybit_borrowable_rows(sources[source_id]) if source_id == "BY_MARGIN_BORROWABLE" else _rows(sources[source_id], *path)
        except (KeyError, TypeError, PitError):
            source_errors[source_id] = "SCHEMA_FAILURE"
            primary_error = primary_error or "SCHEMA_FAILURE"
            return None

    bn_instruments = source_rows("BN_FUT_EXCHANGE_INFO", "symbols")
    bn_premium = source_rows("BN_FUT_PREMIUM_INDEX")
    bn_books = source_rows("BN_FUT_BOOK_TICKER")
    by_spots = source_rows("BY_SPOT_INSTRUMENTS", "result", "list")
    by_currencies = source_rows("BY_MARGIN_BORROWABLE")
    by_instruments = None
    if "BY_LINEAR_INSTRUMENTS" not in source_errors:
        try:
            by_instruments = []
            for page in sources["BY_LINEAR_INSTRUMENTS"]:
                by_instruments.extend(_rows(page, "result", "list"))
        except (KeyError, TypeError, PitError):
            source_errors["BY_LINEAR_INSTRUMENTS"] = "SCHEMA_FAILURE"
            primary_error = primary_error or "SCHEMA_FAILURE"
            by_instruments = None

    decisions, assets = symbol_decisions(rows), []
    reason_codes = list(source_errors.values())
    raw_by_source: dict[str, list[str]] = {}
    for manifest in source_manifests:
        if manifest.get("raw_sha256"):
            raw_by_source.setdefault(manifest["source_id"], []).append(manifest["raw_sha256"])
    for row, mapping in zip(rows, decisions):
        venue_rows = []
        for venue in VENUES:
            empty = {
                "ask_price": None,
                "bid_price": None,
                "borrowable": None,
                "exchange_symbol": mapping["exchange_symbol"],
                "funding_observed_at_utc": None,
                "funding_rate": None,
                "index_price": None,
                "mapping_status": mapping["mapping_status"],
                "mark_price": None,
                "missing_reasons": list(mapping["missing_reasons"]),
                "perp_exists": None,
                "source_raw_sha256s": [],
                "spread_bps": None,
                "venue": venue,
            }
            if mapping["mapping_status"] != "MAPPED":
                venue_rows.append(empty)
                continue
            base, candidate = mapping["base"], mapping["exchange_symbol"]
            if venue == "BINANCE_USDM":
                product_reason = source_errors.get("BN_FUT_EXCHANGE_INFO")
                product = (
                    {"perp_exists": None, "missing_reasons": [product_reason]}
                    if product_reason else perp_decision(venue, base, bn_instruments)
                )
                borrow_reason = "NOT_OBSERVED_PUBLIC_ONLY"
                borrowable = None
                used_sources = ["BN_FUT_EXCHANGE_INFO"]
                ticker_reason = source_errors.get("BN_FUT_PREMIUM_INDEX") or source_errors.get("BN_FUT_BOOK_TICKER")
            else:
                product_reason = source_errors.get("BY_LINEAR_INSTRUMENTS")
                product = (
                    {"perp_exists": None, "missing_reasons": [product_reason]}
                    if product_reason else perp_decision(venue, base, by_instruments)
                )
                borrow_reason = source_errors.get("BY_SPOT_INSTRUMENTS") or source_errors.get("BY_MARGIN_BORROWABLE")
                borrowable = None if borrow_reason else borrowable_bybit(base, by_currencies, by_spots)
                used_sources = ["BY_LINEAR_INSTRUMENTS", "BY_SPOT_INSTRUMENTS", "BY_MARGIN_BORROWABLE"]
                ticker_reason = source_errors.get("BY_LINEAR_TICKERS")
            empty["borrowable"] = borrowable
            empty["perp_exists"] = product["perp_exists"]
            empty["missing_reasons"] = list(product["missing_reasons"])
            if borrowable is None:
                borrow_reason = borrow_reason or "SCHEMA_FAILURE"
                empty["missing_reasons"] = ordered(empty["missing_reasons"] + [borrow_reason], MISSING_REASON_ORDER)
                if borrow_reason in REASON_CODE_ORDER:
                    reason_codes.append(borrow_reason)
            if product["perp_exists"] is True:
                used_sources += ["BN_FUT_PREMIUM_INDEX", "BN_FUT_BOOK_TICKER"] if venue == "BINANCE_USDM" else ["BY_LINEAR_TICKERS"]
                if ticker_reason:
                    empty["missing_reasons"] = ordered(empty["missing_reasons"] + [ticker_reason], MISSING_REASON_ORDER)
                    reason_codes.append(ticker_reason)
                else:
                    try:
                        empty.update(
                            _ticker_record(
                                venue,
                                candidate,
                                bn_premium,
                                bn_books,
                                sources["BY_LINEAR_TICKERS"],
                            )
                        )
                    except (KeyError, PitError):
                        empty["missing_reasons"] = ordered(empty["missing_reasons"] + ["SCHEMA_FAILURE"], MISSING_REASON_ORDER)
                        reason_codes.append("SCHEMA_FAILURE")
            elif product["perp_exists"] is None:
                reason_codes.extend(
                    code for code in product["missing_reasons"] if code in REASON_CODE_ORDER
                )
            empty["source_raw_sha256s"] = sorted({item for source in used_sources for item in raw_by_source.get(source, [])})
            venue_rows.append(empty)
        assets.append(
            {
                "coingecko_id": row["id"],
                "coingecko_symbol": row["symbol"],
                "universe_rank": mapping["universe_rank"],
                "venues": venue_rows,
            }
        )
    reason_codes = ordered(reason_codes, REASON_CODE_ORDER)
    kind = "SNAPSHOT_COMPLETE" if not reason_codes else "SNAPSHOT_PARTIAL"
    value = claim["value"]
    manifests_hashes = [sha256(canonical_bytes(item)) for item in source_manifests]
    raw_manifests = [item for item in source_manifests if item.get("raw_sha256")]
    snapshot = {
        "I_impl": value["I_impl"],
        "available_raw_count": len(raw_manifests),
        "available_raw_relative_paths": [item["raw_relative_path"] for item in raw_manifests],
        "available_raw_sha256s": sorted({item["raw_sha256"] for item in raw_manifests}),
        "assets": assets,
        "attempt_id": value["attempt_id"],
        "attempt_started_at_utc": value["claimed_at_utc"],
        "capture_completed_at_utc": value["claimed_at_utc"],
        "claim_sha256": sha256(canonical_bytes(value)),
        "contract_id": CONTRACT_ID,
        "epoch_id": EPOCH_ID,
        "formal_slot_utc": value["formal_slot_utc"],
        "idempotency_key": value["idempotency_key"],
        "materialized_at_utc": value["claimed_at_utc"],
        "image_digest": value["image_digest"],
        "outcome_kind": kind,
        "qa": {"qa_status": "QA_OK" if not reason_codes else "QA_FAILURE", "reason_codes": reason_codes},
        "reason_codes": reason_codes,
        "slot_status": OUTCOME_STATUS[kind],
        "source_manifest_sha256s": sorted(set(manifests_hashes)),
        "universe_raw_sha256": sha256(raw_universe),
        "universe_source_id": "CG_TOP250",
        "workflow_run_id": value["workflow_run_id"],
        "job_id": value["job_id"],
        "log_locator": value["log_locator"],
        "log_sha256": sha256(run_log_bytes(claim)),
        "claim_relative_path": claim["relative_path"],
        "previous_ledger_head": value["expected_parent_before_claim"],
    }
    validate_snapshot(snapshot)
    return snapshot


def validate_snapshot(
    snapshot: dict[str, Any],
    claim: dict[str, Any] | None = None,
    source_manifests: list[dict[str, Any]] | None = None,
    history: GitContext | None = None,
    require_context: bool = False,
    expected_i_impl: str | None = None,
) -> None:
    validate_normative(snapshot)
    if snapshot.get("contract_id") != CONTRACT_ID or snapshot.get("epoch_id") != EPOCH_ID:
        raise PitError("IDENTITY_MISMATCH")
    if not SHA256_RE.fullmatch(snapshot.get("I_impl", "")):
        raise PitError("IMPLEMENTATION_MISMATCH")
    if expected_i_impl is not None and snapshot["I_impl"] != expected_i_impl:
        raise PitError("IMPLEMENTATION_MISMATCH")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", snapshot.get("image_digest", "")):
        raise PitError("IMAGE_PROVENANCE")
    slot = snapshot.get("formal_slot_utc")
    if snapshot.get("idempotency_key") != EPOCH_ID + "|" + str(slot):
        raise PitError("IDEMPOTENCY_MISMATCH")
    for key in ("attempt_started_at_utc", "capture_completed_at_utc", "materialized_at_utc", "formal_slot_utc"):
        validate_utc(snapshot.get(key))
    if snapshot.get("outcome_kind") not in {"SNAPSHOT_COMPLETE", "SNAPSHOT_PARTIAL", "SNAPSHOT_INVALID"}:
        raise PitError("INVALID_SNAPSHOT_KIND")
    if set(snapshot.get("qa", {})) != {"qa_status", "reason_codes"}:
        raise PitError("INVALID_QA_SHAPE")
    if snapshot["qa"]["reason_codes"] != snapshot.get("reason_codes"):
        raise PitError("QA_REASON_MISMATCH")
    kind, reasons, qa_status = snapshot["outcome_kind"], snapshot["reason_codes"], snapshot["qa"]["qa_status"]
    if (
        kind == "SNAPSHOT_COMPLETE" and (reasons != [] or qa_status != "QA_OK")
        or kind == "SNAPSHOT_PARTIAL" and (not reasons or qa_status != "QA_FAILURE")
        or kind == "SNAPSHOT_INVALID" and (reasons != ["QA_FAILURE"] or qa_status != "QA_FAILURE")
    ):
        raise PitError("OUTCOME_QA_RELATION")
    assets = snapshot.get("assets")
    if not isinstance(assets, list) or len(assets) != 250:
        raise PitError("INVALID_ASSET_COUNT")
    if [asset.get("universe_rank") for asset in assets] != list(range(1, 251)):
        raise PitError("INVALID_RANKS")
    ids = [asset.get("coingecko_id") for asset in assets]
    if any(not isinstance(item, str) or not item for item in ids) or len(ids) != len(set(ids)):
        raise PitError("INVALID_IDS")
    symbols = [asset.get("coingecko_symbol") for asset in assets]
    valid_bases = [ascii_upper(x) for x in symbols if isinstance(x, str) and SYMBOL_RE.fullmatch(x)]
    counts = {base: valid_bases.count(base) for base in set(valid_bases)}
    all_venue_rows = 0
    for asset in assets:
        if set(asset) != {"coingecko_id", "coingecko_symbol", "universe_rank", "venues"}:
            raise PitError("INVALID_ASSET_SHAPE")
        venues = asset.get("venues")
        if not isinstance(venues, list) or [row.get("venue") for row in venues] != list(VENUES):
            raise PitError("INVALID_VENUES")
        all_venue_rows += len(venues)
        for row in venues:
            required = {
                "venue", "mapping_status", "exchange_symbol", "perp_exists",
                "funding_rate", "funding_observed_at_utc", "bid_price", "ask_price",
                "spread_bps", "mark_price", "index_price", "borrowable",
                "missing_reasons", "source_raw_sha256s",
            }
            if set(row) != required:
                raise PitError("MISSING_VENUE_FIELD")
            symbol = asset["coingecko_symbol"]
            if not isinstance(symbol, str) or not SYMBOL_RE.fullmatch(symbol):
                expected_mapping, expected_reason = "UNMAPPABLE", ["INVALID_SYMBOL_FORMAT"]
            elif counts[ascii_upper(symbol)] > 1:
                expected_mapping, expected_reason = "AMBIGUOUS", ["AMBIGUOUS_SYMBOL"]
            else:
                expected_mapping, expected_reason = "MAPPED", None
            if row["mapping_status"] != expected_mapping:
                raise PitError("MAPPING_RELATION")
            prices = ("funding_rate", "funding_observed_at_utc", "bid_price", "ask_price", "spread_bps", "mark_price", "index_price")
            if expected_mapping != "MAPPED":
                if row["exchange_symbol"] is not None or row["perp_exists"] is not None or row["borrowable"] is not None:
                    raise PitError("MAPPING_RELATION")
                if row["missing_reasons"] != expected_reason or any(row[field] is not None for field in prices):
                    raise PitError("MAPPING_RELATION")
            else:
                candidate = ascii_upper(symbol) + "USDT"
                if row["exchange_symbol"] != candidate:
                    raise PitError("MAPPING_RELATION")
                source_errors = {"SOURCE_FAILURE", "PARSE_FAILURE", "SCHEMA_FAILURE", "DERIVATION_FAILURE", "QA_FAILURE"}
                binance_public_only = row["venue"] == "BINANCE_USDM"
                public_only_reason = {"NOT_OBSERVED_PUBLIC_ONLY"} if binance_public_only else set()
                if (
                    binance_public_only and (
                        row["borrowable"] is not None
                        or "NOT_OBSERVED_PUBLIC_ONLY" not in row["missing_reasons"]
                    )
                    or not binance_public_only and "NOT_OBSERVED_PUBLIC_ONLY" in row["missing_reasons"]
                ):
                    raise PitError("BORROWABLE_RELATION")
                if row["perp_exists"] is False:
                    base_reasons = {"NOT_APPLICABLE_NO_PERP"} | public_only_reason
                    error_reasons = set(row["missing_reasons"]) - base_reasons
                    error_state = (
                        bool(error_reasons)
                        and error_reasons <= source_errors
                        and error_reasons <= set(reasons)
                    )
                    expected_reasons = ordered(
                        list(base_reasons | error_reasons),
                        MISSING_REASON_ORDER,
                    )
                    if (
                        any(row[field] is not None for field in prices)
                        or row["missing_reasons"] != expected_reasons
                        or not binance_public_only and row["borrowable"] is None and (
                            not error_state or row["missing_reasons"] != expected_reasons
                        )
                        or not binance_public_only and row["borrowable"] is not None and error_reasons
                    ):
                        raise PitError("PERP_RELATION")
                elif row["perp_exists"] is True:
                    market_complete = all(row[field] is not None for field in prices)
                    market_missing = all(row[field] is None for field in prices)
                    error_reasons = set(row["missing_reasons"]) - public_only_reason
                    error_state = bool(error_reasons) and error_reasons <= source_errors and error_reasons <= set(reasons)
                    if not market_complete and not (market_missing and error_state):
                        raise PitError("PERP_RELATION")
                    if market_complete:
                        if row["spread_bps"] != spread_bps(row["bid_price"], row["ask_price"]):
                            raise PitError("SPREAD_RELATION")
                        validate_utc(row["funding_observed_at_utc"])
                        for field in ("funding_rate", "bid_price", "ask_price", "mark_price", "index_price"):
                            parse_decimal_string(row[field], positive=field != "funding_rate")
                    if not binance_public_only and row["borrowable"] is None and not error_state:
                        raise PitError("BORROWABLE_RELATION")
                    if (
                        error_reasons and not error_state
                        or market_complete and row["borrowable"] is not None and error_reasons
                        or market_complete and not error_reasons
                        and row["missing_reasons"] != ordered(public_only_reason, MISSING_REASON_ORDER)
                    ):
                        raise PitError("PERP_RELATION")
                else:
                    if any(row[field] is not None for field in prices):
                        raise PitError("PERP_RELATION")
                    allowed_null_reasons = {
                        ("AMBIGUOUS_EXCHANGE_PRODUCT", "QA_FAILURE"),
                        ("SOURCE_FAILURE",), ("PARSE_FAILURE",), ("SCHEMA_FAILURE",),
                        ("DERIVATION_FAILURE",), ("QA_FAILURE",),
                    }
                    product_reasons = tuple(
                        reason for reason in row["missing_reasons"]
                        if reason != "NOT_OBSERVED_PUBLIC_ONLY"
                    )
                    if product_reasons not in allowed_null_reasons:
                        raise PitError("PERP_RELATION")
                if (
                    not binance_public_only
                    and row["borrowable"] is None
                    and not any(code in row["missing_reasons"] for code in source_errors)
                ):
                    raise PitError("BORROWABLE_RELATION")
            for digest in row["source_raw_sha256s"]:
                if digest not in snapshot.get("available_raw_sha256s", []):
                    raise PitError("RAW_PROVENANCE")
    if all_venue_rows != 500:
        raise PitError("INVALID_VENUE_COUNT")
    raw_paths = snapshot.get("available_raw_relative_paths")
    raw_hashes = snapshot.get("available_raw_sha256s")
    if snapshot.get("available_raw_count") != len(raw_paths or []) or not isinstance(raw_hashes, list):
        raise PitError("RAW_PROVENANCE")
    if require_context and (claim is None or source_manifests is None or history is None or expected_i_impl is None):
        raise PitError("CONTEXT_REQUIRED")
    if history is not None and not isinstance(history, GitContext):
        raise PitError("HISTORY_PROVENANCE")
    if claim is not None:
        value = claim["value"]
        expected = {
            "I_impl": value["I_impl"], "formal_slot_utc": value["formal_slot_utc"],
            "idempotency_key": value["idempotency_key"], "attempt_id": value["attempt_id"],
            "image_digest": value["image_digest"],
            "workflow_run_id": value["workflow_run_id"], "job_id": value["job_id"],
            "log_locator": value["log_locator"], "claim_relative_path": claim["relative_path"],
            "claim_sha256": sha256(canonical_bytes(value)),
        }
        if any(snapshot.get(key) != val for key, val in expected.items()):
            raise PitError("CLAIM_PROVENANCE")
    if source_manifests is not None:
        hashes = sorted({sha256(canonical_bytes(item)) for item in source_manifests})
        if snapshot.get("source_manifest_sha256s") != hashes:
            raise PitError("SOURCE_PROVENANCE")
        pairs = [(item["raw_relative_path"], item["raw_sha256"]) for item in source_manifests if item.get("raw_sha256")]
        if snapshot.get("available_raw_relative_paths") != [x[0] for x in pairs] or snapshot.get("available_raw_sha256s") != sorted({x[1] for x in pairs}):
            raise PitError("RAW_PROVENANCE")
        source_hashes: dict[str, list[str]] = {}
        for item in source_manifests:
            if item.get("raw_sha256"):
                source_hashes.setdefault(item["source_id"], []).append(item["raw_sha256"])
        for asset in assets:
            for row in asset["venues"]:
                if row["mapping_status"] != "MAPPED":
                    expected_hashes: list[str] = []
                elif row["venue"] == "BINANCE_USDM":
                    required_sources = ["BN_FUT_EXCHANGE_INFO"]
                    if row["perp_exists"] is True:
                        required_sources += ["BN_FUT_PREMIUM_INDEX", "BN_FUT_BOOK_TICKER"]
                    expected_hashes = sorted({digest for source in required_sources for digest in source_hashes.get(source, [])})
                else:
                    required_sources = ["BY_LINEAR_INSTRUMENTS", "BY_SPOT_INSTRUMENTS", "BY_MARGIN_BORROWABLE"]
                    if row["perp_exists"] is True:
                        required_sources += ["BY_LINEAR_TICKERS"]
                    expected_hashes = sorted({digest for source in required_sources for digest in source_hashes.get(source, [])})
                has_source_error = any(reason in {
                    "SOURCE_FAILURE", "PARSE_FAILURE", "SCHEMA_FAILURE", "DERIVATION_FAILURE", "QA_FAILURE",
                } for reason in row["missing_reasons"])
                if (
                    row["source_raw_sha256s"] != expected_hashes
                    or row["mapping_status"] == "MAPPED" and not expected_hashes and not has_source_error
                ):
                    raise PitError("VENUE_RAW_PROVENANCE")
    if history is not None:
        if history.key != snapshot["idempotency_key"] or history.claim is None or history.claim_bytes != canonical_bytes(claim["value"]):
            raise PitError("HISTORY_PROVENANCE")
        if history.outcome_count not in ({1} if require_context else {0, 1}) or not history.claim_before_raw:
            raise PitError("HISTORY_PROVENANCE")
        if history.log is None or sha256(history.log) != snapshot["log_sha256"]:
            raise PitError("LOG_PROVENANCE")
        if {path: sha256(data) for path, data in history.raw.items()} != {
            path: digest for path, digest in zip(snapshot["available_raw_relative_paths"], [
                next(item["raw_sha256"] for item in source_manifests if item.get("raw_relative_path") == path)
                for path in snapshot["available_raw_relative_paths"]
            ])
        }:
            raise PitError("RAW_PROVENANCE")
        if sorted(sha256(canonical_bytes(item)) for item in history.source_manifests.values()) != sorted(
            sha256(canonical_bytes(item)) for item in source_manifests
        ):
            raise PitError("SOURCE_PROVENANCE")
    if source_manifests is not None or history is not None:
        if claim is None or source_manifests is None or history is None or expected_i_impl is None:
            raise PitError("CONTEXT_REQUIRED")
        raw_universe, sources = reconstruct_snapshot_sources(claim, source_manifests, history, expected_i_impl)
        try:
            rebuilt = build_snapshot(raw_universe, sources, claim)
        except PitError as exc:
            raise PitError("RAW_REDERIVATION_MISMATCH") from exc
        rebuilt["previous_ledger_head"] = snapshot["previous_ledger_head"]
        if canonical_bytes(rebuilt) != canonical_bytes(snapshot):
            raise PitError("RAW_REDERIVATION_MISMATCH")


def reconstruct_snapshot_sources(
    claim: dict[str, Any],
    source_manifests: list[dict[str, Any]],
    history: GitContext,
    expected_i_impl: str,
) -> tuple[bytes, dict[str, Any]]:
    if not SHA256_RE.fullmatch(expected_i_impl) or claim["value"].get("I_impl") != expected_i_impl:
        raise PitError("IMPLEMENTATION_MISMATCH")
    if any(
        not isinstance(item, dict)
        or item.get("source_id") not in SOURCE_ORDER
        or type(item.get("page_ordinal")) is not int
        for item in source_manifests
    ):
        raise PitError("SOURCE_PROVENANCE")
    ordered_manifests = sorted(
        source_manifests,
        key=lambda item: (SOURCE_ORDER.index(item["source_id"]), item["page_ordinal"]),
    )
    if source_manifests != ordered_manifests:
        raise PitError("SOURCE_PROVENANCE")
    groups = {source_id: [] for source_id in SOURCE_ORDER}
    for manifest in source_manifests:
        validate_normative(manifest)
        source_id, page = manifest.get("source_id"), manifest.get("page_ordinal")
        if source_id not in groups or type(page) is not int or page < 0:
            raise PitError("SOURCE_PROVENANCE")
        groups[source_id].append(manifest)
    if any(not groups[source_id] for source_id in SOURCE_ORDER):
        raise PitError("INCOMPLETE_SOURCE_SET")
    if any(len(groups[source_id]) != 1 for source_id in SOURCE_ORDER if source_id != "BY_LINEAR_INSTRUMENTS"):
        raise PitError("INCOMPLETE_SOURCE_SET")
    bybit_pages = groups["BY_LINEAR_INSTRUMENTS"]
    if [item["page_ordinal"] for item in bybit_pages] != list(range(len(bybit_pages))):
        raise PitError("INCOMPLETE_SOURCE_SET")
    expected_manifest_paths = {
        slot_artifact(
            claim["value"]["formal_slot_utc"], "source-manifests",
            item["source_id"] + "-" + str(item["page_ordinal"]) + ".json",
        ): item
        for item in source_manifests
    }
    if history.source_manifests != expected_manifest_paths:
        raise PitError("SOURCE_PROVENANCE")

    sources: dict[str, Any] = {}
    referenced_raw: set[str] = set()
    failure_seen = False
    for source_id in SOURCE_ORDER:
        parsed_pages = []
        expected_url = URLS[source_id]
        for page, manifest in enumerate(groups[source_id]):
            statuses = (
                manifest.get("source_status"),
                manifest.get("parse_status"),
                manifest.get("qa_status"),
            )
            failure_code = {
                ("SOURCE_FAILURE", "PARSE_NOT_RUN", "QA_NOT_RUN"): "SOURCE_FAILURE",
                ("SOURCE_OK", "PARSE_FAILURE", "QA_NOT_RUN"): "PARSE_FAILURE",
                ("SOURCE_OK", "SCHEMA_FAILURE", "QA_NOT_RUN"): "SCHEMA_FAILURE",
                ("SOURCE_OK", "PARSE_OK", "DERIVATION_FAILURE"): "DERIVATION_FAILURE",
                ("SOURCE_OK", "PARSE_OK", "QA_FAILURE"): "QA_FAILURE",
            }.get(statuses)
            not_run = statuses == ("SOURCE_NOT_RUN", "PARSE_NOT_RUN", "QA_NOT_RUN")
            source_ok = statuses == ("SOURCE_OK", "PARSE_OK", "QA_OK")
            if (
                manifest.get("I_impl") != expected_i_impl
                or manifest.get("page_ordinal") != page
                or manifest.get("canonical_url_without_secret") != expected_url
                or not (source_ok or failure_code or not_run)
            ):
                raise PitError("SOURCE_PROVENANCE")
            if not_run:
                if (
                    not failure_seen
                    or len(groups[source_id]) != 1
                    or page != 0
                    or any(manifest.get(field) is not None for field in (
                        "error_record_relative_path", "error_record_sha256", "fetched_at_utc",
                        "http_status", "raw_bytes", "raw_relative_path", "raw_sha256",
                        "requested_at_utc", "server_time_utc",
                    ))
                    or manifest.get("attempt_count") != 0
                ):
                    raise PitError("SOURCE_PROVENANCE")
                continue
            if failure_seen:
                raise PitError("SOURCE_PROVENANCE")
            if source_ok:
                if manifest.get("error_record_relative_path") is not None or manifest.get("error_record_sha256") is not None:
                    raise PitError("SOURCE_PROVENANCE")
            else:
                error_path = manifest.get("error_record_relative_path")
                if (
                    not isinstance(error_path, str)
                    or artifact_path(error_path) != error_path
                    or not SHA256_RE.fullmatch(manifest.get("error_record_sha256", ""))
                ):
                    raise PitError("SOURCE_PROVENANCE")
                failure_seen = True
            raw_path = manifest.get("raw_relative_path")
            raw = history.raw.get(raw_path) if isinstance(raw_path, str) else None
            if raw is None:
                if failure_code != "SOURCE_FAILURE" or any(manifest.get(field) is not None for field in (
                    "raw_relative_path", "raw_bytes", "raw_sha256", "fetched_at_utc",
                )):
                    raise PitError("RAW_PROVENANCE")
                continue
            if (
                raw_path != slot_artifact(claim["value"]["formal_slot_utc"], "raw", source_id + "-" + str(page) + ".bin")
                or manifest.get("raw_bytes") != len(raw)
                or manifest.get("raw_sha256") != sha256(raw)
            ):
                raise PitError("RAW_PROVENANCE")
            referenced_raw.add(raw_path)
            if failure_code is not None:
                continue
            try:
                body = json.loads(raw, parse_constant=_reject_constant)
            except (json.JSONDecodeError, TypeError, UnicodeDecodeError, PitError) as exc:
                raise PitError("RAW_REPARSE_FAILURE") from exc
            if source_id == "CG_TOP250":
                rows, gap = parse_universe(raw)
                if rows is None or gap is not None:
                    raise PitError("RAW_REPARSE_FAILURE")
                sources[source_id] = body
            else:
                validate_source_schema(source_id, body)
                parsed_pages.append(body)
            if source_id == "BY_LINEAR_INSTRUMENTS":
                cursor = body["result"]["nextPageCursor"]
                if page + 1 < len(groups[source_id]):
                    if not cursor:
                        raise PitError("INCOMPLETE_SOURCE_SET")
                    expected_url = URLS[source_id] + "&cursor=" + urllib.parse.quote(cursor, safe="")
                elif cursor and not failure_seen:
                    raise PitError("INCOMPLETE_SOURCE_SET")
        if source_id != "CG_TOP250" and parsed_pages:
            sources[source_id] = parsed_pages if source_id == "BY_LINEAR_INSTRUMENTS" else parsed_pages[0]
    if groups["CG_TOP250"][0].get("source_status") != "SOURCE_OK":
        raise PitError("SOURCE_PROVENANCE")
    if referenced_raw != set(history.raw):
        raise PitError("RAW_PROVENANCE")
    sources["__source_manifests__"] = source_manifests
    cg_path = groups["CG_TOP250"][0]["raw_relative_path"]
    return history.raw[cg_path], sources


def validate_source_url(source_id: str, url: str) -> None:
    expected = URLS.get(source_id)
    if source_id == "BY_LINEAR_INSTRUMENTS" and url.startswith(expected + "&cursor="):
        cursor = url[len(expected + "&cursor=") :]
        if not cursor or urllib.parse.quote(urllib.parse.unquote(cursor), safe="") != cursor:
            raise PitError("ENDPOINT_NOT_ALLOWED")
        return
    if url != expected:
        raise PitError("ENDPOINT_NOT_ALLOWED")


def retry_request(
    send: Callable[[], tuple[int, bytes, dict[str, str]]],
    sleep: Callable[[float], None] = time.sleep,
    before_attempt: Callable[[], None] | None = None,
) -> tuple[int, bytes, int]:
    for attempt in range(1, 4):
        if before_attempt:
            before_attempt()
        try:
            status, body, headers = send()
        except OSError:
            status, body, headers = 0, b"", {}
        if 200 <= status <= 299:
            return status, body, attempt
        retryable = status == 0 or status == 429 or 500 <= status <= 599
        if not retryable or attempt == 3:
            raise PitError("SOURCE_FAILURE")
        delay = (1, 2)[attempt - 1]
        if "Retry-After" in headers:
            try:
                delay = max(0, min(60, int(headers["Retry-After"])))
            except ValueError:
                pass
        sleep(delay)
    raise AssertionError("unreachable")


def paginate_bybit(fetch: Callable[[str], bytes]) -> list[bytes]:
    url, pages, seen = URLS["BY_LINEAR_INSTRUMENTS"], [], set()
    while True:
        validate_source_url("BY_LINEAR_INSTRUMENTS", url)
        raw = fetch(url)
        pages.append(raw)
        try:
            body = json.loads(raw)
            cursor = body["result"].get("nextPageCursor", "")
        except (json.JSONDecodeError, KeyError, AttributeError, TypeError):
            raise PitError("SCHEMA_FAILURE")
        if not cursor:
            return pages
        if not isinstance(cursor, str) or cursor in seen:
            raise PitError("SCHEMA_FAILURE")
        seen.add(cursor)
        url = URLS["BY_LINEAR_INSTRUMENTS"] + "&cursor=" + urllib.parse.quote(cursor, safe="")


def validate_source_schema(source_id: str, body: Any) -> None:
    if source_id == "CG_TOP250":
        raise PitError("INTERNAL_RAW_SCHEMA_REQUIRED")
    if source_id == "BN_FUT_EXCHANGE_INFO":
        rows = _rows(body, "symbols")
    elif source_id in {"BN_FUT_PREMIUM_INDEX", "BN_FUT_BOOK_TICKER"}:
        rows = _rows(body)
    elif source_id in {"BY_LINEAR_INSTRUMENTS", "BY_SPOT_INSTRUMENTS"}:
        rows = _rows(body, "result", "list")
        if source_id == "BY_LINEAR_INSTRUMENTS":
            cursor = body["result"].get("nextPageCursor", "")
            if not isinstance(cursor, str):
                raise PitError("SCHEMA_FAILURE")
    elif source_id == "BY_MARGIN_BORROWABLE":
        rows = _bybit_borrowable_rows(body)
    elif source_id == "BY_LINEAR_TICKERS":
        rows = _rows(body, "result", "list")
        utc_from_ms(body.get("time"))
    else:
        raise PitError("UNKNOWN_SOURCE")
    if source_id == "BY_SPOT_INSTRUMENTS" and any(row.get("marginTrading") not in MARGIN_TRADING for row in rows):
        raise PitError("SCHEMA_FAILURE")
    if source_id == "BY_MARGIN_BORROWABLE" and any(type(row.get("borrowable")) is not bool for row in rows):
        raise PitError("SCHEMA_FAILURE")
    required: dict[str, dict[str, type]] = {
        "BN_FUT_EXCHANGE_INFO": {"symbol": str, "baseAsset": str, "quoteAsset": str, "contractType": str, "status": str},
        "BN_FUT_PREMIUM_INDEX": {"symbol": str, "lastFundingRate": str, "markPrice": str, "indexPrice": str, "time": int},
        "BN_FUT_BOOK_TICKER": {"symbol": str, "bidPrice": str, "askPrice": str},
        "BY_LINEAR_INSTRUMENTS": {"symbol": str, "baseCoin": str, "quoteCoin": str, "settleCoin": str, "contractType": str, "status": str},
        "BY_LINEAR_TICKERS": {"symbol": str, "fundingRate": str, "bid1Price": str, "ask1Price": str, "markPrice": str, "indexPrice": str},
        "BY_SPOT_INSTRUMENTS": {"baseCoin": str, "quoteCoin": str, "marginTrading": str},
        "BY_MARGIN_BORROWABLE": {"currency": str, "borrowable": bool},
    }
    for row in rows:
        if any(type(row.get(field)) is not expected for field, expected in required[source_id].items()):
            raise PitError("SCHEMA_FAILURE")
    if source_id == "BN_FUT_PREMIUM_INDEX":
        for row in rows:
            utc_from_ms(row["time"])
            parse_decimal_string(row["lastFundingRate"])
            parse_decimal_string(row["markPrice"], positive=True)
            parse_decimal_string(row["indexPrice"], positive=True)
    elif source_id == "BN_FUT_BOOK_TICKER":
        for row in rows:
            bid = parse_decimal_string(row["bidPrice"])
            ask = parse_decimal_string(row["askPrice"])
            if (bid != 0 or ask != 0) and (bid <= 0 or ask <= 0 or bid > ask):
                raise PitError("SCHEMA_FAILURE")
    elif source_id == "BY_LINEAR_TICKERS":
        for row in rows:
            if row["fundingRate"]:
                parse_decimal_string(row["fundingRate"])
            bid = parse_decimal_string(row["bid1Price"], positive=True)
            ask = parse_decimal_string(row["ask1Price"], positive=True)
            parse_decimal_string(row["markPrice"], positive=True)
            parse_decimal_string(row["indexPrice"], positive=True)
            if bid > ask:
                raise PitError("SCHEMA_FAILURE")


def materialization_deadline(slot: str) -> datetime:
    validate_utc(slot)
    return datetime.strptime(slot, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc) + timedelta(minutes=15)


def acquisition_window(slot: str, now: datetime) -> None:
    start = datetime.strptime(slot, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    if now.tzinfo is None or not start <= now.astimezone(timezone.utc) < start + timedelta(minutes=10):
        raise PitError("ACQUISITION_WINDOW_CLOSED")


def add_slots(slot: str, count: int = 1) -> str:
    validate_utc(slot)
    value = datetime.strptime(slot, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc) + timedelta(minutes=30 * count)
    return value.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def artifact_path(relative: str) -> str:
    if (
        not isinstance(relative, str)
        or "\\" in relative
        or relative.startswith("/")
        or ".." in relative.split("/")
        or not relative.startswith(PREFIX)
        or "collector_v4" in relative.lower()
    ):
        raise PitError("PATH_NOT_ALLOWED")
    return relative


def ensure_impl_boundary(pinned: str, current: str, epoch_id: str, h0: str) -> None:
    if pinned != current:
        raise PitError("NEW_EPOCH_AND_H0_REQUIRED")
    if epoch_id != EPOCH_ID or not h0:
        raise PitError("INVALID_EPOCH_BOUNDARY")


def claim_filesafe(slot: str) -> str:
    validate_utc(slot)
    return slot.replace("-", "").replace(":", "").replace(".", "")


def slot_root(slot: str) -> str:
    return artifact_path(PREFIX + "slots/" + claim_filesafe(slot) + "/")


def slot_artifact(slot: str, kind: str, name: str) -> str:
    if kind not in {"raw", "source-manifests", "errors"}:
        raise PitError("PATH_NOT_ALLOWED")
    return artifact_path(slot_root(slot) + kind + "/" + name)


def make_claim(
    slot: str,
    i_impl: str,
    writer: str,
    parent: str,
    claimed_at: str | None = None,
    workflow_run_id: str = "fixture-run",
    job_id: str = "fixture-job",
    log_locator: str | None = None,
    image_digest: str = "sha256:" + "0" * 64,
) -> dict[str, Any]:
    validate_utc(slot)
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", image_digest):
        raise PitError("IMAGE_PROVENANCE")
    key = EPOCH_ID + "|" + slot
    filesafe = claim_filesafe(slot)
    return {
        "relative_path": artifact_path(PREFIX + "claims/" + filesafe + ".json"),
        "value": {
            "I_impl": i_impl,
            "attempt_id": sha256(key.encode())[:32],
            "attempt_ordinal": 1,
            "authorized_writer_identity": writer,
            "claim_status": "CLAIMED",
            "claimed_at_utc": claimed_at or slot,
            "contract_id": CONTRACT_ID,
            "epoch_id": EPOCH_ID,
            "expected_parent_before_claim": parent,
            "formal_slot_utc": slot,
            "idempotency_key": key,
            "image_digest": image_digest,
            "job_id": job_id,
            "log_locator": log_locator or artifact_path(slot_root(slot) + "run.log"),
            "workflow_run_id": workflow_run_id,
        },
    }


def run_log_bytes(claim: dict[str, Any]) -> bytes:
    value = claim["value"]
    return (
        "PIT_LEDGER_PUBLIC_ONLY_V8\n"
        f"idempotency_key={value['idempotency_key']}\n"
        f"attempt_id={value['attempt_id']}\n"
        f"workflow_run_id={value['workflow_run_id']}\n"
        f"job_id={value['job_id']}\n"
        f"image_digest={value['image_digest']}\n"
    ).encode("utf-8")


def base_outcome(
    claim: dict[str, Any],
    kind: str,
    reason_codes: Iterable[str],
    raw: list[tuple[str, bytes]],
    previous_head: str | None = None,
    source_manifest_sha256s: Iterable[str] = (),
    log_bytes: bytes | None = None,
) -> dict[str, Any]:
    value = claim["value"]
    raw_paths = [artifact_path(path) for path, _ in raw]
    raw_hashes = sorted({sha256(body) for _, body in raw})
    return {
        "I_impl": value["I_impl"],
        "attempt_id": value["attempt_id"],
        "available_raw_count": len(raw),
        "available_raw_relative_paths": raw_paths,
        "available_raw_sha256s": raw_hashes,
        "claim_relative_path": claim["relative_path"],
        "claim_sha256": sha256(canonical_bytes(value)),
        "contract_id": CONTRACT_ID,
        "epoch_id": EPOCH_ID,
        "formal_slot_utc": value["formal_slot_utc"],
        "idempotency_key": value["idempotency_key"],
        "image_digest": value["image_digest"],
        "job_id": value["job_id"],
        "log_locator": value["log_locator"],
        "log_sha256": sha256(log_bytes if log_bytes is not None else run_log_bytes(claim)),
        "materialized_at_utc": value["formal_slot_utc"],
        "outcome_kind": kind,
        "previous_ledger_head": previous_head or value["expected_parent_before_claim"],
        "reason_codes": ordered(reason_codes, REASON_CODE_ORDER),
        "slot_status": OUTCOME_STATUS[kind],
        "source_manifest_sha256s": sorted(set(source_manifest_sha256s)),
        "workflow_run_id": value["workflow_run_id"],
    }


@dataclass
class Ledger:
    authorized_writer: str
    head: str = "H0"
    claims: dict[str, dict[str, Any]] = field(default_factory=dict)
    outcomes: dict[str, dict[str, Any]] = field(default_factory=dict)
    raw: dict[str, list[tuple[str, bytes]]] = field(default_factory=dict)
    manifests: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    error_records: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    history_claim_keys: set[str] = field(default_factory=set)
    published_bytes: dict[str, bytes] = field(default_factory=dict)
    events: list[str] = field(default_factory=list)
    writer_stop: bool = False
    writes_after_stop: int = 0

    def _write(self, marker: str) -> None:
        if self.writer_stop:
            self.writes_after_stop += 1
            raise PitError("EPOCH_WRITER_STOP")
        self.events.append(marker)
        self.head = sha256((self.head + "|" + marker).encode())

    def claim(self, claim: dict[str, Any], writer: str, expected_parent: str) -> str:
        key = claim["value"]["idempotency_key"]
        if writer != self.authorized_writer:
            self.writer_stop = True
            return "EPOCH_WRITER_STOP"
        if self.writer_stop or expected_parent != self.head:
            return self.cas_reject(key, writer)
        if key in self.outcomes:
            return "DUPLICATE_NO_WRITE"
        if key in self.claims:
            return "CLAIM_HELD_NO_WRITE"
        if claim["value"].get("authorized_writer_identity") != writer:
            self.writer_stop = True
            return "EPOCH_WRITER_STOP"
        if claim["value"].get("expected_parent_before_claim") != expected_parent:
            self.writer_stop = True
            return "EPOCH_WRITER_STOP"
        if key != EPOCH_ID + "|" + claim["value"].get("formal_slot_utc", ""):
            self.writer_stop = True
            return "EPOCH_WRITER_STOP"
        validate_normative(claim["value"])
        data = canonical_bytes(claim["value"])
        self.claims[key] = claim
        self.history_claim_keys.add(key)
        self.published_bytes[claim["relative_path"]] = data
        self._write("claim:" + key)
        return "CLAIMED"

    def archive_raw(self, key: str, source_id: str, body: bytes) -> str:
        if key not in self.claims:
            raise PitError("CLAIM_REQUIRED_BEFORE_REQUEST")
        page = sum(path.rsplit("/", 1)[-1].startswith(source_id + "-") for path, _ in self.raw.get(key, []))
        path = slot_artifact(self.claims[key]["value"]["formal_slot_utc"], "raw", source_id + "-" + str(page) + ".bin")
        self.raw.setdefault(key, []).append((path, body))
        self._write("raw:" + key + ":" + source_id)
        return path

    def parse_after_raw(self, key: str, parser: Callable[[bytes], Any]) -> Any:
        if key not in self.raw or not self.raw[key]:
            raise PitError("RAW_REQUIRED_BEFORE_PARSE")
        self.events.append("parse:" + key)
        return parser(self.raw[key][-1][1])

    def publish_manifest(self, key: str, manifest: dict[str, Any]) -> None:
        if manifest.get("raw_sha256") is not None and key not in self.raw:
            raise PitError("RAW_REQUIRED_BEFORE_MANIFEST")
        validate_normative(manifest)
        self.manifests.setdefault(key, []).append(manifest)
        self._write("manifest:" + key + ":" + manifest["source_id"])

    def publish_error(self, key: str, record: dict[str, Any]) -> tuple[str, str]:
        validate_normative(record)
        data = canonical_bytes(record)
        claim = self.claims[key]
        path = slot_artifact(claim["value"]["formal_slot_utc"], "errors", record["source_id"] + "-" + str(record["page_ordinal"]) + ".json")
        self.error_records.setdefault(key, []).append(record)
        self.published_bytes[path] = data
        self._write("error:" + key + ":" + record["source_id"])
        return path, sha256(data)

    def publish_outcome(self, key: str, outcome: dict[str, Any], writer: str, expected_parent: str) -> str:
        if self.writer_stop or writer != self.authorized_writer:
            self.writer_stop = True
            return "EPOCH_WRITER_STOP"
        if expected_parent != self.head:
            return self.cas_reject(key, writer)
        if key in self.outcomes:
            return "DUPLICATE_NO_WRITE"
        if outcome.get("idempotency_key") != key or outcome.get("formal_slot_utc") != key.split("|", 1)[1]:
            self.writer_stop = True
            return "EPOCH_WRITER_STOP"
        claim = self.claims.get(key)
        if claim:
            if (
                outcome.get("claim_relative_path") != claim["relative_path"]
                or outcome.get("claim_sha256") != sha256(canonical_bytes(claim["value"]))
                or outcome.get("attempt_id") != claim["value"]["attempt_id"]
                or outcome.get("workflow_run_id") != claim["value"]["workflow_run_id"]
                or outcome.get("job_id") != claim["value"]["job_id"]
                or outcome.get("log_locator") != claim["value"]["log_locator"]
            ):
                self.writer_stop = True
                return "EPOCH_WRITER_STOP"
        if outcome.get("previous_ledger_head") != expected_parent:
            self.writer_stop = True
            return "EPOCH_WRITER_STOP"
        validate_normative(outcome)
        self.outcomes[key] = outcome
        self.published_bytes[artifact_path(PREFIX + "outcomes/" + claim_filesafe(outcome["formal_slot_utc"]) + ".json")] = canonical_bytes(outcome)
        self._write("outcome:" + key)
        return "PUBLISHED_SLOT_OUTCOME"

    def cas_reject(
        self,
        key: str,
        observed_writer: str,
        observed_key: str | None = None,
        observed_parent: str | None = None,
        observed_bytes: bytes | None = None,
        observed_sha256: str | None = None,
        observed_delta: Iterable[str] | None = None,
    ) -> str:
        if observed_writer != self.authorized_writer or observed_key not in {None, key}:
            self.writer_stop = True
            return "EPOCH_WRITER_STOP"
        if key in self.outcomes:
            outcome = self.outcomes[key]
            expected = canonical_bytes(outcome)
            path = artifact_path(PREFIX + "outcomes/" + claim_filesafe(outcome["formal_slot_utc"]) + ".json")
            if observed_bytes is not None and observed_bytes != expected:
                self.writer_stop = True
            if observed_sha256 is not None and observed_sha256 != sha256(expected):
                self.writer_stop = True
            if observed_parent is not None and observed_parent != outcome["previous_ledger_head"]:
                self.writer_stop = True
            if observed_delta is not None and list(observed_delta) != [path]:
                self.writer_stop = True
            validate_canonical(expected)
            if self.writer_stop:
                return "EPOCH_WRITER_STOP"
            return "DUPLICATE_NO_WRITE"
        if key in self.claims:
            return "CLAIM_HELD_NO_WRITE"
        self.writer_stop = True
        return "EPOCH_WRITER_STOP"

    def observe_history(self, writer: str, parents: int, changed_paths: Iterable[str], force: bool = False) -> str:
        try:
            for path in changed_paths:
                artifact_path(path)
        except PitError:
            self.writer_stop = True
        if writer != self.authorized_writer or parents != 1 or force:
            self.writer_stop = True
        return "EPOCH_WRITER_STOP" if self.writer_stop else "OK"

    def recover(
        self,
        claim: dict[str, Any],
        writer: str,
        expected_parent: str,
        now: datetime,
        history_complete: bool = True,
    ) -> str:
        key = claim["value"]["idempotency_key"]
        if self.writer_stop:
            return "EPOCH_WRITER_STOP"
        if not history_complete or now.astimezone(timezone.utc) < materialization_deadline(claim["value"]["formal_slot_utc"]):
            return "EPOCH_WRITER_STOP"
        if key in self.outcomes:
            return "DUPLICATE_NO_WRITE"
        if expected_parent != self.head:
            return self.cas_reject(key, writer)
        if key in self.claims:
            raw = self.raw.get(key, [])
            reasons = ["ATTEMPT_ABORTED"] + ([] if raw else ["NO_RAW_DURABLY_PUBLISHED"])
            outcome = base_outcome(self.claims[key], "ABORTED_ATTEMPT", reasons, raw, self.head)
        else:
            if key in self.history_claim_keys:
                self.writer_stop = True
                return "EPOCH_WRITER_STOP"
            outcome = base_outcome(claim, "GAP_NO_RUN", ["NO_CLAIM_BEFORE_DEADLINE"], [], self.head)
        return self.publish_outcome(key, outcome, writer, self.head)


def fixture_vertical_slice(raw: bytes, parser: Callable[[bytes], Any]) -> tuple[Ledger, dict[str, Any]]:
    writer, slot = "fixture-writer", "2026-07-26T20:00:00.000Z"
    ledger = Ledger(writer)
    claim = make_claim(slot, "A" * 64, writer, ledger.head)
    if ledger.claim(claim, writer, ledger.head) != "CLAIMED":
        raise PitError("CLAIM_NOT_PUBLISHED_NO_REQUEST")
    key = claim["value"]["idempotency_key"]
    ledger.events.append("request:" + key)
    ledger.archive_raw(key, "CG_TOP250", raw)
    parsed = ledger.parse_after_raw(key, parser)
    outcome = base_outcome(claim, "SNAPSHOT_COMPLETE", [], ledger.raw[key], ledger.head)
    if ledger.publish_outcome(key, outcome, writer, ledger.head) != "PUBLISHED_SLOT_OUTCOME":
        raise PitError("OUTCOME_NOT_PUBLISHED")
    return ledger, {"parsed": parsed, "outcome": outcome}


def acquire_fixture_sources(
    ledger: Ledger,
    claim: dict[str, Any],
    fetch: Callable[[str, str], tuple[int, bytes, dict[str, str]]],
    clock: Callable[[], datetime] | None = None,
    durable_publish: Callable[[str, bytes, bool], None] | None = None,
    sleep: Callable[[float], None] = lambda _: None,
    publish_failure: bool = True,
) -> dict[str, Any]:
    """Frozen producer path; fixture/live transports differ only at ``fetch``."""
    key = claim["value"]["idempotency_key"]
    if key not in ledger.claims:
        raise PitError("CLAIM_REQUIRED_BEFORE_REQUEST")
    slot = claim["value"]["formal_slot_utc"]
    fixed = datetime.strptime(slot, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc) + timedelta(minutes=1)
    clock = clock or (lambda: fixed)
    parsed: dict[str, Any] = {}
    manifest_hashes: list[str] = []
    failure: str | None = None
    page_cursors: set[str] = set()

    def not_run(source_id: str) -> None:
        manifest = {
            "I_impl": claim["value"]["I_impl"], "attempt_count": 0,
            "auth_class": "PUBLIC",
            "canonical_url_without_secret": URLS[source_id], "error_record_relative_path": None,
            "error_record_sha256": None, "fetched_at_utc": None, "http_status": None,
            "method": "GET", "page_ordinal": 0, "parse_status": "PARSE_NOT_RUN",
            "qa_status": "QA_NOT_RUN", "raw_bytes": None, "raw_relative_path": None,
            "raw_sha256": None, "requested_at_utc": None, "server_time_utc": None,
            "source_id": source_id, "source_status": "SOURCE_NOT_RUN",
        }
        ledger.manifests.setdefault(key, []).append(manifest)
        if durable_publish:
            relative = slot_artifact(slot, "source-manifests", source_id + "-0.json")
            durable_publish(relative, canonical_bytes(manifest), False)
        manifest_hashes.append(sha256(canonical_bytes(manifest)))

    for source_id in SOURCE_ORDER:
        if failure is not None:
            not_run(source_id)
            continue
        pages: list[Any] = []

        def capture_page(url: str) -> bytes:
            nonlocal failure
            page_ordinal = len(pages)
            try:
                acquisition_window(slot, clock())
                validate_source_url(source_id, url)
                ledger.events.append("request:" + key + ":" + source_id)
                status, raw, attempts = retry_request(
                    lambda: fetch(source_id, url), sleep, lambda: acquisition_window(slot, clock())
                )
            except (PitError, Exception):
                status, raw, attempts = None, None, 3
                body, code = None, "SOURCE_FAILURE"
            else:
                ledger.archive_raw(key, source_id, raw)
                if durable_publish:
                    durable_publish(ledger.raw[key][-1][0], raw, True)
                try:
                    acquisition_window(slot, clock())
                except PitError:
                    body, code = None, "SOURCE_FAILURE"
                else:
                    try:
                        body = ledger.parse_after_raw(key, lambda data: json.loads(data, parse_constant=_reject_constant))
                    except (json.JSONDecodeError, TypeError, UnicodeDecodeError):
                        body, code = None, "PARSE_FAILURE"
                    else:
                        try:
                            if source_id == "CG_TOP250":
                                _, gap = parse_universe(raw)
                                if gap is not None:
                                    raise PitError(gap["reason_codes"][0])
                            else:
                                validate_source_schema(source_id, body)
                                if source_id == "BY_LINEAR_INSTRUMENTS":
                                    cursor = body["result"].get("nextPageCursor", "")
                                    if cursor and cursor in page_cursors:
                                        raise PitError("SCHEMA_FAILURE")
                                    if cursor:
                                        page_cursors.add(cursor)
                        except PitError as exc:
                            code = str(exc) if str(exc) in {"SCHEMA_FAILURE", "DERIVATION_FAILURE", "QA_FAILURE"} else "SCHEMA_FAILURE"
                        except Exception:
                            code = "QA_FAILURE"
                        else:
                            code = None
            error_path = error_hash = None
            if code:
                record = {
                    "error_class": code, "log_locator": claim["value"]["log_locator"],
                    "log_sha256": sha256(run_log_bytes(claim)), "page_ordinal": page_ordinal,
                    "raw_relative_path": ledger.raw.get(key, [(None, b"")])[-1][0] if raw is not None else None,
                    "raw_sha256": sha256(raw) if raw is not None else None, "source_id": source_id,
                }
                error_path, error_hash = ledger.publish_error(key, record)
                if durable_publish:
                    durable_publish(error_path, canonical_bytes(record), False)
                failure = code
            raw_path = ledger.raw[key][-1][0] if raw is not None else None
            status_fields = {
                None: ("SOURCE_OK", "PARSE_OK", "QA_OK"),
                "SOURCE_FAILURE": ("SOURCE_FAILURE", "PARSE_NOT_RUN", "QA_NOT_RUN"),
                "PARSE_FAILURE": ("SOURCE_OK", "PARSE_FAILURE", "QA_NOT_RUN"),
                "SCHEMA_FAILURE": ("SOURCE_OK", "SCHEMA_FAILURE", "QA_NOT_RUN"),
                "DERIVATION_FAILURE": ("SOURCE_OK", "PARSE_OK", "DERIVATION_FAILURE"),
                "QA_FAILURE": ("SOURCE_OK", "PARSE_OK", "QA_FAILURE"),
            }[code]
            manifest = {
                "I_impl": claim["value"]["I_impl"], "attempt_count": attempts,
                "auth_class": "PUBLIC",
                "canonical_url_without_secret": url, "error_record_relative_path": error_path,
                "error_record_sha256": error_hash, "fetched_at_utc": claim["value"]["claimed_at_utc"] if raw is not None else None,
                "http_status": status, "method": "GET", "page_ordinal": page_ordinal,
                "parse_status": status_fields[1],
                "qa_status": status_fields[2],
                "raw_bytes": len(raw) if raw is not None else None, "raw_relative_path": raw_path,
                "raw_sha256": sha256(raw) if raw is not None else None,
                "requested_at_utc": claim["value"]["claimed_at_utc"], "server_time_utc": None,
                "source_id": source_id, "source_status": status_fields[0],
            }
            ledger.publish_manifest(key, manifest)
            if durable_publish:
                relative = slot_artifact(slot, "source-manifests", source_id + "-" + str(page_ordinal) + ".json")
                durable_publish(relative, canonical_bytes(manifest), False)
            manifest_hashes.append(sha256(canonical_bytes(manifest)))
            pages.append(body)
            if code:
                raise PitError(code)
            return raw

        try:
            if source_id == "BY_LINEAR_INSTRUMENTS":
                paginate_bybit(capture_page)
                parsed[source_id] = pages
            else:
                capture_page(URLS[source_id])
                parsed[source_id] = pages[0]
        except (PitError, Exception) as exc:
            if failure is None:
                failure = str(exc) if isinstance(exc, PitError) and str(exc) in {"SOURCE_FAILURE", "PARSE_FAILURE", "SCHEMA_FAILURE", "DERIVATION_FAILURE", "QA_FAILURE"} else "QA_FAILURE"
                if key not in ledger.manifests or not ledger.manifests[key]:
                    not_run(source_id)
                last = ledger.manifests[key][-1]
                record = {
                    "error_class": failure, "log_locator": claim["value"]["log_locator"],
                    "log_sha256": sha256(run_log_bytes(claim)), "page_ordinal": last["page_ordinal"],
                    "raw_relative_path": last["raw_relative_path"], "raw_sha256": last["raw_sha256"],
                    "source_id": source_id,
                }
                error_path, error_hash = ledger.publish_error(key, record)
                if durable_publish:
                    durable_publish(error_path, canonical_bytes(record), False)
                last.update(error_record_relative_path=error_path, error_record_sha256=error_hash, parse_status="SCHEMA_FAILURE", qa_status="QA_NOT_RUN")
                manifest_hashes[-1] = sha256(canonical_bytes(last))
            continue
    parsed["__source_manifests__"] = list(ledger.manifests[key])
    parsed["__error_records__"] = list(ledger.error_records.get(key, []))
    if failure:
        if "CG_TOP250" in parsed:
            universe_raw = next(
                raw for path, raw in ledger.raw.get(key, [])
                if path.endswith("/CG_TOP250-0.bin")
            )
            outcome = build_snapshot(universe_raw, parsed, claim)
        else:
            outcome = base_outcome(
                claim, "GAP_UNIVERSE", [failure],
                ledger.raw.get(key, []), ledger.head, manifest_hashes,
            )
        parsed["__outcome__"] = outcome
        if publish_failure and key not in ledger.outcomes:
            ledger.publish_outcome(key, outcome, ledger.authorized_writer, ledger.head)
    return parsed


def implementation_manifest(root: os.PathLike[str] | str = ".") -> bytes:
    base = os.fspath(root)
    files = []
    for relative in IMPLEMENTATION_FILES:
        path = os.path.join(base, *relative.split("/"))
        with open(path, "rb") as handle:
            data = handle.read()
        files.append({"bytes": len(data), "path": relative, "sha256": sha256(data)})
    return canonical_bytes({"files": files})


def current_i_impl(root: os.PathLike[str] | str = ".") -> str:
    return sha256(implementation_manifest(root))


def require_live_authorization(env: dict[str, str], root: os.PathLike[str] | str = ".") -> None:
    required = {
        "PIT_ACTIVATION_APPROVED": "YES",
        "PIT_TARGET_WRITE_APPROVED": "YES",
        "PIT_AUTHORIZED_WRITER": AUTHORIZED_WRITER,
        "PIT_GITHUB_REPO": GITHUB_REPO,
        "PIT_GITHUB_BRANCH": GITHUB_BRANCH,
        "PIT_I_IMPL": "",
        "PIT_H0": "",
        "PIT_ACTIVATION_CANDIDATE_SLOT": "",
        "PIT_IMAGE_DIGEST": "",
        "PIT_DEPLOY_KEY_FINGERPRINT": "",
        "CLOUD_RUN_EXECUTION": "",
        "CLOUD_RUN_JOB": "",
        "CLOUD_RUN_TASK_INDEX": "0",
        "CLOUD_RUN_TASK_ATTEMPT": "0",
    }
    for name, exact in required.items():
        value = env.get(name, "")
        if not value or exact and value != exact:
            raise PitError("STOP_PERMISSION_REQUIRED")
    if (
        not SHA256_RE.fullmatch(env["PIT_I_IMPL"])
        or not re.fullmatch(r"[0-9a-f]{40}", env["PIT_H0"])
        or not re.fullmatch(r"sha256:[0-9a-f]{64}", env["PIT_IMAGE_DIGEST"])
        or not re.fullmatch(r"SHA256:[A-Za-z0-9+/]{43}", env["PIT_DEPLOY_KEY_FINGERPRINT"])
    ):
        raise PitError("STOP_PERMISSION_REQUIRED")
    validate_utc(env["PIT_ACTIVATION_CANDIDATE_SLOT"])
    ensure_impl_boundary(env["PIT_I_IMPL"], current_i_impl(root), EPOCH_ID, env["PIT_H0"])


@dataclass(frozen=True)
class GitContext:
    h0: str
    head: str
    key: str
    claim: dict[str, Any] | None
    claim_bytes: bytes | None
    raw: dict[str, bytes]
    source_manifests: dict[str, dict[str, Any]]
    log: bytes | None
    outcome_count: int
    claim_before_raw: bool


class GitWriter:
    """Exact-parent, no-force writer for the isolated PIT branch."""

    def __init__(self, repo: os.PathLike[str] | str, writer: str, branch: str = GITHUB_BRANCH):
        self.repo, self.writer, self.branch = os.fspath(repo), writer, branch

    def git(self, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        if any(arg in {"--force", "-f", "--force-with-lease"} for arg in args):
            raise PitError("FORCE_FORBIDDEN")
        result = subprocess.run(["git", *args], cwd=self.repo, capture_output=True, text=True, check=False)
        if check and result.returncode:
            raise PitError("GIT_FAILURE")
        return result

    def remote_head(self) -> str:
        result = self.git("ls-remote", "--heads", "origin", f"refs/heads/{self.branch}")
        lines = result.stdout.strip().splitlines()
        if len(lines) != 1 or not re.fullmatch(r"[0-9a-f]{40}", lines[0].split()[0]):
            raise PitError("REMOTE_HEAD_UNPROVEN")
        return lines[0].split()[0]

    def read_at(self, head: str, relative: str) -> bytes | None:
        artifact_path(relative)
        result = subprocess.run(["git", "show", f"{head}:{relative}"], cwd=self.repo, capture_output=True, check=False)
        return result.stdout if result.returncode == 0 else None

    def verify_history(self, h0: str, head: str) -> None:
        self.git("fetch", "--quiet", "--filter=blob:none", "origin", f"refs/heads/{self.branch}")
        if self.git("merge-base", "--is-ancestor", h0, head, check=False).returncode:
            raise PitError("EPOCH_WRITER_STOP")
        raw = self.git(
            "log", "--reverse", "--format=@@%H%x00%P%x00%an%x00%ae",
            "--raw", "--no-abbrev", "--no-renames", f"{h0}..{head}",
        ).stdout
        changes: list[str] | None = None
        index_changes: list[tuple[str, str, str]] = []
        for line in raw.splitlines():
            if line.startswith("@@"):
                if changes == []:
                    raise PitError("EPOCH_WRITER_STOP")
                fields = line[2:].split("\0")
                if len(fields) != 4 or len(fields[1].split()) != 1 or f"{fields[2]} <{fields[3]}>" != self.writer:
                    raise PitError("EPOCH_WRITER_STOP")
                changes = []
            elif line.startswith(":"):
                if changes is None or "\t" not in line:
                    raise PitError("EPOCH_WRITER_STOP")
                metadata, path = line.split("\t", 1)
                fields = metadata.split()
                if len(fields) != 5 or fields[4] not in {"A", "M"} or path in changes:
                    raise PitError("EPOCH_WRITER_STOP")
                status = fields[4]
                if status != "A" and path != LEDGER_INDEX_PATH or artifact_path(path) != path:
                    raise PitError("EPOCH_WRITER_STOP")
                changes.append(path)
                if path == LEDGER_INDEX_PATH:
                    index_changes.append((fields[2], fields[3], status))
        if changes == []:
            raise PitError("EPOCH_WRITER_STOP")
        index = self.read_at(head, LEDGER_INDEX_PATH)
        if not index_changes:
            if index is not None:
                raise PitError("EPOCH_WRITER_STOP")
            return
        if index is None:
            raise PitError("EPOCH_WRITER_STOP")
        records = validate_ledger_index(index)
        if len(records) != len(index_changes):
            raise PitError("EPOCH_WRITER_STOP")
        prefix = b""
        previous = "0" * 40
        for ordinal, (old, new, status) in enumerate(index_changes):
            prefix += canonical_bytes(records[ordinal])
            oid = hashlib.sha1(b"blob " + str(len(prefix)).encode("ascii") + b"\0" + prefix).hexdigest()
            if status != ("A" if ordinal == 0 else "M") or old != previous or new != oid:
                raise PitError("EPOCH_WRITER_STOP")
            previous = new

    def history_absent(self, h0: str, head: str, relative: str) -> bool:
        artifact_path(relative)
        if self.read_at(head, relative) is not None:
            return False
        return not self.git("log", "--format=%H", f"{h0}..{head}", "--", relative).stdout.strip()

    def context(self, h0: str, head: str, key: str) -> GitContext:
        self.verify_history(h0, head)
        slot = key.split("|", 1)[1]
        claim_path = PREFIX + "claims/" + claim_filesafe(slot) + ".json"
        claim_bytes = self.read_at(head, claim_path)
        claim = validate_canonical(claim_bytes) if claim_bytes is not None else None
        root = slot_root(slot)
        paths = self.git("ls-tree", "-r", "--name-only", head, "--", root).stdout.splitlines()
        raw = {path: self.read_at(head, path) for path in paths if path.startswith(root + "raw/")}
        manifests = {
            path: validate_canonical(self.read_at(head, path) or b"")
            for path in paths if path.startswith(root + "source-manifests/")
        }
        log = self.read_at(head, artifact_path(root + "run.log"))
        claim_before_raw = claim is not None
        if claim is not None:
            claim_commits = self.git("log", "--format=%H", "--diff-filter=A", head, "--", claim_path).stdout.splitlines()
            if len(claim_commits) != 1:
                raise PitError("EPOCH_WRITER_STOP")
            for path in raw:
                commits = self.git("log", "--format=%H", "--diff-filter=A", head, "--", path).stdout.splitlines()
                if len(commits) != 1 or self.git("merge-base", "--is-ancestor", claim_commits[0], commits[0], check=False).returncode:
                    claim_before_raw = False
        outcome_path = PREFIX + "outcomes/" + claim_filesafe(slot) + ".json"
        outcome_count = len(self.git("log", "--format=%H", "--diff-filter=A", head, "--", outcome_path).stdout.splitlines())
        return GitContext(h0, head, key, claim, claim_bytes, raw, manifests, log, outcome_count, claim_before_raw)

    def verify_commit(self, commit: str, parent: str, files: dict[str, tuple[bytes, bool]]) -> None:
        if self.remote_head() != commit:
            raise PitError("WRITE_UNCONFIRMED")
        if self.git("show", "-s", "--format=%P", commit).stdout.split() != [parent]:
            raise PitError("EPOCH_WRITER_STOP")
        if self.git("show", "-s", "--format=%an <%ae>", commit).stdout.strip() != self.writer:
            raise PitError("EPOCH_WRITER_STOP")
        changes = [line.split("\t", 1) for line in self.git("diff-tree", "--no-commit-id", "--name-status", "-r", commit).stdout.splitlines()]
        expected_changes = [
            ["M" if path == LEDGER_INDEX_PATH and self.read_at(parent, path) is not None else "A", path]
            for path in sorted(files)
        ]
        if changes != expected_changes:
            raise PitError("EPOCH_WRITER_STOP")
        for relative, (data, raw) in files.items():
            observed = self.read_at(commit, relative)
            if observed != data or sha256(observed or b"") != sha256(data):
                raise PitError("WRITE_UNCONFIRMED")
            if relative == LEDGER_INDEX_PATH:
                validate_ledger_index(observed)
            elif not raw:
                value = validate_canonical(observed)
                if "/claims/" in relative and (
                    value.get("authorized_writer_identity") != self.writer
                    or value.get("expected_parent_before_claim") != parent
                    or relative != PREFIX + "claims/" + claim_filesafe(value.get("formal_slot_utc", "")) + ".json"
                ):
                    raise PitError("EPOCH_WRITER_STOP")

    def publish_many(
        self,
        files: dict[str, tuple[bytes, bool]],
        expected_parent: str,
        message: str,
    ) -> tuple[str, str]:
        if not files:
            raise PitError("EMPTY_DELTA")
        for relative, (data, raw) in files.items():
            artifact_path(relative)
            if not raw:
                validate_canonical(data)
        self.git("fetch", "--quiet", "origin", f"refs/heads/{self.branch}")
        if self.remote_head() != expected_parent or self.git("status", "--porcelain").stdout:
            return "CAS_REJECTED", self.remote_head()
        if any(self.read_at(expected_parent, relative) is not None for relative in files if relative != LEDGER_INDEX_PATH):
            return "CAS_REJECTED", expected_parent
        self.git("checkout", "--quiet", "--detach", expected_parent)
        for relative, (data, _) in files.items():
            target = os.path.join(self.repo, *relative.split("/"))
            os.makedirs(os.path.dirname(target), exist_ok=True)
            with open(target, "wb") as handle:
                handle.write(data)
        self.git("add", "--sparse", "--", *sorted(files))
        if self.git("diff", "--cached", "--name-only").stdout.splitlines() != sorted(files):
            raise PitError("SCOPE_CHANGE")
        self.git("commit", "--quiet", "-m", message)
        commit = self.git("rev-parse", "HEAD").stdout.strip()
        pushed = self.git("push", "origin", f"HEAD:refs/heads/{self.branch}", check=False)
        if pushed.returncode:
            self.git("fetch", "--quiet", "origin", f"refs/heads/{self.branch}")
            return "CAS_REJECTED", self.remote_head()
        self.verify_commit(commit, expected_parent, files)
        return "PUBLISHED", commit

    def publish(self, relative: str, data: bytes, expected_parent: str, message: str, raw: bool = False) -> tuple[str, str]:
        return self.publish_many({relative: (data, raw)}, expected_parent, message)

    def verify_duplicate(
        self,
        key: str,
        expected_parent: str | None,
        head: str,
        h0: str,
        expected_i_impl: str,
    ) -> str:
        try:
            self.verify_history(h0, head)
        except PitError:
            return "EPOCH_WRITER_STOP"
        slot = key.split("|", 1)[1]
        claim_path = artifact_path(PREFIX + "claims/" + claim_filesafe(slot) + ".json")
        outcome_path = artifact_path(PREFIX + "outcomes/" + claim_filesafe(slot) + ".json")
        claim_data, outcome_data = self.read_at(head, claim_path), self.read_at(head, outcome_path)
        claim = validate_canonical(claim_data) if claim_data is not None else None
        if not SHA256_RE.fullmatch(expected_i_impl) or claim is not None and claim.get("I_impl") != expected_i_impl:
            return "EPOCH_WRITER_STOP"
        if claim is None:
            if outcome_data is None:
                return "EPOCH_WRITER_STOP"
            gap = validate_canonical(outcome_data)
            if gap.get("outcome_kind") != "GAP_NO_RUN" or gap.get("claim_relative_path") is not None or gap.get("claim_sha256") is not None:
                return "EPOCH_WRITER_STOP"
            if not self.history_absent(h0, head, claim_path):
                return "EPOCH_WRITER_STOP"
        else:
            gap = None
        claim_commits = self.git("log", "--format=%H", "--diff-filter=A", head, "--", claim_path).stdout.splitlines()
        if claim is not None and len(claim_commits) != 1:
            return "EPOCH_WRITER_STOP"
        if claim is not None:
            claim_commit = claim_commits[0]
            claim_parents = self.git("show", "-s", "--format=%P", claim_commit).stdout.split()
            claim_delta = self.git("diff-tree", "--no-commit-id", "--name-only", "-r", claim_commit).stdout.splitlines()
            claim_author = self.git("show", "-s", "--format=%an <%ae>", claim_commit).stdout.strip()
        if claim is not None and (
            claim.get("idempotency_key") != key
            or claim.get("authorized_writer_identity") != self.writer
            or len(claim_parents) != 1
            or claim.get("expected_parent_before_claim") != claim_parents[0]
            or expected_parent is not None and claim_parents[0] != expected_parent
            or claim_delta != [claim_path]
            or claim_author != self.writer
        ):
            return "EPOCH_WRITER_STOP"
        if outcome_data is None:
            return "CLAIM_HELD_NO_WRITE"
        outcome = validate_canonical(outcome_data)
        if outcome.get("I_impl") != expected_i_impl:
            return "EPOCH_WRITER_STOP"
        outcome_commits = self.git("log", "--format=%H", "--diff-filter=A", head, "--", outcome_path).stdout.splitlines()
        if len(outcome_commits) != 1:
            return "EPOCH_WRITER_STOP"
        outcome_commit = outcome_commits[0]
        outcome_parents = self.git("show", "-s", "--format=%P", outcome_commit).stdout.split()
        outcome_delta = self.git("diff-tree", "--no-commit-id", "--name-only", "-r", outcome_commit).stdout.splitlines()
        outcome_author = self.git("show", "-s", "--format=%an <%ae>", outcome_commit).stdout.strip()
        slot_manifest_path = artifact_path(slot_root(slot) + "slot-manifest.json")
        checkpoint_path = artifact_path(PREFIX + "checkpoints/" + claim_filesafe(slot) + ".json")
        if (
            outcome.get("idempotency_key") != key
            or len(outcome_parents) != 1
            or outcome.get("previous_ledger_head") != outcome_parents[0]
            or outcome_delta != sorted([LEDGER_INDEX_PATH, checkpoint_path, outcome_path, slot_manifest_path])
            or outcome_author != self.writer
        ):
            return "EPOCH_WRITER_STOP"
        if claim is not None and any(outcome.get(field) != claim.get(field) for field in (
            "I_impl", "formal_slot_utc", "idempotency_key", "attempt_id", "image_digest",
            "workflow_run_id", "job_id", "log_locator",
        )):
            return "EPOCH_WRITER_STOP"
        if claim is not None and (outcome.get("claim_relative_path") != claim_path or outcome.get("claim_sha256") != sha256(claim_data)):
            return "EPOCH_WRITER_STOP"
        log = self.read_at(head, outcome.get("log_locator", ""))
        if log is None or sha256(log) != outcome.get("log_sha256"):
            return "EPOCH_WRITER_STOP"
        paths = outcome.get("available_raw_relative_paths")
        hashes = outcome.get("available_raw_sha256s")
        if outcome.get("available_raw_count") != len(paths or []) or not isinstance(hashes, list):
            return "EPOCH_WRITER_STOP"
        observed_raw_hashes = []
        for path in paths:
            data = self.read_at(head, path)
            if data is None:
                return "EPOCH_WRITER_STOP"
            observed_raw_hashes.append(sha256(data))
        if sorted(set(observed_raw_hashes)) != hashes:
            return "EPOCH_WRITER_STOP"
        manifest_paths = [
            path for path in self.git("ls-tree", "-r", "--name-only", head, "--", slot_root(slot) + "source-manifests/").stdout.splitlines()
            if path.startswith(slot_root(slot) + "source-manifests/")
        ]
        manifests = [validate_canonical(self.read_at(head, path) or b"") for path in manifest_paths]
        manifests.sort(key=lambda item: (SOURCE_ORDER.index(item["source_id"]), item["page_ordinal"]))
        manifest_hashes = sorted({sha256(canonical_bytes(item)) for item in manifests})
        if outcome.get("source_manifest_sha256s") != manifest_hashes:
            return "EPOCH_WRITER_STOP"
        for manifest in manifests:
            if manifest.get("I_impl") != expected_i_impl:
                return "EPOCH_WRITER_STOP"
            if manifest.get("raw_relative_path"):
                raw = self.read_at(head, manifest["raw_relative_path"])
                if raw is None or sha256(raw) != manifest["raw_sha256"]:
                    return "EPOCH_WRITER_STOP"
        slot_manifest_data = self.read_at(head, slot_manifest_path)
        if slot_manifest_data is None:
            return "EPOCH_WRITER_STOP"
        slot_manifest = validate_canonical(slot_manifest_data)
        if (
            slot_manifest.get("idempotency_key") != key
            or slot_manifest.get("outcome_sha256") != sha256(outcome_data)
            or slot_manifest.get("source_manifest_sha256s") != manifest_hashes
            or slot_manifest.get("raw_sha256s") != hashes
        ):
            return "EPOCH_WRITER_STOP"
        records = [item for item in self.ledger_records(head) if item.get("idempotency_key") == key]
        if len(records) != 1 or records[0].get("outcome_sha256") != sha256(outcome_data) or records[0].get("slot_manifest_sha256") != sha256(slot_manifest_data):
            return "EPOCH_WRITER_STOP"
        checkpoint_data = self.read_at(head, checkpoint_path)
        if checkpoint_data is None:
            return "EPOCH_WRITER_STOP"
        checkpoint = validate_canonical(checkpoint_data)
        record = records[0]
        if checkpoint != {
            "formal_slot_utc": slot,
            "ledger_index_sha256": sha256(self.read_at(head, LEDGER_INDEX_PATH) or b""),
            "ledger_seq": record["ledger_seq"],
            "terminal_parent": outcome_parents[0],
        } or slot_manifest.get("ledger_seq") != record["ledger_seq"]:
            return "EPOCH_WRITER_STOP"
        if outcome.get("outcome_kind") in {"SNAPSHOT_COMPLETE", "SNAPSHOT_PARTIAL", "SNAPSHOT_INVALID"}:
            try:
                if claim is None:
                    raise PitError("CLAIM_PROVENANCE")
                context = self.context(h0, head, key)
                wrapped_claim = {"relative_path": claim_path, "value": claim}
                validate_snapshot(outcome, wrapped_claim, manifests, context, True, expected_i_impl)
            except PitError:
                return "EPOCH_WRITER_STOP"
        return "DUPLICATE_NO_WRITE"

    def ledger_records(self, head: str) -> list[dict[str, Any]]:
        data = self.read_at(head, LEDGER_INDEX_PATH)
        return [] if data is None else validate_ledger_index(data)

    def publish_terminal(
        self,
        h0: str,
        claim: dict[str, Any],
        kind: str,
        reasons: Iterable[str],
        claim_exists: bool,
        expected_i_impl: str,
        payload: dict[str, Any] | None = None,
    ) -> tuple[str, str]:
        key, slot = claim["value"]["idempotency_key"], claim["value"]["formal_slot_utc"]
        if not SHA256_RE.fullmatch(expected_i_impl) or claim["value"].get("I_impl") != expected_i_impl:
            return "EPOCH_WRITER_STOP", self.remote_head()
        head = self.remote_head()
        self.verify_history(h0, head)
        log_path, log = claim["value"]["log_locator"], run_log_bytes(claim)
        if self.read_at(head, log_path) is None:
            status, head = self.publish(log_path, log, head, "PIT run log", True)
            if status != "PUBLISHED":
                return status, head
        context = self.context(h0, head, key)
        manifests = sorted(
            context.source_manifests.items(),
            key=lambda item: (SOURCE_ORDER.index(item[1]["source_id"]), item[1]["page_ordinal"]),
        )
        raw_pairs = [
            (manifest["raw_relative_path"], manifest["raw_sha256"])
            for _, manifest in manifests if manifest.get("raw_sha256")
        ]
        value = claim["value"]
        outcome = payload or {
            "I_impl": value["I_impl"], "attempt_id": value["attempt_id"] if claim_exists else None,
            "available_raw_count": len(raw_pairs), "available_raw_relative_paths": [item[0] for item in raw_pairs],
            "available_raw_sha256s": sorted({item[1] for item in raw_pairs}),
            "claim_relative_path": claim["relative_path"] if claim_exists else None,
            "claim_sha256": sha256(canonical_bytes(value)) if claim_exists else None,
            "contract_id": CONTRACT_ID, "epoch_id": EPOCH_ID, "formal_slot_utc": slot,
            "idempotency_key": key, "image_digest": value["image_digest"],
            "job_id": value["job_id"], "log_locator": log_path,
            "log_sha256": sha256(log), "materialized_at_utc": value["claimed_at_utc"],
            "outcome_kind": kind, "previous_ledger_head": head,
            "reason_codes": ordered(reasons, REASON_CODE_ORDER), "slot_status": OUTCOME_STATUS[kind],
            "source_manifest_sha256s": sorted({sha256(canonical_bytes(item)) for _, item in manifests}),
            "workflow_run_id": value["workflow_run_id"],
        }
        if payload is not None:
            outcome = dict(payload)
            outcome.update(
                previous_ledger_head=head, image_digest=value["image_digest"],
                log_locator=log_path, log_sha256=sha256(log),
                source_manifest_sha256s=sorted({sha256(canonical_bytes(item)) for _, item in manifests}),
                available_raw_count=len(raw_pairs),
                available_raw_relative_paths=[item[0] for item in raw_pairs],
                available_raw_sha256s=sorted({item[1] for item in raw_pairs}),
            )
        if outcome.get("outcome_kind") in {"SNAPSHOT_COMPLETE", "SNAPSHOT_PARTIAL", "SNAPSHOT_INVALID"}:
            validate_snapshot(
                outcome,
                claim,
                [item for _, item in manifests],
                context,
                False,
                expected_i_impl,
            )
        outcome_path = PREFIX + "outcomes/" + claim_filesafe(slot) + ".json"
        outcome_bytes = canonical_bytes(outcome)
        seq = len(self.ledger_records(head)) + 1
        slot_manifest = {
            "claim_sha256": outcome["claim_sha256"], "error_record_sha256s": sorted({
                item["error_record_sha256"] for _, item in manifests if item.get("error_record_sha256")
            }),
            "formal_slot_utc": slot, "idempotency_key": key, "ledger_seq": seq,
            "outcome_relative_path": outcome_path, "outcome_sha256": sha256(outcome_bytes),
            "raw_sha256s": outcome["available_raw_sha256s"],
            "source_config_sha256": sha256(canonical_bytes(URLS)),
            "source_manifest_sha256s": outcome["source_manifest_sha256s"],
        }
        slot_manifest_path = artifact_path(slot_root(slot) + "slot-manifest.json")
        slot_manifest_bytes = canonical_bytes(slot_manifest)
        record = {
            "formal_slot_utc": slot, "idempotency_key": key, "ledger_seq": seq,
            "outcome_relative_path": outcome_path, "outcome_sha256": sha256(outcome_bytes),
            "slot_manifest_relative_path": slot_manifest_path,
            "slot_manifest_sha256": sha256(slot_manifest_bytes),
        }
        old_index = self.read_at(head, LEDGER_INDEX_PATH) or b""
        index = old_index + canonical_bytes(record)
        validate_ledger_index(index)
        checkpoint_path = artifact_path(PREFIX + "checkpoints/" + claim_filesafe(slot) + ".json")
        checkpoint = canonical_bytes({
            "formal_slot_utc": slot, "ledger_index_sha256": sha256(index),
            "ledger_seq": seq, "terminal_parent": head,
        })
        status, terminal_head = self.publish_many(
            {
                LEDGER_INDEX_PATH: (index, True),
                checkpoint_path: (checkpoint, False),
                outcome_path: (outcome_bytes, False),
                slot_manifest_path: (slot_manifest_bytes, False),
            },
            head, "PIT slot terminal",
        )
        if status == "PUBLISHED":
            return status, terminal_head
        return self.verify_duplicate(key, None, terminal_head, h0, expected_i_impl), terminal_head

    def recover_slot(
        self,
        h0: str,
        claim: dict[str, Any],
        now: datetime,
        expected_i_impl: str,
    ) -> tuple[str, str]:
        slot, key = claim["value"]["formal_slot_utc"], claim["value"]["idempotency_key"]
        head = self.remote_head()
        if now.astimezone(timezone.utc) < materialization_deadline(slot):
            return "EPOCH_WRITER_STOP", head
        self.verify_history(h0, head)
        claim_path = claim["relative_path"]
        outcome_path = artifact_path(PREFIX + "outcomes/" + claim_filesafe(slot) + ".json")
        if self.read_at(head, outcome_path) is not None:
            return self.verify_duplicate(key, None, head, h0, expected_i_impl), head
        claim_data = self.read_at(head, claim_path)
        if claim_data is not None:
            observed = validate_canonical(claim_data)
            if observed.get("idempotency_key") != key:
                return "EPOCH_WRITER_STOP", head
            existing = {"relative_path": claim_path, "value": observed}
            context = self.context(h0, head, key)
            reasons = ["ATTEMPT_ABORTED"] + ([] if context.raw else ["NO_RAW_DURABLY_PUBLISHED"])
            return self.publish_terminal(h0, existing, "ABORTED_ATTEMPT", reasons, True, expected_i_impl)
        if not self.history_absent(h0, head, claim_path):
            return "EPOCH_WRITER_STOP", head
        return self.publish_terminal(h0, claim, "GAP_NO_RUN", ["NO_CLAIM_BEFORE_DEADLINE"], False, expected_i_impl)

    def recover_gap(self, h0: str, claim: dict[str, Any], now: datetime, expected_i_impl: str) -> tuple[str, str]:
        return self.recover_slot(h0, claim, now, expected_i_impl)


def _live_send(source_id: str, url: str) -> tuple[int, bytes, dict[str, str]]:
    validate_source_url(source_id, url)
    request = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, response.read(), dict(response.headers)
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read(), dict(exc.headers)


def prepare_live_slot(
    writer: GitWriter,
    h0: str,
    activation_slot: str,
    slot: str,
    now: datetime,
    i_impl: str,
    writer_id: str,
    workflow_run_id: str = "recovery-run",
    job_id: str = "capture",
    image_digest: str = "sha256:" + "0" * 64,
) -> str:
    records = writer.ledger_records(writer.remote_head())
    overdue = add_slots(records[-1]["formal_slot_utc"]) if records else activation_slot
    while overdue <= slot and now >= materialization_deadline(overdue):
        recovery_claim = make_claim(
            overdue, i_impl, writer_id, writer.remote_head(),
            now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z",
            workflow_run_id, job_id, image_digest=image_digest,
        )
        status, _ = writer.recover_slot(h0, recovery_claim, now, i_impl)
        if status not in {"PUBLISHED", "DUPLICATE_NO_WRITE"}:
            raise PitError("EPOCH_WRITER_STOP")
        overdue = add_slots(overdue)
    if overdue < slot:
        raise PitError("EPOCH_WRITER_STOP")
    if overdue == slot:
        acquisition_window(slot, now)
    return writer.remote_head()


def live_capture() -> None:
    env = dict(os.environ)
    root = os.path.dirname(os.path.abspath(__file__))
    require_live_authorization(env, root)
    now = datetime.now(timezone.utc)
    minute = 30 if now.minute >= 30 else 0
    slot_dt = now.replace(minute=minute, second=0, microsecond=0)
    slot = slot_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    writer_id = env["PIT_AUTHORIZED_WRITER"]
    writer = GitWriter(root, writer_id)
    head = writer.remote_head()
    writer.verify_history(env["PIT_H0"], head)
    head = prepare_live_slot(
        writer, env["PIT_H0"], env["PIT_ACTIVATION_CANDIDATE_SLOT"], slot, now,
        env["PIT_I_IMPL"], writer_id, env["CLOUD_RUN_EXECUTION"],
        env["CLOUD_RUN_JOB"], env["PIT_IMAGE_DIGEST"],
    )
    key = EPOCH_ID + "|" + slot
    outcome_path = artifact_path(PREFIX + "outcomes/" + claim_filesafe(slot) + ".json")
    if writer.read_at(head, outcome_path) is not None:
        print(writer.verify_duplicate(key, None, head, env["PIT_H0"], env["PIT_I_IMPL"]))
        return
    claim = make_claim(
        slot, env["PIT_I_IMPL"], writer_id, head,
        now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z",
        env["CLOUD_RUN_EXECUTION"], env["CLOUD_RUN_JOB"], image_digest=env["PIT_IMAGE_DIGEST"],
    )
    key, claim_path = claim["value"]["idempotency_key"], claim["relative_path"]
    if not writer.history_absent(env["PIT_H0"], head, claim_path):
        print(writer.verify_duplicate(key, None, head, env["PIT_H0"], env["PIT_I_IMPL"]))
        return
    status, head = writer.publish(claim_path, canonical_bytes(claim["value"]), head, "PIT claim")
    if status != "PUBLISHED":
        print(writer.verify_duplicate(
            key, claim["value"]["expected_parent_before_claim"], head, env["PIT_H0"], env["PIT_I_IMPL"]
        ))
        return
    ledger = Ledger(writer_id, head=head)
    ledger.claims[key] = claim
    ledger.history_claim_keys.add(key)
    pending_evidence: dict[str, tuple[bytes, bool]] = {}

    def durable(relative: str, data: bytes, raw: bool) -> None:
        nonlocal head
        if not raw:
            pending_evidence[relative] = (data, False)
            return
        status, new_head = writer.publish(relative, data, head, "PIT artifact", raw)
        if status != "PUBLISHED":
            raise PitError(writer.verify_duplicate(key, head, new_head, env["PIT_H0"], env["PIT_I_IMPL"]))
        head = new_head

    parsed = acquire_fixture_sources(
        ledger, claim,
        _live_send,
        lambda: datetime.now(timezone.utc), durable, time.sleep, False,
    )
    log_path = claim["value"]["log_locator"]
    pending_evidence[log_path] = (run_log_bytes(claim), True)
    status, head = writer.publish_many(pending_evidence, head, "PIT evidence")
    if status != "PUBLISHED":
        raise PitError(writer.verify_duplicate(key, None, head, env["PIT_H0"], env["PIT_I_IMPL"]))
    if "__outcome__" in parsed:
        outcome = parsed["__outcome__"]
    else:
        outcome = build_snapshot(parsed["CG_TOP250"].encode() if isinstance(parsed["CG_TOP250"], str) else ledger.raw[key][0][1], parsed, claim)
    if outcome.get("outcome_kind") in {"SNAPSHOT_COMPLETE", "SNAPSHOT_PARTIAL", "SNAPSHOT_INVALID"}:
        validate_snapshot(outcome)
    status, final_head = writer.publish_terminal(
        env["PIT_H0"], claim, outcome["outcome_kind"], outcome["reason_codes"], True, env["PIT_I_IMPL"], outcome
    )
    if status not in {"PUBLISHED", "DUPLICATE_NO_WRITE"}:
        raise PitError("EPOCH_WRITER_STOP")
    if outcome.get("outcome_kind") in {"SNAPSHOT_COMPLETE", "SNAPSHOT_PARTIAL", "SNAPSHOT_INVALID"}:
        final = validate_canonical(writer.read_at(final_head, PREFIX + "outcomes/" + claim_filesafe(slot) + ".json") or b"")
        context = writer.context(env["PIT_H0"], final_head, key)
        manifests = sorted(context.source_manifests.values(), key=lambda item: (SOURCE_ORDER.index(item["source_id"]), item["page_ordinal"]))
        validate_snapshot(final, claim, manifests, context, True, env["PIT_I_IMPL"])
    print("PUBLISHED_SLOT_OUTCOME" if status == "PUBLISHED" else "DUPLICATE_NO_WRITE")


def cloud_run() -> int:
    env = dict(os.environ)
    root = os.path.dirname(os.path.abspath(__file__))
    stage = "STOP_PERMISSION_REQUIRED_AUTH"
    try:
        stage = "STOP_PERMISSION_REQUIRED_AUTH"
        require_live_authorization(env, root)
        stage = "STOP_PERMISSION_REQUIRED_SECRET_MOUNT"
        if not os.path.isfile(SECRET_MOUNT):
            raise PitError(stage)
        with tempfile.TemporaryDirectory(prefix="pit-ledger-cloud-run-") as temp:
            key_path = os.path.join(temp, "id_ed25519")
            public_path = key_path + ".pub"
            known_hosts_path = os.path.join(temp, "known_hosts")
            clone_path = os.path.join(temp, "repo")
            shutil.copyfile(SECRET_MOUNT, key_path)
            os.chmod(key_path, 0o600)

            def checked(*args: str, cwd: str | None = None, run_env: dict[str, str] | None = None) -> str:
                result = subprocess.run(
                    list(args), cwd=cwd, env=run_env, capture_output=True,
                    text=True, check=False,
                )
                if result.returncode:
                    raise PitError(stage)
                return result.stdout.strip()

            stage = "STOP_PERMISSION_REQUIRED_KEY_DERIVE"
            public = checked("ssh-keygen", "-y", "-f", key_path)
            with open(public_path, "w", encoding="ascii", newline="\n") as handle:
                handle.write(public + "\n")
            stage = "STOP_PERMISSION_REQUIRED_KEY_FINGERPRINT"
            fingerprint = checked("ssh-keygen", "-l", "-E", "sha256", "-f", public_path).split()
            if len(fingerprint) < 2 or fingerprint[1] != env["PIT_DEPLOY_KEY_FINGERPRINT"]:
                raise PitError(stage)
            with open(known_hosts_path, "w", encoding="ascii", newline="\n") as handle:
                handle.write(KNOWN_HOSTS)
            ssh_command = (
                "ssh -i " + shlex.quote(key_path)
                + " -o IdentitiesOnly=yes -o UserKnownHostsFile=" + shlex.quote(known_hosts_path)
                + " -o StrictHostKeyChecking=yes"
            )
            child_env = dict(env, GIT_SSH_COMMAND=ssh_command)
            stage = "STOP_PERMISSION_REQUIRED_CLONE"
            checked(
                "git", "clone", "--quiet", "--filter=blob:none", "--sparse",
                "--branch", GITHUB_BRANCH, "--single-branch", GITHUB_REPO,
                clone_path, run_env=child_env,
            )
            stage = "STOP_PERMISSION_REQUIRED_BINDING"
            checked("git", "merge-base", "--is-ancestor", env["PIT_H0"], "HEAD", cwd=clone_path)
            if (
                checked("git", "branch", "--show-current", cwd=clone_path) != GITHUB_BRANCH
                or checked("git", "remote", "get-url", "origin", cwd=clone_path) != GITHUB_REPO
                or checked("git", "config", "--get", "remote.origin.partialclonefilter", cwd=clone_path) != "blob:none"
                or os.path.exists(os.path.join(clone_path, "pit_ledger"))
                or current_i_impl(clone_path) != env["PIT_I_IMPL"]
            ):
                raise PitError(stage)
            stage = "STOP_PERMISSION_REQUIRED_WRITER_ID"
            checked("git", "config", "user.name", "PIT Ledger Writer", cwd=clone_path)
            checked("git", "config", "user.email", "pit-ledger@users.noreply.github.com", cwd=clone_path)
            if (
                checked("git", "config", "user.name", cwd=clone_path) + " <"
                + checked("git", "config", "user.email", cwd=clone_path) + ">"
                != AUTHORIZED_WRITER
            ):
                raise PitError(stage)
            stage = "STOP_PERMISSION_REQUIRED_CAPTURE"
            result = subprocess.run(
                [sys.executable, os.path.join(clone_path, "pit_ledger.py"), "capture"],
                cwd=clone_path, env=child_env, capture_output=True, text=True, check=False,
            )
            if result.returncode:
                detail = result.stderr.strip()
                controlled = re.fullmatch(r"PIT_ERROR:([A-Z][A-Z0-9_]{2,127})", detail)
                if controlled:
                    print(controlled.group(1), file=sys.stderr)
                elif result.stderr:
                    print(
                        f"CAPTURE_CHILD_STDERR_SHA256={sha256(result.stderr.encode())} "
                        f"BYTES={len(result.stderr.encode())}",
                        file=sys.stderr,
                    )
                raise PitError(stage)
            return 0
    except Exception:
        raise PitError(stage) from None


def oracle_objects() -> tuple[dict[str, Any], ...]:
    return (
        universe_gap("QA_FAILURE"),
        funding_schema_failure("BN_FUT_PREMIUM_INDEX"),
        perp_decision(
            "BINANCE_USDM",
            "BTC",
            [
                {"symbol": "BTCUSDT", "baseAsset": "BTC", "quoteAsset": "USDT", "contractType": "PERPETUAL", "status": "TRADING"},
                {"symbol": "BTCUSDT", "baseAsset": "BTC", "quoteAsset": "USDT", "contractType": "PERPETUAL", "status": "TRADING"},
            ],
        ),
        {
            "borrowable": None, "mapping_status": "MAPPED",
            "missing_reasons": ["NOT_APPLICABLE_NO_PERP", "NOT_OBSERVED_PUBLIC_ONLY"],
            "perp_exists": False, "reason_codes": [], "venue": "BINANCE_USDM",
        },
        {
            "borrowable": None, "mapping_status": "MAPPED",
            "missing_reasons": ["NOT_OBSERVED_PUBLIC_ONLY", "SOURCE_FAILURE"],
            "outcome_kind": "SNAPSHOT_PARTIAL", "perp_exists": None,
            "reason_codes": ["SOURCE_FAILURE"], "slot_status": "PARTIAL", "venue": "BINANCE_USDM",
        },
        {
            "borrowable": None, "mapping_status": "MAPPED",
            "missing_reasons": ["NOT_OBSERVED_PUBLIC_ONLY", "SCHEMA_FAILURE"],
            "outcome_kind": "SNAPSHOT_PARTIAL", "perp_exists": True,
            "reason_codes": ["SCHEMA_FAILURE"], "slot_status": "PARTIAL", "venue": "BINANCE_USDM",
        },
    )


def self_check() -> None:
    expected = (
        (242, "BB6A1F4C2A99D23E31C79809A1D25A38720A004831C34FE168EC681788EC2165"),
        (333, "8D13A204BDC833121A88F3C531B74A677987E0CE8A96C5610C9DA8C10CA2300D"),
        (257, "96701B35D8FC21BB3BD17BD3F27BA492E82C0959AEEEC9CB31BDDD58B7575CA4"),
        (179, "C467F918C0C5948E83DD95484A4502306F8344CA0BAC8507946D73E1033035FE"),
        (244, "C92039EC6CDBCB43F6210450CE969B230252AA700577FE77C8FF2D9149F4640A"),
        (244, "4BC9C82E097C5348587C773DC0D6EA18C08D3A68A4FCAC1146A3A44BAD0AD8B6"),
    )
    for value, oracle in zip(oracle_objects(), expected):
        data = canonical_bytes(value)
        assert (len(data), sha256(data)) == oracle
        assert validate_canonical(data) == value
    assert spread_bps("99", "100") == "100.50251256"
    assert spread_bps("397999999999900000000000000000000", "402000000000100000000000000000001") == "100.00000001"
    assert SHA256_RE.fullmatch(current_i_impl(os.path.dirname(os.path.abspath(__file__))))
    try:
        require_live_authorization({})
    except PitError as exc:
        assert str(exc) == "STOP_PERMISSION_REQUIRED"
    else:
        raise AssertionError("live boundary opened")
    print("SELF_CHECK PASS")


def e2e_self_check() -> None:
    writer_identity = "PIT Ledger Writer <pit@example.invalid>"
    with tempfile.TemporaryDirectory(prefix="pit-ledger-e2e-") as temp:
        remote, seed = os.path.join(temp, "remote.git"), os.path.join(temp, "seed")

        def run(cwd: str, *args: str) -> str:
            result = subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, check=False)
            if result.returncode:
                raise AssertionError(result.stderr)
            return result.stdout.strip()

        os.mkdir(remote)
        run(remote, "init", "--bare", "--quiet")
        os.mkdir(seed)
        run(seed, "init", "--quiet")
        run(seed, "config", "user.name", "seed")
        run(seed, "config", "user.email", "seed@example.invalid")
        run(seed, "commit", "--allow-empty", "--quiet", "-m", "H0")
        run(seed, "branch", "-M", GITHUB_BRANCH)
        run(seed, "remote", "add", "origin", remote)
        run(seed, "push", "--quiet", "-u", "origin", GITHUB_BRANCH)
        h0 = run(seed, "rev-parse", "HEAD")
        clones = []
        for name in ("a", "b", "wrong"):
            path = os.path.join(temp, name)
            run(temp, "clone", "--quiet", "--branch", GITHUB_BRANCH, remote, path)
            run(path, "config", "user.name", "Actual Writer" if name == "wrong" else "PIT Ledger Writer")
            run(path, "config", "user.email", "actual@example.invalid" if name == "wrong" else "pit@example.invalid")
            clones.append(path)
        a, b = GitWriter(clones[0], writer_identity), GitWriter(clones[1], writer_identity)
        slot = "2026-07-26T20:00:00.000Z"
        claim = make_claim(slot, "A" * 64, writer_identity, h0)
        key = claim["value"]["idempotency_key"]
        status, claim_head = a.publish(claim["relative_path"], canonical_bytes(claim["value"]), h0, "claim")
        assert status == "PUBLISHED"
        status, raced_head = b.publish(claim["relative_path"], canonical_bytes(claim["value"]), h0, "racing claim")
        assert status == "CAS_REJECTED" and raced_head == claim_head
        loser_path = artifact_path(PREFIX + "race-loser.json")
        loser_target = os.path.join(clones[1], *loser_path.split("/"))
        os.makedirs(os.path.dirname(loser_target), exist_ok=True)
        with open(loser_target, "wb") as handle:
            handle.write(canonical_bytes({}))
        b.git("add", "--", loser_path)
        b.git("commit", "--quiet", "-m", "losing non-fast-forward")
        rejected = b.git("push", "origin", f"HEAD:refs/heads/{b.branch}", check=False)
        assert rejected.returncode != 0 and b.remote_head() == claim_head
        assert b.verify_duplicate(key, h0, raced_head, h0, "A" * 64) == "CLAIM_HELD_NO_WRITE"
        raw_path = slot_artifact(slot, "raw", "CG_TOP250-0.bin")
        raw = b'{"fixture":true}'
        status, raw_head = a.publish(raw_path, raw, claim_head, "raw", True)
        assert status == "PUBLISHED"
        manifest = {
            "I_impl": "A" * 64, "attempt_count": 1, "auth_class": "PUBLIC",
            "canonical_url_without_secret": URLS["CG_TOP250"], "error_record_relative_path": None,
            "error_record_sha256": None, "fetched_at_utc": slot, "http_status": 200,
            "method": "GET", "page_ordinal": 0, "parse_status": "PARSE_OK", "qa_status": "QA_OK",
            "raw_bytes": len(raw), "raw_relative_path": raw_path, "raw_sha256": sha256(raw),
            "requested_at_utc": slot, "server_time_utc": None, "source_id": "CG_TOP250",
            "source_status": "SOURCE_OK",
        }
        manifest_files = {
            slot_artifact(slot, "source-manifests", "CG_TOP250-0.json"): (canonical_bytes(manifest), False)
        }
        for source_id in SOURCE_ORDER[1:]:
            not_run = {
                "I_impl": "A" * 64, "attempt_count": 0,
                "auth_class": "PUBLIC",
                "canonical_url_without_secret": URLS[source_id], "error_record_relative_path": None,
                "error_record_sha256": None, "fetched_at_utc": None, "http_status": None,
                "method": "GET", "page_ordinal": 0, "parse_status": "PARSE_NOT_RUN",
                "qa_status": "QA_NOT_RUN", "raw_bytes": None, "raw_relative_path": None,
                "raw_sha256": None, "requested_at_utc": None, "server_time_utc": None,
                "source_id": source_id, "source_status": "SOURCE_NOT_RUN",
            }
            manifest_files[slot_artifact(slot, "source-manifests", source_id + "-0.json")] = (canonical_bytes(not_run), False)
        status, _ = a.publish_many(manifest_files, raw_head, "source manifests")
        assert status == "PUBLISHED"
        status, complete_head = a.publish_terminal(
            h0, claim, "ABORTED_ATTEMPT", ["ATTEMPT_ABORTED"], True, "A" * 64
        )
        assert status == "PUBLISHED"
        assert b.verify_duplicate(key, h0, complete_head, h0, "A" * 64) == "DUPLICATE_NO_WRITE"
        assert b.verify_duplicate(key, h0, complete_head, h0, "B" * 64) == "EPOCH_WRITER_STOP"
        slot2 = add_slots(slot)
        claim2 = make_claim(slot2, "A" * 64, writer_identity, complete_head)
        status, _ = a.publish(claim2["relative_path"], canonical_bytes(claim2["value"]), complete_head, "stale claim")
        assert status == "PUBLISHED"
        status, aborted_head = a.recover_slot(
            h0, claim2, datetime(2026, 7, 26, 20, 46, tzinfo=timezone.utc), "A" * 64
        )
        assert status == "PUBLISHED"
        aborted = validate_canonical(a.read_at(aborted_head, PREFIX + "outcomes/" + claim_filesafe(slot2) + ".json") or b"")
        assert aborted["outcome_kind"] == "ABORTED_ATTEMPT"
        slot3 = add_slots(slot2)
        gap_claim = make_claim(slot3, "A" * 64, writer_identity, aborted_head)
        before = a.remote_head()
        assert a.recover_slot(
            h0, gap_claim, datetime(2026, 7, 26, 21, 14, tzinfo=timezone.utc), "A" * 64
        )[0] == "EPOCH_WRITER_STOP"
        assert a.remote_head() == before
        status, recovery_head = a.recover_slot(
            h0, gap_claim, datetime(2026, 7, 26, 21, 16, tzinfo=timezone.utc), "A" * 64
        )
        assert status == "PUBLISHED"
        gap = validate_canonical(a.read_at(recovery_head, PREFIX + "outcomes/" + claim_filesafe(slot3) + ".json") or b"")
        assert gap["outcome_kind"] == "GAP_NO_RUN"
        records = a.ledger_records(recovery_head)
        assert [row["ledger_seq"] for row in records] == [1, 2, 3]
        assert [row["formal_slot_utc"] for row in records] == [slot, slot2, slot3]
        a.verify_history(h0, recovery_head)
        try:
            a.git("push", "--force", "origin", "HEAD")
        except PitError as exc:
            assert str(exc) == "FORCE_FORBIDDEN"
        else:
            raise AssertionError("force was not forbidden")

        crash_slot = add_slots(slot3)
        crash_claim = make_claim(crash_slot, "A" * 64, writer_identity, recovery_head)
        status, crash_claim_head = a.publish(
            crash_claim["relative_path"], canonical_bytes(crash_claim["value"]), recovery_head, "crash claim"
        )
        assert status == "PUBLISHED"
        status, log_head = a.publish(
            crash_claim["value"]["log_locator"], run_log_bytes(crash_claim), crash_claim_head, "crash log", True
        )
        assert status == "PUBLISHED"

        class CrashAfterRemoteAcceptance(GitWriter):
            def verify_commit(self, commit: str, parent: str, files: dict[str, tuple[bytes, bool]]) -> None:
                super().verify_commit(commit, parent, files)
                raise RuntimeError("SIMULATED_CRASH_AFTER_REMOTE_ACCEPTANCE")

        crashing = CrashAfterRemoteAcceptance(clones[0], writer_identity)
        try:
            crashing.publish_terminal(
                h0, crash_claim, "ABORTED_ATTEMPT",
                ["ATTEMPT_ABORTED", "NO_RAW_DURABLY_PUBLISHED"], True, "A" * 64,
            )
        except RuntimeError as exc:
            assert str(exc) == "SIMULATED_CRASH_AFTER_REMOTE_ACCEPTANCE"
        else:
            raise AssertionError("terminal crash was not simulated")
        crash_head = a.remote_head()
        status, resumed_head = a.recover_slot(
            h0, crash_claim, datetime(2026, 7, 26, 21, 46, tzinfo=timezone.utc), "A" * 64
        )
        assert status == "DUPLICATE_NO_WRITE" and resumed_head == crash_head
        assert [row["ledger_seq"] for row in a.ledger_records(resumed_head)] == [1, 2, 3, 4]

        bad_slot = add_slots(crash_slot)
        bad_claim = make_claim(bad_slot, "A" * 64, writer_identity, resumed_head)
        status, bad_claim_head = a.publish(bad_claim["relative_path"], canonical_bytes(bad_claim["value"]), resumed_head, "bad provenance claim")
        assert status == "PUBLISHED"
        bad_outcome = base_outcome(bad_claim, "ABORTED_ATTEMPT", ["ATTEMPT_ABORTED", "NO_RAW_DURABLY_PUBLISHED"], [], bad_claim_head)
        bad_outcome.update(I_impl="B" * 64, workflow_run_id="wrong-run", job_id="wrong-job")
        status, bad_head = a.publish_terminal(
            h0, bad_claim, "ABORTED_ATTEMPT", bad_outcome["reason_codes"], True, "A" * 64, bad_outcome
        )
        assert status == "PUBLISHED"
        assert a.verify_duplicate(
            bad_claim["value"]["idempotency_key"], resumed_head, bad_head, h0, "A" * 64
        ) == "EPOCH_WRITER_STOP"
        tampered_records = a.ledger_records(bad_head)
        tampered_records[0]["outcome_sha256"] = "C" * 64
        status, tampered_head = a.publish_many(
            {LEDGER_INDEX_PATH: (canonical_jsonl(tampered_records), True)},
            bad_head, "tampered historical prefix fixture",
        )
        assert status == "PUBLISHED"
        try:
            a.verify_history(h0, tampered_head)
        except PitError as exc:
            assert str(exc) == "EPOCH_WRITER_STOP"
        else:
            raise AssertionError("tampered index prefix accepted")
        run(remote, "update-ref", f"refs/heads/{GITHUB_BRANCH}", bad_head, tampered_head)
        wrong = GitWriter(clones[2], writer_identity)
        wrong_claim = make_claim(add_slots(bad_slot), "A" * 64, writer_identity, bad_head)
        try:
            wrong.publish(wrong_claim["relative_path"], canonical_bytes(wrong_claim["value"]), bad_head, "wrong author")
        except PitError as exc:
            assert str(exc) == "EPOCH_WRITER_STOP"
        else:
            raise AssertionError("wrong author claim accepted")
    print("E2E_SELF_CHECK PASS")


def main(argv: list[str]) -> int:
    if argv == ["self-check"]:
        self_check()
        return 0
    if argv in (["workflow"], ["capture"]):
        live_capture()
        return 0
    if argv == ["cloud-run"]:
        return cloud_run()
    if argv == ["e2e-self-check"]:
        e2e_self_check()
        return 0
    print("usage: pit_ledger.py self-check|e2e-self-check|capture|cloud-run", file=sys.stderr)
    return 2


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except PitError as exc:
        prefix = "PIT_ERROR:" if sys.argv[1:] == ["capture"] else ""
        print(prefix + str(exc), file=sys.stderr)
        raise SystemExit(1)
