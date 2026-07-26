"""PIT ledger v1 r4: deterministic producer/validator core.

Live capture is fail-closed until a separately authorized activation supplies
every boundary value.  Offline fixtures exercise the same code paths.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Iterable

CONTRACT_ID = "PIT_LEDGER_V1"
EPOCH_ID = "BASKET_PIT_LEDGER_TOP250_BINANCE_BYBIT_V1"
PREFIX = f"pit_ledger/{EPOCH_ID}/"
CONCURRENCY_GROUP = f"pit-ledger-{EPOCH_ID}"
VENUES = ("BINANCE_USDM", "BYBIT_LINEAR")
SOURCE_ORDER = (
    "CG_TOP250",
    "BN_FUT_EXCHANGE_INFO",
    "BN_FUT_PREMIUM_INDEX",
    "BN_FUT_BOOK_TICKER",
    "BN_MARGIN_ASSETS",
    "BN_MARGIN_PAIRS",
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
    "BN_MARGIN_ASSETS": "https://api.binance.com/sapi/v1/margin/allAssets",
    "BN_MARGIN_PAIRS": "https://api.binance.com/sapi/v1/margin/allPairs",
    "BY_LINEAR_INSTRUMENTS": "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000",
    "BY_LINEAR_TICKERS": "https://api.bybit.com/v5/market/tickers?category=linear",
    "BY_SPOT_INSTRUMENTS": "https://api.bybit.com/v5/market/instruments-info?category=spot",
    "BY_MARGIN_BORROWABLE": "https://api.bybit.com/v5/spot-margin-trade/data?vipLevel=No%20VIP",
}
READ_ONLY_SOURCES = {"BN_MARGIN_ASSETS", "BN_MARGIN_PAIRS"}
MARGIN_TRADING = ("none", "both", "utaOnly", "normalSpotOnly")
MISSING_REASON_ORDER = (
    "NOT_APPLICABLE_NO_PERP",
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
    "auth_class": {"PUBLIC", "READ_ONLY_MARKET_DATA"},
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


class PitError(ValueError):
    """Fail-closed contract violation."""


def _validate_derived(value: Any) -> None:
    if value is None or isinstance(value, (bool, str)):
        return
    if isinstance(value, int) and not isinstance(value, bool):
        return
    if isinstance(value, list):
        for item in value:
            _validate_derived(item)
        return
    if isinstance(value, dict) and all(isinstance(key, str) for key in value):
        for key, item in value.items():
            if key in FORBIDDEN_FIELDS:
                raise PitError("FREE_FORM_FIELD")
            _validate_derived(item)
        return
    raise PitError("NON_CANONICAL_SCALAR")


def canonical_bytes(value: Any) -> bytes:
    _validate_derived(value)
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
    return b"".join(canonical_bytes(row) for row in rows)


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


def validate_normative(value: Any) -> None:
    if isinstance(value, list):
        for item in value:
            validate_normative(item)
        return
    if not isinstance(value, dict):
        return
    for key, item in value.items():
        if key in FORBIDDEN_FIELDS:
            raise PitError("FREE_FORM_FIELD")
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
        validate_normative(item)
    if "outcome_kind" in value and value.get("slot_status") != OUTCOME_STATUS[value["outcome_kind"]]:
        raise PitError("OUTCOME_STATUS_MISMATCH")


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


def borrowable_binance(base: str, assets: Any, pairs: Any) -> bool | None:
    try:
        asset = _unique(assets, lambda row: row.get("assetName") == base)
        pair = _unique(pairs, lambda row: row.get("base") == base and row.get("quote") == "USDT")
        if asset is not None and type(asset.get("isBorrowable")) is not bool:
            raise PitError("SCHEMA_FAILURE")
        for field in ("isMarginTrade", "isSellAllowed"):
            if pair is not None and type(pair.get(field)) is not bool:
                raise PitError("SCHEMA_FAILURE")
        return bool(asset and pair and asset["isBorrowable"] and pair["isMarginTrade"] and pair["isSellAllowed"])
    except PitError:
        return None


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


def spread_bps(bid: Any, ask: Any) -> str:
    b = parse_decimal_string(bid, positive=True)
    a = parse_decimal_string(ask, positive=True)
    if b > a:
        raise PitError("SCHEMA_FAILURE")
    scale = max(-b.as_tuple().exponent, -a.as_tuple().exponent, 0)
    factor = Decimal(10) ** scale
    B, A = int(b * factor), int(a * factor)
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
    try:
        bn_instruments = _rows(sources["BN_FUT_EXCHANGE_INFO"], "symbols")
        by_instruments = []
        for page in sources["BY_LINEAR_INSTRUMENTS"]:
            by_instruments.extend(_rows(page, "result", "list"))
        bn_premium = _rows(sources["BN_FUT_PREMIUM_INDEX"])
        bn_books = _rows(sources["BN_FUT_BOOK_TICKER"])
        bn_assets = _rows(sources["BN_MARGIN_ASSETS"])
        bn_pairs = _rows(sources["BN_MARGIN_PAIRS"])
        by_spots = _rows(sources["BY_SPOT_INSTRUMENTS"], "result", "list")
        by_currencies = _rows(sources["BY_MARGIN_BORROWABLE"], "result", "list")
    except (KeyError, TypeError, PitError):
        return {
            "outcome_kind": "SNAPSHOT_PARTIAL",
            "reason_codes": ["SCHEMA_FAILURE"],
            "slot_status": "PARTIAL",
        }
    decisions, assets, reason_codes = symbol_decisions(rows), [], []
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
                product = perp_decision(venue, base, bn_instruments)
                borrowable = borrowable_binance(base, bn_assets, bn_pairs)
            else:
                product = perp_decision(venue, base, by_instruments)
                borrowable = borrowable_bybit(base, by_currencies, by_spots)
            empty["borrowable"] = borrowable
            empty["perp_exists"] = product["perp_exists"]
            empty["missing_reasons"] = list(product["missing_reasons"])
            if borrowable is None and product["perp_exists"] is not None:
                empty["missing_reasons"] = ordered(empty["missing_reasons"] + ["SCHEMA_FAILURE"], MISSING_REASON_ORDER)
                reason_codes.append("SCHEMA_FAILURE")
            if product["perp_exists"] is True:
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
                except PitError:
                    empty["missing_reasons"] = ordered(empty["missing_reasons"] + ["SCHEMA_FAILURE"], MISSING_REASON_ORDER)
                    reason_codes.append("SCHEMA_FAILURE")
            elif product["perp_exists"] is None:
                reason_codes.append("QA_FAILURE" if "QA_FAILURE" in product["missing_reasons"] else "SCHEMA_FAILURE")
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
    snapshot = {
        "I_impl": value["I_impl"],
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
        "outcome_kind": kind,
        "qa": {"qa_status": "QA_OK" if not reason_codes else "QA_FAILURE", "reason_codes": reason_codes},
        "reason_codes": reason_codes,
        "slot_status": OUTCOME_STATUS[kind],
        "universe_raw_sha256": sha256(raw_universe),
        "universe_source_id": "CG_TOP250",
    }
    validate_snapshot(snapshot)
    return snapshot


def validate_snapshot(snapshot: dict[str, Any]) -> None:
    validate_normative(snapshot)
    if snapshot.get("contract_id") != CONTRACT_ID or snapshot.get("epoch_id") != EPOCH_ID:
        raise PitError("IDENTITY_MISMATCH")
    if set(snapshot.get("qa", {})) != {"qa_status", "reason_codes"}:
        raise PitError("INVALID_QA_SHAPE")
    if snapshot["qa"]["reason_codes"] != snapshot.get("reason_codes"):
        raise PitError("QA_REASON_MISMATCH")
    assets = snapshot.get("assets")
    if not isinstance(assets, list) or len(assets) != 250:
        raise PitError("INVALID_ASSET_COUNT")
    if [asset.get("universe_rank") for asset in assets] != list(range(1, 251)):
        raise PitError("INVALID_RANKS")
    ids = [asset.get("coingecko_id") for asset in assets]
    if any(not isinstance(item, str) or not item for item in ids) or len(ids) != len(set(ids)):
        raise PitError("INVALID_IDS")
    for asset in assets:
        venues = asset.get("venues")
        if not isinstance(venues, list) or [row.get("venue") for row in venues] != list(VENUES):
            raise PitError("INVALID_VENUES")
        for row in venues:
            required = {
                "venue", "mapping_status", "exchange_symbol", "perp_exists",
                "funding_rate", "funding_observed_at_utc", "bid_price", "ask_price",
                "spread_bps", "mark_price", "index_price", "borrowable",
                "missing_reasons", "source_raw_sha256s",
            }
            if not required.issubset(row):
                raise PitError("MISSING_VENUE_FIELD")


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
) -> tuple[int, bytes, int]:
    for attempt in range(1, 4):
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


def artifact_path(relative: str) -> str:
    if (
        not isinstance(relative, str)
        or "\\" in relative
        or relative.startswith("/")
        or ".." in relative.split("/")
        or not relative.startswith(PREFIX)
        or "v4" in relative.lower()
    ):
        raise PitError("PATH_NOT_ALLOWED")
    return relative


def ensure_impl_boundary(pinned: str, current: str, epoch_id: str, h0: str) -> None:
    if pinned != current:
        raise PitError("NEW_EPOCH_AND_H0_REQUIRED")
    if epoch_id != EPOCH_ID or not h0:
        raise PitError("INVALID_EPOCH_BOUNDARY")


def make_claim(slot: str, i_impl: str, writer: str, parent: str) -> dict[str, Any]:
    validate_utc(slot)
    key = EPOCH_ID + "|" + slot
    filesafe = slot.replace("-", "").replace(":", "").replace(".", "")
    return {
        "relative_path": artifact_path(PREFIX + "claims/" + filesafe + ".json"),
        "value": {
            "I_impl": i_impl,
            "attempt_id": sha256(key.encode())[:32],
            "attempt_ordinal": 1,
            "authorized_writer_identity": writer,
            "claim_status": "CLAIMED",
            "claimed_at_utc": slot,
            "contract_id": CONTRACT_ID,
            "epoch_id": EPOCH_ID,
            "expected_parent_before_claim": parent,
            "formal_slot_utc": slot,
            "idempotency_key": key,
            "job_id": "fixture-job",
            "log_locator": "fixture-log",
            "workflow_run_id": "fixture-run",
        },
    }


def base_outcome(claim: dict[str, Any], kind: str, reason_codes: Iterable[str], raw: list[tuple[str, bytes]]) -> dict[str, Any]:
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
        "job_id": value["job_id"],
        "log_locator": value["log_locator"],
        "log_sha256": sha256(b"fixture-log"),
        "materialized_at_utc": value["formal_slot_utc"],
        "outcome_kind": kind,
        "previous_ledger_head": value["expected_parent_before_claim"],
        "reason_codes": ordered(reason_codes, REASON_CODE_ORDER),
        "slot_status": OUTCOME_STATUS[kind],
        "source_manifest_sha256s": [],
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
        validate_normative(claim["value"])
        self.claims[key] = claim
        self._write("claim:" + key)
        return "CLAIMED"

    def archive_raw(self, key: str, source_id: str, body: bytes) -> str:
        if key not in self.claims:
            raise PitError("CLAIM_REQUIRED_BEFORE_REQUEST")
        path = artifact_path(PREFIX + "raw/" + source_id + "-" + str(len(self.raw.get(key, []))) + ".bin")
        self.raw.setdefault(key, []).append((path, body))
        self._write("raw:" + key + ":" + source_id)
        return path

    def parse_after_raw(self, key: str, parser: Callable[[bytes], Any]) -> Any:
        if key not in self.raw or not self.raw[key]:
            raise PitError("RAW_REQUIRED_BEFORE_PARSE")
        self.events.append("parse:" + key)
        return parser(self.raw[key][-1][1])

    def publish_manifest(self, key: str, manifest: dict[str, Any]) -> None:
        if key not in self.raw:
            raise PitError("RAW_REQUIRED_BEFORE_MANIFEST")
        validate_normative(manifest)
        self.manifests.setdefault(key, []).append(manifest)
        self._write("manifest:" + key + ":" + manifest["source_id"])

    def publish_outcome(self, key: str, outcome: dict[str, Any], writer: str, expected_parent: str) -> str:
        if self.writer_stop or writer != self.authorized_writer:
            self.writer_stop = True
            return "EPOCH_WRITER_STOP"
        if expected_parent != self.head:
            return self.cas_reject(key, writer)
        if key in self.outcomes:
            return "DUPLICATE_NO_WRITE"
        validate_normative(outcome)
        self.outcomes[key] = outcome
        self._write("outcome:" + key)
        return "PUBLISHED_SLOT_OUTCOME"

    def cas_reject(self, key: str, observed_writer: str, observed_key: str | None = None) -> str:
        if observed_writer != self.authorized_writer or observed_key not in {None, key}:
            self.writer_stop = True
            return "EPOCH_WRITER_STOP"
        if key in self.outcomes:
            validate_canonical(canonical_bytes(self.outcomes[key]))
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

    def recover(self, claim: dict[str, Any], writer: str, expected_parent: str) -> str:
        key = claim["value"]["idempotency_key"]
        if self.writer_stop:
            return "EPOCH_WRITER_STOP"
        if key in self.outcomes:
            return "DUPLICATE_NO_WRITE"
        if expected_parent != self.head:
            return self.cas_reject(key, writer)
        if key in self.claims:
            raw = self.raw.get(key, [])
            reasons = ["ATTEMPT_ABORTED"] + ([] if raw else ["NO_RAW_DURABLY_PUBLISHED"])
            outcome = base_outcome(self.claims[key], "ABORTED_ATTEMPT", reasons, raw)
        else:
            outcome = base_outcome(claim, "GAP_NO_RUN", ["NO_CLAIM_BEFORE_DEADLINE"], [])
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
    outcome = base_outcome(claim, "SNAPSHOT_COMPLETE", [], ledger.raw[key])
    if ledger.publish_outcome(key, outcome, writer, ledger.head) != "PUBLISHED_SLOT_OUTCOME":
        raise PitError("OUTCOME_NOT_PUBLISHED")
    return ledger, {"parsed": parsed, "outcome": outcome}


def acquire_fixture_sources(
    ledger: Ledger,
    claim: dict[str, Any],
    fetch: Callable[[str, str], tuple[int, bytes, dict[str, str]]],
) -> dict[str, Any]:
    """Offline producer: frozen order, raw-before-parse, manifest-before-next."""
    key = claim["value"]["idempotency_key"]
    if key not in ledger.claims:
        raise PitError("CLAIM_REQUIRED_BEFORE_REQUEST")
    parsed: dict[str, Any] = {}
    for source_id in SOURCE_ORDER:
        pages, url = [], URLS[source_id]
        while True:
            validate_source_url(source_id, url)
            ledger.events.append("request:" + key + ":" + source_id)
            status, raw, attempts = retry_request(lambda: fetch(source_id, url), lambda _: None)
            ledger.archive_raw(key, source_id, raw)
            try:
                body = ledger.parse_after_raw(key, json.loads)
                parse_status, qa_status = "PARSE_OK", "QA_OK"
            except (json.JSONDecodeError, TypeError):
                body, parse_status, qa_status = None, "PARSE_FAILURE", "QA_NOT_RUN"
            manifest = {
                "I_impl": claim["value"]["I_impl"],
                "attempt_count": attempts,
                "auth_class": "READ_ONLY_MARKET_DATA" if source_id in READ_ONLY_SOURCES else "PUBLIC",
                "canonical_url_without_secret": url,
                "error_record_relative_path": None,
                "error_record_sha256": None,
                "fetched_at_utc": claim["value"]["claimed_at_utc"],
                "http_status": status,
                "method": "GET",
                "page_ordinal": len(pages),
                "parse_status": parse_status,
                "qa_status": qa_status,
                "raw_bytes": len(raw),
                "raw_relative_path": ledger.raw[key][-1][0],
                "raw_sha256": sha256(raw),
                "requested_at_utc": claim["value"]["claimed_at_utc"],
                "server_time_utc": None,
                "source_id": source_id,
                "source_status": "SOURCE_OK",
            }
            ledger.publish_manifest(key, manifest)
            pages.append(body)
            if source_id != "BY_LINEAR_INSTRUMENTS":
                parsed[source_id] = body
                break
            try:
                cursor = body["result"].get("nextPageCursor", "")
            except (KeyError, AttributeError, TypeError):
                raise PitError("SCHEMA_FAILURE")
            if not cursor:
                parsed[source_id] = pages
                break
            if not isinstance(cursor, str):
                raise PitError("SCHEMA_FAILURE")
            url = URLS[source_id] + "&cursor=" + urllib.parse.quote(cursor, safe="")
    return parsed


def require_live_authorization(env: dict[str, str]) -> None:
    required = {
        "PIT_ACTIVATION_APPROVED": "YES",
        "PIT_TARGET_WRITE_APPROVED": "YES",
        "PIT_SECRET_APPROVED": "YES",
        "PIT_KEY_PERMISSION_PROOF": "READ_ONLY_MARKET_DATA_NO_TRADE_BORROW_TRANSFER_WITHDRAW",
        "PIT_AUTHORIZED_WRITER": "",
        "PIT_I_IMPL": "",
        "PIT_H0": "",
        "BINANCE_MARKET_DATA_API_KEY": "",
    }
    for name, exact in required.items():
        value = env.get(name, "")
        if not value or exact and value != exact:
            raise PitError("STOP_PERMISSION_REQUIRED")
    ensure_impl_boundary(env["PIT_I_IMPL"], env["PIT_I_IMPL_CURRENT"], EPOCH_ID, env["PIT_H0"])


def _live_send(source_id: str, url: str, api_key: str) -> tuple[int, bytes, dict[str, str]]:
    validate_source_url(source_id, url)
    headers = {}
    if source_id in READ_ONLY_SOURCES:
        if not api_key:
            raise PitError("STOP_PERMISSION_REQUIRED")
        headers["X-MBX-APIKEY"] = api_key
    request = urllib.request.Request(url, method="GET", headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, response.read(), dict(response.headers)
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read(), dict(exc.headers)


def live_capture() -> None:
    require_live_authorization(dict(os.environ))
    raise PitError("STOP_PERMISSION_REQUIRED")  # activation also requires audited H_stage/I_impl decision


def oracle_objects() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
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
    )


def self_check() -> None:
    expected = (
        (242, "BB6A1F4C2A99D23E31C79809A1D25A38720A004831C34FE168EC681788EC2165"),
        (333, "8D13A204BDC833121A88F3C531B74A677987E0CE8A96C5610C9DA8C10CA2300D"),
        (257, "96701B35D8FC21BB3BD17BD3F27BA492E82C0959AEEEC9CB31BDDD58B7575CA4"),
    )
    for value, oracle in zip(oracle_objects(), expected):
        data = canonical_bytes(value)
        assert (len(data), sha256(data)) == oracle
        assert validate_canonical(data) == value
    assert spread_bps("99", "100") == "100.50251256"
    try:
        require_live_authorization({})
    except PitError as exc:
        assert str(exc) == "STOP_PERMISSION_REQUIRED"
    else:
        raise AssertionError("live boundary opened")
    print("SELF_CHECK PASS")


def main(argv: list[str]) -> int:
    if argv == ["self-check"]:
        self_check()
        return 0
    if argv in (["workflow"], ["capture"]):
        live_capture()
        return 0
    print("usage: pit_ledger.py self-check|workflow", file=sys.stderr)
    return 2


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except PitError as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1)
