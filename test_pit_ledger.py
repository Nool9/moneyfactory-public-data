import base64
import hashlib
import io
import json
import os
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from datetime import datetime, timezone
from decimal import Decimal, localcontext
from pathlib import Path

import pit_ledger as p


def universe(volumes=None, symbols=None):
    volumes = volumes or [str(250 - i) for i in range(250)]
    symbols = symbols or [f"x{i}" for i in range(250)]
    rows = []
    for i, (volume, symbol) in enumerate(zip(volumes, symbols)):
        rows.append(
            '{"current_price":"1","id":"id%d","market_cap_rank":%d,"symbol":"%s","total_volume":%s}'
            % (i, i + 1, symbol, volume)
        )
    return ("[" + ",".join(rows) + "]").encode()


def binance_instrument(base="BTC"):
    return {
        "symbol": base + "USDT",
        "baseAsset": base,
        "quoteAsset": "USDT",
        "contractType": "PERPETUAL",
        "status": "TRADING",
    }


def bybit_instrument(base="BTC"):
    return {
        "symbol": base + "USDT",
        "baseCoin": base,
        "quoteCoin": "USDT",
        "settleCoin": "USDT",
        "contractType": "LinearPerpetual",
        "status": "Trading",
    }


def complete_snapshot_fixture(bybit_page_count=1, binance_case=None, failed_source=None, parent="H0", writer="writer", bybit_borrowable_body=None):
    slot = "2026-07-26T20:00:00.000Z"
    ledger = p.Ledger(writer, head=parent)
    claim = p.make_claim(slot, "A" * 64, writer, ledger.head)
    ledger.claim(claim, writer, ledger.head)
    bodies = {
        "CG_TOP250": universe(), "BN_FUT_EXCHANGE_INFO": b'{"symbols":[]}',
        "BN_FUT_PREMIUM_INDEX": b"[]", "BN_FUT_BOOK_TICKER": b"[]",
        "BY_LINEAR_TICKERS": b'{"result":{"list":[]},"time":1720000000123}',
        "BY_SPOT_INSTRUMENTS": b'{"result":{"list":[]}}',
        "BY_MARGIN_BORROWABLE": b'{"result":{"vipCoinList":[{"vipLevel":"No VIP","list":[]}]}}',
    }
    if bybit_borrowable_body is not None:
        bodies["BY_MARGIN_BORROWABLE"] = bybit_borrowable_body
    if binance_case in {"missing_ticker", "public_perp"}:
        bodies["BN_FUT_EXCHANGE_INFO"] = json.dumps(
            {"symbols": [binance_instrument("X0")]}, separators=(",", ":")
        ).encode()
    if binance_case == "public_perp":
        bodies.update(
            BN_FUT_PREMIUM_INDEX=b'[{"indexPrice":"100","lastFundingRate":"0.001","markPrice":"100","symbol":"X0USDT","time":1720000000123}]',
            BN_FUT_BOOK_TICKER=b'[{"askPrice":"100","bidPrice":"99","symbol":"X0USDT"}]',
        )
    bybit_pages = [
        b'{"result":{"list":[],"nextPageCursor":"next"}}',
        b'{"result":{"list":[],"nextPageCursor":""}}',
    ] if bybit_page_count == 2 else [b'{"result":{"list":[],"nextPageCursor":""}}']

    def fetch(source_id, url):
        if source_id == failed_source:
            return 500, b"", {}
        if source_id == "BY_LINEAR_INSTRUMENTS":
            return 200, bybit_pages[1 if "&cursor=" in url else 0], {}
        return 200, bodies[source_id], {}

    parsed = p.acquire_fixture_sources(ledger, claim, fetch)
    snapshot = parsed.get("__outcome__") or p.build_snapshot(bodies["CG_TOP250"], parsed, claim)
    manifests = parsed["__source_manifests__"]
    manifest_map = {
        p.slot_artifact(
            slot, "source-manifests", item["source_id"] + "-" + str(item["page_ordinal"]) + ".json"
        ): item
        for item in manifests
    }
    context = p.GitContext(
        "H0", "HEAD", claim["value"]["idempotency_key"], claim["value"],
        p.canonical_bytes(claim["value"]), dict(ledger.raw[claim["value"]["idempotency_key"]]),
        manifest_map, p.run_log_bytes(claim), 0, True,
    )
    return claim, manifests, snapshot, context


class CanonicalTests(unittest.TestCase):
    def test_global_json_jsonl_and_artifact_type_oracles(self):
        fixtures = {
            "claim": {"claim_status": "CLAIMED", "n": 1},
            "error": {"error_class": "SCHEMA_FAILURE", "source_id": "CG_TOP250"},
            "source_manifest": {
                "method": "GET",
                "parse_status": "PARSE_OK",
                "qa_status": "QA_OK",
                "source_id": "CG_TOP250",
                "source_status": "SOURCE_OK",
            },
            "slot_manifest": {"reason_codes": [], "slot_status": "COMPLETE"},
            "gap": {
                "outcome_kind": "GAP_NO_RUN",
                "reason_codes": ["NO_CLAIM_BEFORE_DEADLINE"],
                "slot_status": "GAP_NO_RUN",
            },
            "outcome": {
                "outcome_kind": "SNAPSHOT_COMPLETE",
                "reason_codes": [],
                "slot_status": "COMPLETE",
            },
            "qa": {"qa_status": "QA_OK", "reason_codes": []},
            "checkpoint": {"ledger_seq": 1},
            "ledger_index": {"ledger_seq": 1},
        }
        for name, value in fixtures.items():
            expected = (json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":")) + "\n").encode()
            actual = p.canonical_bytes(value)
            self.assertEqual(actual, expected, name)
            self.assertEqual(p.validate_canonical(actual), value)
            self.assertEqual(len(p.sha256(actual)), 64)
            self.assertEqual(p.sha256(actual), p.sha256(actual).upper())
        rows = [{"ledger_seq": 1}, {"ledger_seq": 2}]
        self.assertEqual(p.canonical_jsonl(rows), b'{"ledger_seq":1}\n{"ledger_seq":2}\n')
        with self.assertRaises(p.PitError):
            p.canonical_jsonl([])

    def test_noncanonical_scalars_freeform_and_bytes_rejected(self):
        for value in ({"x": 1.0}, {"x": Decimal("1")}, {"diagnostic": "x"}):
            with self.assertRaises(p.PitError):
                p.canonical_bytes(value)
        for raw in (b'{"x":1}\r\n', b'{"x": 1}\n', b"\xef\xbb\xbf{}\n", b"{}"):
            with self.assertRaises(p.PitError):
                p.validate_canonical(raw)

    def test_closed_enums_statuses_tristate_http_and_reasons(self):
        valid = {
            "auth_class": "PUBLIC",
            "borrowable": None,
            "http_status": 200,
            "mapping_status": "MAPPED",
            "method": "GET",
            "missing_reasons": ["AMBIGUOUS_EXCHANGE_PRODUCT", "QA_FAILURE"],
            "outcome_kind": "SNAPSHOT_PARTIAL",
            "parse_status": "PARSE_OK",
            "perp_exists": True,
            "qa_status": "QA_OK",
            "reason_codes": ["SCHEMA_FAILURE", "QA_FAILURE"],
            "run_status": "PUBLISHED_SLOT_OUTCOME",
            "slot_status": "PARTIAL",
            "source_id": "CG_TOP250",
            "source_status": "SOURCE_OK",
            "venue": "BINANCE_USDM",
        }
        p.validate_normative(valid)
        bad = [
            {"method": "POST"},
            {"source_id": "OTHER"},
            {"mapping_status": "MAYBE"},
            {"borrowable": 1},
            {"http_status": "200"},
            {"http_status": 600},
            {"outcome_kind": "SNAPSHOT_PARTIAL", "slot_status": "COMPLETE"},
            {"missing_reasons": ["QA_FAILURE", "SCHEMA_FAILURE"]},
            {"reason_codes": ["QA_FAILURE", "QA_FAILURE"]},
            {"reason_codes": ["NOT_OBSERVED_PUBLIC_ONLY"]},
            {"auth_class": "READ_ONLY"},
            {"reason_codes": ["OTHER"]},
        ]
        for value in bad:
            with self.assertRaises(p.PitError, msg=value):
                p.validate_normative(value)

    def test_reason_dedupe_has_frozen_order(self):
        self.assertEqual(
            p.ordered(["QA_FAILURE", "SCHEMA_FAILURE", "QA_FAILURE"], p.MISSING_REASON_ORDER),
            ["SCHEMA_FAILURE", "QA_FAILURE"],
        )
        self.assertEqual(
            p.ordered(["NO_RAW_DURABLY_PUBLISHED", "ATTEMPT_ABORTED"], p.REASON_CODE_ORDER),
            ["ATTEMPT_ABORTED", "NO_RAW_DURABLY_PUBLISHED"],
        )
        self.assertEqual(
            p.ordered(["SOURCE_FAILURE", "NOT_OBSERVED_PUBLIC_ONLY"], p.MISSING_REASON_ORDER),
            ["NOT_OBSERVED_PUBLIC_ONLY", "SOURCE_FAILURE"],
        )

    def test_r4_and_public_only_exact_byte_oracles(self):
        expected = (
            (242, "BB6A1F4C2A99D23E31C79809A1D25A38720A004831C34FE168EC681788EC2165"),
            (333, "8D13A204BDC833121A88F3C531B74A677987E0CE8A96C5610C9DA8C10CA2300D"),
            (257, "96701B35D8FC21BB3BD17BD3F27BA492E82C0959AEEEC9CB31BDDD58B7575CA4"),
            (179, "C467F918C0C5948E83DD95484A4502306F8344CA0BAC8507946D73E1033035FE"),
            (244, "C92039EC6CDBCB43F6210450CE969B230252AA700577FE77C8FF2D9149F4640A"),
            (244, "4BC9C82E097C5348587C773DC0D6EA18C08D3A68A4FCAC1146A3A44BAD0AD8B6"),
        )
        for value, oracle in zip(p.oracle_objects(), expected):
            data = p.canonical_bytes(value)
            self.assertEqual((len(data), p.sha256(data)), oracle)
            self.assertEqual(p.validate_canonical(data), value)
        r301 = p.oracle_objects()[2]
        for reasons in (["AMBIGUOUS_EXCHANGE_PRODUCT"], ["AMBIGUOUS_EXCHANGE_PRODUCT", "QA_FAILURE", "SCHEMA_FAILURE"]):
            bad = dict(r301, missing_reasons=reasons)
            with self.assertRaises(p.PitError):
                p.canonical_bytes(bad)

    def test_contextual_array_hash_and_jsonl_witnesses(self):
        with self.assertRaises(p.PitError):
            p.canonical_jsonl([{"ledger_seq": 2}, {"ledger_seq": 1}])
        for value in (
            {"raw_sha256": "a" * 64},
            {"x": [2, 1]},
            [2, 1],
            {"x": [[2, 1]]},
            {"files": [{"path": "z"}, {"path": "a"}]},
            {"available_raw_sha256s": ["B" * 64, "A" * 64]},
        ):
            with self.assertRaises(p.PitError, msg=value):
                p.canonical_bytes(value)


class UniverseAndMappingTests(unittest.TestCase):
    def test_exact_decimal_universe_null_equal_exponent_and_beyond_float(self):
        values = ["1e1000", "1e1000", "null", "9007199254740993", "9007199254740992"]
        values += [str(245 - i) for i in range(245)]
        rows, gap = p.parse_universe(universe(values))
        self.assertIsNone(gap)
        self.assertEqual(len(rows), 250)
        self.assertEqual(rows[0]["total_volume"], Decimal("1e1000"))
        self.assertEqual(rows[3]["total_volume"], Decimal("9007199254740993"))

    def test_universe_requires_250_unique_ids_and_fields(self):
        rows, gap = p.parse_universe(b"[]")
        self.assertIsNone(rows)
        self.assertEqual(gap["reason_codes"], ["SCHEMA_FAILURE"])
        raw = universe().replace(b'"id":"id1"', b'"id":"id0"', 1)
        self.assertEqual(p.parse_universe(raw)[1]["reason_codes"], ["SCHEMA_FAILURE"])
        raw = universe().replace(b',"total_volume":250', b"", 1)
        self.assertEqual(p.parse_universe(raw)[1]["outcome_kind"], "GAP_UNIVERSE")

    def test_universe_wrong_type_invalid_json_and_inversion_total_map(self):
        wrong = universe().replace(b'"total_volume":250', b'"total_volume":"250"', 1)
        self.assertEqual(p.parse_universe(wrong)[1]["reason_codes"], ["SCHEMA_FAILURE"])
        self.assertEqual(p.parse_universe(b"{")[1]["reason_codes"], ["PARSE_FAILURE"])
        inverted = ["100", "1", "50"] + [str(247 - i) for i in range(247)]
        gap = p.parse_universe(universe(inverted))[1]
        self.assertEqual(gap["reason_codes"], ["QA_FAILURE"])
        self.assertEqual(gap["outcome_kind"], "GAP_UNIVERSE")
        self.assertEqual(p.sha256(p.canonical_bytes(gap)), "BB6A1F4C2A99D23E31C79809A1D25A38720A004831C34FE168EC681788EC2165")
        invalid_number = universe().replace(b'"total_volume":250', b'"total_volume":NaN', 1)
        self.assertEqual(p.parse_universe(invalid_number)[1]["reason_codes"], ["SCHEMA_FAILURE"])

    def test_ascii_symbol_duplicates_invalid_and_unique(self):
        rows = [{"symbol": "abc"}, {"symbol": "ABC"}, {"symbol": "eth"}, {"symbol": "x-y"}]
        decisions = p.symbol_decisions(rows)
        self.assertEqual([x["mapping_status"] for x in decisions], ["AMBIGUOUS", "AMBIGUOUS", "MAPPED", "UNMAPPABLE"])
        self.assertEqual(decisions[2]["exchange_symbol"], "ETHUSDT")
        self.assertEqual(decisions[3]["missing_reasons"], ["INVALID_SYMBOL_FORMAT"])

    def test_perp_table_binance_bybit_zero_one_many_and_incomplete(self):
        for venue, row in (("BINANCE_USDM", binance_instrument()), ("BYBIT_LINEAR", bybit_instrument())):
            self.assertTrue(p.perp_decision(venue, "BTC", [row])["perp_exists"])
            self.assertFalse(p.perp_decision(venue, "BTC", [])["perp_exists"])
            self.assertEqual(p.perp_decision(venue, "BTC", [])["missing_reasons"], ["NOT_APPLICABLE_NO_PERP"])
            many = p.perp_decision(venue, "BTC", [row, dict(row)])
            self.assertIsNone(many["perp_exists"])
            self.assertEqual(many["missing_reasons"], ["AMBIGUOUS_EXCHANGE_PRODUCT", "QA_FAILURE"])
            self.assertEqual(many["outcome_kind"], "SNAPSHOT_PARTIAL")
            self.assertIsNone(p.perp_decision(venue, "BTC", [], False)["perp_exists"])

    def test_binance_mapped_borrow_is_public_only_null_and_complete(self):
        _, _, snapshot, _ = complete_snapshot_fixture(binance_case="public_perp")
        row = snapshot["assets"][0]["venues"][0]
        self.assertIs(row["perp_exists"], True)
        self.assertIsNone(row["borrowable"])
        self.assertEqual(row["missing_reasons"], ["NOT_OBSERVED_PUBLIC_ONLY"])
        self.assertEqual(
            (snapshot["outcome_kind"], snapshot["reason_codes"], snapshot["qa"]),
            ("SNAPSHOT_COMPLETE", [], {"qa_status": "QA_OK", "reason_codes": []}),
        )
        for invalid in (False, 0):
            broken = json.loads(json.dumps(snapshot))
            broken["assets"][0]["venues"][0]["borrowable"] = invalid
            with self.assertRaises(p.PitError):
                p.validate_snapshot(broken)

    def test_bybit_borrow_envelope_and_r4_truth_table(self):
        body = {
            "result": {"vipCoinList": [{"vipLevel": "No VIP", "list": [
                {"currency": "BTC", "borrowable": True},
            ]}]},
        }
        p.validate_source_schema("BY_MARGIN_BORROWABLE", body)
        self.assertEqual(p._bybit_borrowable_rows(body), body["result"]["vipCoinList"][0]["list"])
        claim, manifests, snapshot, context = complete_snapshot_fixture(
            bybit_borrowable_body=json.dumps(body, separators=(",", ":")).encode()
        )
        p.validate_snapshot(snapshot, claim, manifests, context, False, "A" * 64)
        for invalid in (
            {"result": {"vipCoinList": []}},
            {"result": {"vipCoinList": [{"vipLevel": "No VIP", "list": []}, {"vipLevel": "No VIP", "list": []}]}},
            {"result": {"vipCoinList": [{"vipLevel": "VIP 1", "list": []}]}},
            {"result": {"vipCoinList": [{"vipLevel": "No VIP", "list": [{"currency": "BTC", "borrowable": 1}]}]}},
        ):
            with self.subTest(invalid=invalid):
                with self.assertRaises(p.PitError):
                    p.validate_source_schema("BY_MARGIN_BORROWABLE", invalid)
        _, manifests, snapshot, _ = complete_snapshot_fixture(bybit_borrowable_body=b"{")
        borrow_manifest = next(item for item in manifests if item["source_id"] == "BY_MARGIN_BORROWABLE")
        self.assertEqual((borrow_manifest["source_status"], borrow_manifest["parse_status"], snapshot["reason_codes"]), ("SOURCE_OK", "PARSE_FAILURE", ["PARSE_FAILURE"]))

        currencies = [{"currency": "BTC", "borrowable": True}]
        expected = {"none": False, "normalSpotOnly": False, "utaOnly": True, "both": True}
        for enum, result in expected.items():
            spots = [{"baseCoin": "BTC", "quoteCoin": "USDT", "marginTrading": enum}]
            self.assertIs(p.borrowable_bybit("BTC", currencies, spots), result)
        self.assertIs(p.borrowable_bybit("BTC", [], [{"baseCoin": "BTC", "quoteCoin": "USDT", "marginTrading": "both"}]), False)
        self.assertIs(p.borrowable_bybit("BTC", currencies, []), False)
        self.assertIsNone(p.borrowable_bybit("BTC", currencies * 2, [{"baseCoin": "BTC", "quoteCoin": "USDT", "marginTrading": "both"}]))
        self.assertIsNone(p.borrowable_bybit("BTC", currencies, [{"baseCoin": "BTC", "quoteCoin": "USDT", "marginTrading": "both"}] * 2))
        self.assertIsNone(p.borrowable_bybit("BTC", currencies, [{"baseCoin": "BTC", "quoteCoin": "USDT", "marginTrading": "future"}]))

    def test_binance_inactive_book_ticker_is_source_only(self):
        valid = [
            {"symbol": "BTCUSDT", "bidPrice": "99", "askPrice": "100"},
            {"symbol": "BTCUSDT_260626", "bidPrice": "0.0", "askPrice": "0.0"},
        ]
        p.validate_source_schema("BN_FUT_BOOK_TICKER", valid)
        for bid, ask in (("0", "1"), ("1", "0"), ("-1", "1"), ("2", "1")):
            with self.subTest(bid=bid, ask=ask):
                with self.assertRaises(p.PitError):
                    p.validate_source_schema("BN_FUT_BOOK_TICKER", [{"symbol": "BTCUSDT", "bidPrice": bid, "askPrice": ask}])
        premium = [{"symbol": "BTCUSDT", "lastFundingRate": "0.001", "markPrice": "100", "indexPrice": "100", "time": 1720000000123}]
        with self.assertRaises(p.PitError):
            p._ticker_record("BINANCE_USDM", "BTCUSDT", premium, [{"symbol": "BTCUSDT", "bidPrice": "0", "askPrice": "0"}], None)

    def test_bybit_empty_funding_is_source_only(self):
        row = {"symbol": "BTCUSDT", "fundingRate": "", "bid1Price": "99", "ask1Price": "100", "markPrice": "100", "indexPrice": "100"}
        body = {"result": {"list": [row]}, "time": 1720000000123}
        p.validate_source_schema("BY_LINEAR_TICKERS", body)
        malformed = dict(row, fundingRate="not-a-decimal")
        with self.assertRaises(p.PitError):
            p.validate_source_schema("BY_LINEAR_TICKERS", {"result": {"list": [malformed]}, "time": 1720000000123})
        with self.assertRaises(p.PitError):
            p._ticker_record("BYBIT_LINEAR", "BTCUSDT", None, None, body)

    def test_full_250_asset_500_venue_snapshot_and_validator(self):
        writer, slot = "writer", "2026-07-26T20:00:00.000Z"
        ledger = p.Ledger(writer)
        claim = p.make_claim(slot, "A" * 64, writer, ledger.head)
        ledger.claim(claim, writer, ledger.head)
        raw = universe()
        bodies = {
            "CG_TOP250": raw,
            "BN_FUT_EXCHANGE_INFO": b'{"symbols":[]}',
            "BN_FUT_PREMIUM_INDEX": b"[]",
            "BN_FUT_BOOK_TICKER": b"[]",
            "BY_LINEAR_TICKERS": b'{"result":{"list":[]},"time":1720000000123}',
            "BY_SPOT_INSTRUMENTS": b'{"result":{"list":[]}}',
            "BY_MARGIN_BORROWABLE": b'{"result":{"vipCoinList":[{"vipLevel":"No VIP","list":[]}]}}',
        }
        pages = [b'{"result":{"list":[],"nextPageCursor":"next"}}', b'{"result":{"list":[],"nextPageCursor":""}}']
        def fetch(source, url):
            body = pages.pop(0) if source == "BY_LINEAR_INSTRUMENTS" else bodies[source]
            return 200, body, {}
        parsed = p.acquire_fixture_sources(ledger, claim, fetch)
        kinds = [event.split(":", 1)[0] for event in ledger.events]
        self.assertEqual(kinds[0], "claim")
        self.assertEqual(kinds[1:], ["request", "raw", "parse", "manifest"] * 4 + ["request", "raw", "parse", "manifest"] * 2 + ["request", "raw", "parse", "manifest"] * 3)
        request_sources = [event.rsplit(":", 1)[-1] for event in ledger.events if event.startswith("request:")]
        self.assertEqual(request_sources, list(p.SOURCE_ORDER[:4]) + ["BY_LINEAR_INSTRUMENTS"] * 2 + list(p.SOURCE_ORDER[5:]))
        self.assertEqual({item["auth_class"] for item in parsed["__source_manifests__"]}, {"PUBLIC"})
        snapshot = p.build_snapshot(raw, parsed, claim)
        p.validate_snapshot(snapshot)
        self.assertEqual(snapshot["outcome_kind"], "SNAPSHOT_COMPLETE")
        self.assertEqual(len(snapshot["assets"]), 250)
        self.assertEqual(sum(len(asset["venues"]) for asset in snapshot["assets"]), 500)
        self.assertEqual([row["venue"] for row in snapshot["assets"][0]["venues"]], list(p.VENUES))
        self.assertIsNone(snapshot["assets"][0]["venues"][0]["borrowable"])
        self.assertEqual(
            snapshot["assets"][0]["venues"][0]["missing_reasons"],
            ["NOT_APPLICABLE_NO_PERP", "NOT_OBSERVED_PUBLIC_ONLY"],
        )
        broken = dict(snapshot, assets=snapshot["assets"][:-1])
        with self.assertRaises(p.PitError):
            p.validate_snapshot(broken)


class DerivationAndSourceTests(unittest.TestCase):
    def test_spread_exact_periodic_oracle_and_invalid_inputs(self):
        self.assertEqual(p.spread_bps("99", "100"), "100.50251256")
        self.assertEqual(p.spread_bps("1.00", "1.00"), "0.00000000")
        for bid, ask in (("0", "1"), ("2", "1"), ("1e0", "2"), (1, "2")):
            with self.assertRaises(p.PitError):
                p.spread_bps(bid, ask)
        with localcontext() as context:
            context.prec = 6
            self.assertEqual(
                p.spread_bps("397999999999900000000000000000000", "402000000000100000000000000000001"),
                "100.00000001",
            )

    def test_funding_timestamp_binance_row_and_bybit_top_level(self):
        stamp = 1720000000123
        self.assertEqual(
            p.funding_observation("BN_FUT_PREMIUM_INDEX", {"time": stamp, "lastFundingRate": "0.001"}, {}),
            ("0.001", "2024-07-03T09:46:40.123Z"),
        )
        self.assertEqual(
            p.funding_observation("BY_LINEAR_TICKERS", {"fundingRate": "-0.001"}, {"time": stamp}),
            ("-0.001", "2024-07-03T09:46:40.123Z"),
        )
        for stamp in (None, "1", -1, p.MAX_UNIX_MS + 1, True):
            with self.assertRaises(p.PitError):
                p.funding_observation("BN_FUT_PREMIUM_INDEX", {"time": stamp, "lastFundingRate": "0.1"}, {})
        self.assertEqual(p.funding_schema_failure("BY_LINEAR_TICKERS")["funding_observed_at_utc"], None)

    def test_endpoint_method_auth_allowlist_and_no_v4(self):
        self.assertEqual(p.SOURCE_ORDER, (
            "CG_TOP250", "BN_FUT_EXCHANGE_INFO", "BN_FUT_PREMIUM_INDEX",
            "BN_FUT_BOOK_TICKER", "BY_LINEAR_INSTRUMENTS", "BY_LINEAR_TICKERS",
            "BY_SPOT_INSTRUMENTS", "BY_MARGIN_BORROWABLE",
        ))
        for source, url in p.URLS.items():
            p.validate_source_url(source, url)
            self.assertNotIn("v4", url.lower())
        for source, url in (("CG_TOP250", p.URLS["CG_TOP250"] + "&page=2"), ("BY_LINEAR_TICKERS", "http://api.bybit.com/")):
            with self.assertRaises(p.PitError):
                p.validate_source_url(source, url)
        self.assertEqual(set(p.SOURCE_ORDER), set(p.URLS))
        self.assertEqual(p.ENUMS["method"], {"GET"})
        self.assertEqual(p.ENUMS["auth_class"], {"PUBLIC"})
        production = (
            Path(p.__file__).read_text()
            + (Path(__file__).parent / "Dockerfile").read_text()
        )
        for forbidden in (
            "BN_MARGIN_ASSETS", "BN_MARGIN_PAIRS", "READ_ONLY_MARKET_DATA",
            "BINANCE_MARKET_DATA_API_KEY", "PIT_KEY_PERMISSION_PROOF",
            "PIT_SECRET_APPROVED", "X-MBX-APIKEY",
        ):
            self.assertNotIn(forbidden, production)

    def test_bybit_cursor_pagination_received_order_complete(self):
        seen = []
        bodies = [
            b'{"result":{"nextPageCursor":"a/b"}}',
            b'{"result":{"nextPageCursor":"second"}}',
            b'{"result":{"nextPageCursor":""}}',
        ]
        def fetch(url):
            seen.append(url)
            return bodies[len(seen) - 1]
        self.assertEqual(p.paginate_bybit(fetch), bodies)
        self.assertEqual(seen[1], p.URLS["BY_LINEAR_INSTRUMENTS"] + "&cursor=a%2Fb")
        self.assertEqual(len(seen), 3)
        repeated = iter((b'{"result":{"nextPageCursor":"x"}}', b'{"result":{"nextPageCursor":"x"}}'))
        with self.assertRaises(p.PitError):
            p.paginate_bybit(lambda _: next(repeated))

    def test_retry_only_transport_429_5xx_no_quality_selection(self):
        calls, sleeps = [], []
        responses = [(500, b"bad", {}), (429, b"busy", {"Retry-After": "99"}), (200, b"first-2xx-even-if-empty", {})]
        def send():
            calls.append(1)
            return responses[len(calls) - 1]
        self.assertEqual(p.retry_request(send, sleeps.append), (200, b"first-2xx-even-if-empty", 3))
        self.assertEqual(sleeps, [1, 60])
        calls.clear()
        self.assertEqual(p.retry_request(lambda: (200, b"low-quality", {}), sleeps.append)[1], b"low-quality")
        self.assertEqual(calls, [])
        with self.assertRaises(p.PitError):
            p.retry_request(lambda: (404, b"", {}), sleeps.append)


class WriterProtocolTests(unittest.TestCase):
    def setUp(self):
        self.writer = "writer"
        self.slot = "2026-07-26T20:00:00.000Z"
        self.ledger = p.Ledger(self.writer)
        self.claim = p.make_claim(self.slot, "A" * 64, self.writer, self.ledger.head)
        self.key = self.claim["value"]["idempotency_key"]

    def test_vertical_claim_request_raw_parse_manifest_outcome_order(self):
        ledger, result = p.fixture_vertical_slice(b'{"ok":true}', json.loads)
        kinds = [event.split(":", 1)[0] for event in ledger.events]
        self.assertEqual(kinds, ["claim", "request", "raw", "parse", "outcome"])
        self.assertEqual(result["parsed"], {"ok": True})
        self.assertEqual(len(ledger.outcomes), 1)
        self.assertEqual(result["outcome"]["available_raw_count"], 1)

    def test_claim_required_before_request_and_raw_before_parse(self):
        with self.assertRaises(p.PitError):
            self.ledger.archive_raw(self.key, "CG_TOP250", b"{}")
        with self.assertRaises(p.PitError):
            self.ledger.parse_after_raw(self.key, json.loads)
        self.assertEqual(self.ledger.claim(self.claim, self.writer, self.ledger.head), "CLAIMED")
        with self.assertRaises(p.PitError):
            self.ledger.parse_after_raw(self.key, json.loads)

    def test_no_claim_recovery_gap_no_run(self):
        result = self.ledger.recover(self.claim, self.writer, self.ledger.head, datetime(2026, 7, 26, 20, 16, tzinfo=timezone.utc))
        self.assertEqual(result, "PUBLISHED_SLOT_OUTCOME")
        outcome = self.ledger.outcomes[self.key]
        self.assertEqual(outcome["outcome_kind"], "GAP_NO_RUN")
        self.assertEqual(outcome["reason_codes"], ["NO_CLAIM_BEFORE_DEADLINE"])

    def test_claim_recovery_aborted_preserves_raw_run_job_log(self):
        self.assertEqual(self.ledger.claim(self.claim, self.writer, self.ledger.head), "CLAIMED")
        self.ledger.archive_raw(self.key, "CG_TOP250", b"raw")
        self.assertEqual(self.ledger.recover(self.claim, self.writer, self.ledger.head, datetime(2026, 7, 26, 20, 16, tzinfo=timezone.utc)), "PUBLISHED_SLOT_OUTCOME")
        outcome = self.ledger.outcomes[self.key]
        self.assertEqual(outcome["outcome_kind"], "ABORTED_ATTEMPT")
        self.assertEqual(outcome["available_raw_count"], 1)
        self.assertEqual(outcome["reason_codes"], ["ATTEMPT_ABORTED"])
        for key in ("workflow_run_id", "job_id", "log_locator", "log_sha256"):
            self.assertIn(key, outcome)

    def test_aborted_without_raw_has_exact_reasons(self):
        self.ledger.claim(self.claim, self.writer, self.ledger.head)
        self.ledger.recover(self.claim, self.writer, self.ledger.head, datetime(2026, 7, 26, 20, 16, tzinfo=timezone.utc))
        self.assertEqual(
            self.ledger.outcomes[self.key]["reason_codes"],
            ["ATTEMPT_ABORTED", "NO_RAW_DURABLY_PUBLISHED"],
        )

    def test_two_runs_exactly_one_outcome_and_verified_duplicate(self):
        self.ledger.claim(self.claim, self.writer, self.ledger.head)
        outcome = p.base_outcome(self.claim, "SNAPSHOT_COMPLETE", [], [], self.ledger.head)
        self.assertEqual(self.ledger.publish_outcome(self.key, outcome, self.writer, self.ledger.head), "PUBLISHED_SLOT_OUTCOME")
        count = len(self.ledger.outcomes)
        self.assertEqual(self.ledger.cas_reject(self.key, self.writer, self.key), "DUPLICATE_NO_WRITE")
        self.assertEqual(len(self.ledger.outcomes), count)
        self.assertEqual(self.ledger.claim(self.claim, self.writer, self.ledger.head), "DUPLICATE_NO_WRITE")
        self.assertEqual(
            self.ledger.cas_reject(self.key, self.writer, self.key, outcome["previous_ledger_head"], b"{}\n", p.sha256(p.canonical_bytes(outcome)), [p.PREFIX + "outcomes/" + p.claim_filesafe(self.slot) + ".json"]),
            "EPOCH_WRITER_STOP",
        )

    def test_cas_claim_held_and_unknown_delta_stops(self):
        self.ledger.claim(self.claim, self.writer, self.ledger.head)
        self.assertEqual(self.ledger.cas_reject(self.key, self.writer, self.key), "CLAIM_HELD_NO_WRITE")
        other = p.make_claim("2026-07-26T20:30:00.000Z", "A" * 64, self.writer, self.ledger.head)
        self.assertEqual(self.ledger.cas_reject(other["value"]["idempotency_key"], self.writer), "EPOCH_WRITER_STOP")
        self.assertTrue(self.ledger.writer_stop)

    def test_foreign_writer_merge_force_or_path_latches_stop_zero_recovery_writes(self):
        cases = [
            ("foreign", 1, [p.PREFIX + "ok.json"], False),
            (self.writer, 2, [p.PREFIX + "ok.json"], False),
            (self.writer, 1, [p.PREFIX + "ok.json"], True),
            (self.writer, 1, ["collector_v4/data.json"], False),
        ]
        for writer, parents, paths, force in cases:
            ledger = p.Ledger(self.writer)
            self.assertEqual(ledger.observe_history(writer, parents, paths, force), "EPOCH_WRITER_STOP")
            self.assertEqual(ledger.recover(self.claim, self.writer, ledger.head, datetime(2026, 7, 26, 20, 16, tzinfo=timezone.utc)), "EPOCH_WRITER_STOP")
            self.assertEqual(ledger.writes_after_stop, 0)
            self.assertEqual(len(ledger.outcomes), 0)

    def test_recovery_before_deadline_and_history_unknown_or_present_stop(self):
        before = self.ledger.head
        self.assertEqual(self.ledger.recover(self.claim, self.writer, before, datetime(2026, 7, 26, 20, 14, tzinfo=timezone.utc)), "EPOCH_WRITER_STOP")
        self.assertEqual(self.ledger.head, before)
        ledger = p.Ledger(self.writer, history_claim_keys={self.key})
        self.assertEqual(ledger.recover(self.claim, self.writer, ledger.head, datetime(2026, 7, 26, 20, 16, tzinfo=timezone.utc)), "EPOCH_WRITER_STOP")
        unknown = p.Ledger(self.writer)
        self.assertEqual(unknown.recover(self.claim, self.writer, unknown.head, datetime(2026, 7, 26, 20, 16, tzinfo=timezone.utc), False), "EPOCH_WRITER_STOP")

    def test_path_allowlist_and_collector_v4_read_boundary(self):
        self.assertEqual(p.artifact_path(p.PREFIX + "claims/x.json"), p.PREFIX + "claims/x.json")
        for path in ("../x", p.PREFIX + "../x", "collector_v4/x", p.PREFIX + "collector_v4.json", p.PREFIX + "x\\y"):
            with self.assertRaises(p.PitError):
                p.artifact_path(path)

    def test_two_slots_have_disjoint_artifact_namespaces(self):
        second_slot = p.add_slots(self.slot)
        second = p.make_claim(second_slot, "A" * 64, self.writer, "H")
        first_ledger, second_ledger = p.Ledger(self.writer), p.Ledger(self.writer)
        first_ledger.claim(self.claim, self.writer, first_ledger.head)
        second["value"]["expected_parent_before_claim"] = second_ledger.head
        second_ledger.claim(second, self.writer, second_ledger.head)
        first_path = first_ledger.archive_raw(self.key, "CG_TOP250", b"a")
        second_path = second_ledger.archive_raw(second["value"]["idempotency_key"], "CG_TOP250", b"b")
        self.assertNotEqual(first_path, second_path)
        self.assertIn(p.claim_filesafe(self.slot), first_path)
        self.assertIn(p.claim_filesafe(second_slot), second_path)

    def test_impl_change_requires_new_epoch_and_h0(self):
        p.ensure_impl_boundary("A", "A", p.EPOCH_ID, "H0")
        for args in (("A", "B", p.EPOCH_ID, "H0"), ("A", "A", "OLD", "H0"), ("A", "A", p.EPOCH_ID, "")):
            with self.assertRaises(p.PitError):
                p.ensure_impl_boundary(*args)


class PermissionAndStaticTests(unittest.TestCase):
    def test_live_boundary_fail_closed_for_each_missing_permission(self):
        digest = p.current_i_impl(Path(__file__).parent)
        full = {
            "PIT_ACTIVATION_APPROVED": "YES",
            "PIT_ACTIVATION_CANDIDATE_SLOT": "2026-07-26T20:00:00.000Z",
            "PIT_TARGET_WRITE_APPROVED": "YES",
            "PIT_AUTHORIZED_WRITER": p.AUTHORIZED_WRITER,
            "PIT_GITHUB_REPO": p.GITHUB_REPO,
            "PIT_GITHUB_BRANCH": p.GITHUB_BRANCH,
            "PIT_I_IMPL": digest,
            "PIT_H0": "a" * 40,
            "PIT_IMAGE_DIGEST": "sha256:" + "b" * 64,
            "PIT_DEPLOY_KEY_FINGERPRINT": "SHA256:" + "C" * 43,
            "CLOUD_RUN_EXECUTION": "pit-ledger-abcde",
            "CLOUD_RUN_JOB": "pit-ledger",
            "CLOUD_RUN_TASK_INDEX": "0",
            "CLOUD_RUN_TASK_ATTEMPT": "0",
        }
        p.require_live_authorization(full, Path(__file__).parent)
        for name in tuple(full):
            env = dict(full)
            env.pop(name)
            with self.assertRaises(p.PitError, msg=name):
                p.require_live_authorization(env, Path(__file__).parent)
        for bad in ("A", "0" * 64):
            with self.assertRaises(p.PitError):
                p.require_live_authorization(dict(full, PIT_I_IMPL=bad), Path(__file__).parent)

    def test_cloud_run_and_ssh_keygen_fingerprint_witness(self):
        root = Path(__file__).parent
        full = {
            "PIT_ACTIVATION_APPROVED": "YES",
            "PIT_ACTIVATION_CANDIDATE_SLOT": "2026-07-26T20:00:00.000Z",
            "PIT_TARGET_WRITE_APPROVED": "YES",
            "PIT_AUTHORIZED_WRITER": p.AUTHORIZED_WRITER,
            "PIT_GITHUB_REPO": p.GITHUB_REPO,
            "PIT_GITHUB_BRANCH": p.GITHUB_BRANCH,
            "PIT_I_IMPL": p.current_i_impl(root),
            "PIT_H0": "a" * 40,
            "PIT_IMAGE_DIGEST": "sha256:" + "b" * 64,
            "PIT_DEPLOY_KEY_FINGERPRINT": "SHA256:" + "C" * 43,
            "CLOUD_RUN_EXECUTION": "pit-ledger-abcde",
            "CLOUD_RUN_JOB": "pit-ledger",
            "CLOUD_RUN_TASK_INDEX": "0",
            "CLOUD_RUN_TASK_ATTEMPT": "0",
        }
        calls = []

        secret = "PRIVATE_KEY_SHOULD_NOT_LEAK"
        child_stderr = [secret]

        def offline_run(args, **kwargs):
            calls.append((args, kwargs))
            if args[:2] == ["ssh-keygen", "-y"]:
                return subprocess.CompletedProcess(args, 0, "ssh-ed25519 AAAA\n", "")
            if args[:2] == ["ssh-keygen", "-l"]:
                return subprocess.CompletedProcess(args, 0, "256 " + full["PIT_DEPLOY_KEY_FINGERPRINT"] + " key (ED25519)\n", "")
            if args[:2] == ["git", "clone"] or args[:3] == ["git", "merge-base", "--is-ancestor"]:
                return subprocess.CompletedProcess(args, 0, "", "")
            if args[:3] == ["git", "branch", "--show-current"]:
                return subprocess.CompletedProcess(args, 0, p.GITHUB_BRANCH + "\n", "")
            if args[:3] == ["git", "remote", "get-url"]:
                return subprocess.CompletedProcess(args, 0, p.GITHUB_REPO + "\n", "")
            if args == ["git", "config", "--get", "remote.origin.partialclonefilter"]:
                return subprocess.CompletedProcess(args, 0, "blob:none\n", "")
            if args[:3] == ["git", "config", "user.name"]:
                return subprocess.CompletedProcess(args, 0, "PIT Ledger Writer\n" if len(args) == 3 else "", "")
            if args[:3] == ["git", "config", "user.email"]:
                return subprocess.CompletedProcess(args, 0, "pit-ledger@users.noreply.github.com\n" if len(args) == 3 else "", "")
            if args[-1] == "capture":
                return subprocess.CompletedProcess(args, 1, secret, child_stderr[0])
            raise AssertionError(args)

        with (
            patch.dict(os.environ, {}, clear=True),
            patch.object(p.subprocess, "run") as no_call,
            self.assertRaisesRegex(p.PitError, "^STOP_PERMISSION_REQUIRED_AUTH$"),
        ):
            p.cloud_run()
        no_call.assert_not_called()
        for child_stderr[0], expected in (
            (secret, "CAPTURE_CHILD_STDERR_SHA256="),
            ("PIT_ERROR:SCHEMA_FAILURE", "SCHEMA_FAILURE"),
        ):
            with tempfile.TemporaryDirectory() as temp:
                key = Path(temp, "id_ed25519")
                key.write_bytes(b"private fixture")
                with (
                    patch.dict(os.environ, full, clear=True),
                    patch.object(p, "SECRET_MOUNT", str(key)),
                    patch.object(p, "current_i_impl", return_value=full["PIT_I_IMPL"]),
                    patch.object(p.subprocess, "run", side_effect=offline_run),
                    patch("sys.stderr", new_callable=io.StringIO) as stderr,
                    self.assertRaises(p.PitError) as raised,
                ):
                    p.cloud_run()
            self.assertEqual(str(raised.exception), "STOP_PERMISSION_REQUIRED_CAPTURE")
            self.assertIn(expected, stderr.getvalue())
            if child_stderr[0] == secret:
                self.assertNotIn(secret, str(raised.exception))
                self.assertNotIn(secret, stderr.getvalue())
        self.assertEqual(calls[-1][0][-1], "capture")
        self.assertTrue(calls[-1][1]["capture_output"])
        self.assertTrue(calls[-1][1]["text"])
        clone = next(args for args, _ in calls if args[:2] == ["git", "clone"])
        self.assertIn("--filter=blob:none", clone)
        self.assertIn("--sparse", clone)
        with tempfile.TemporaryDirectory() as temp:
            private = Path(temp, "id_ed25519")
            generated = subprocess.run(
                ["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(private)],
                capture_output=True, text=True, check=False,
            )
            self.assertEqual(generated.returncode, 0, generated.stderr)
            public = private.with_suffix(".pub")
            argv = ["ssh-keygen", "-l", "-E", "sha256", "-f", str(public)]
            result = subprocess.run(argv, capture_output=True, text=True, check=False)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.args, argv)
            fingerprint = result.stdout.split()[1]
            key_blob = base64.b64decode(public.read_text(encoding="ascii").split()[1])
            expected = "SHA256:" + base64.b64encode(hashlib.sha256(key_blob).digest()).decode().rstrip("=")
            self.assertEqual(fingerprint, expected)

    def test_manifest_recomputed_from_raw_bytes(self):
        root = Path(__file__).parent
        manifest = p.implementation_manifest(root)
        self.assertEqual(manifest, (root / "implementation_manifest.json").read_bytes())
        value = json.loads(manifest)
        self.assertEqual([item["path"] for item in value["files"]], list(p.IMPLEMENTATION_FILES))
        for item in value["files"]:
            raw = (root / item["path"]).read_bytes()
            self.assertEqual((item["bytes"], item["sha256"]), (len(raw), p.sha256(raw)))

    def test_live_capture_batches_eight_raw_pages_into_11_commits(self):
        class CountingWriter:
            def __init__(self):
                self.commits = []

            def remote_head(self):
                return "H0"

            def verify_history(self, *_):
                return None

            def read_at(self, *_):
                return None

            def history_absent(self, *_):
                return True

            def publish(self, relative, data, head, message, raw=False):
                self.commits.append((message, [relative], raw))
                return "PUBLISHED", f"H{len(self.commits)}"

            def publish_many(self, files, head, message):
                self.commits.append((message, sorted(files), None))
                return "PUBLISHED", f"H{len(self.commits)}"

            def publish_terminal(self, *_):
                self.commits.append(("terminal", [], None))
                return "PUBLISHED", f"H{len(self.commits)}"

        writer = CountingWriter()
        now = datetime.now(timezone.utc)
        slot = now.replace(minute=30 if now.minute >= 30 else 0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        env = {
            "PIT_ACTIVATION_CANDIDATE_SLOT": slot, "PIT_AUTHORIZED_WRITER": p.AUTHORIZED_WRITER,
            "PIT_H0": "H0", "PIT_I_IMPL": "A" * 64,
            "PIT_IMAGE_DIGEST": "sha256:" + "b" * 64,
            "CLOUD_RUN_EXECUTION": "pit-ledger-test", "CLOUD_RUN_JOB": "pit-ledger",
        }

        def acquire(_, claim, __, ___, durable, ____, _____):
            root = p.slot_root(claim["value"]["formal_slot_utc"])
            for ordinal in range(8):
                durable(root + f"raw/S{ordinal}-0.bin", b"raw", True)
                durable(root + f"source-manifests/S{ordinal}-0.json", p.canonical_bytes({"page": ordinal}), False)
            return {"__outcome__": {"outcome_kind": "ABORTED_ATTEMPT", "reason_codes": ["ATTEMPT_ABORTED"]}}

        with (
            patch.dict(os.environ, env, clear=True),
            patch.object(p, "require_live_authorization"),
            patch.object(p, "GitWriter", return_value=writer),
            patch.object(p, "prepare_live_slot", return_value="H0"),
            patch.object(p, "acquire_fixture_sources", side_effect=acquire),
            patch("sys.stdout", new_callable=io.StringIO),
        ):
            p.live_capture()
        self.assertEqual(len(writer.commits), 11)
        self.assertEqual(writer.commits[0][0], "PIT claim")
        self.assertTrue(all(commit[2] for commit in writer.commits[1:9]))
        self.assertEqual((writer.commits[9][0], len(writer.commits[9][1])), ("PIT evidence", 9))
        self.assertEqual(writer.commits[10][0], "terminal")

    def test_bad_source_schema_is_total_and_never_qa_ok(self):
        writer, slot = "writer", "2026-07-26T20:00:00.000Z"
        ledger = p.Ledger(writer)
        claim = p.make_claim(slot, "A" * 64, writer, ledger.head)
        ledger.claim(claim, writer, ledger.head)
        bodies = {"CG_TOP250": universe(), "BN_FUT_EXCHANGE_INFO": b'{"bad":[]}'}
        result = p.acquire_fixture_sources(ledger, claim, lambda source, _: (200, bodies[source], {}))
        bad = result["__source_manifests__"][1]
        self.assertEqual((bad["parse_status"], bad["qa_status"]), ("SCHEMA_FAILURE", "QA_NOT_RUN"))
        self.assertIsNotNone(bad["error_record_sha256"])
        self.assertEqual(len(result["__source_manifests__"]), len(p.SOURCE_ORDER))
        required = {"claim_relative_path", "available_raw_count", "workflow_run_id", "job_id", "log_locator", "source_manifest_sha256s"}
        self.assertTrue(required.issubset(result["__outcome__"]))

    def test_source_failure_window_handler_and_status_mapping_are_total(self):
        writer, slot = "writer", "2026-07-26T20:00:00.000Z"

        def claimed():
            ledger = p.Ledger(writer)
            claim = p.make_claim(slot, "A" * 64, writer, ledger.head)
            self.assertEqual(ledger.claim(claim, writer, ledger.head), "CLAIMED")
            return ledger, claim

        ledger, claim = claimed()
        result = p.acquire_fixture_sources(ledger, claim, lambda *_: (500, b"", {}))
        self.assertEqual(len(result["__source_manifests__"]), 8)
        self.assertEqual(result["__source_manifests__"][0]["source_status"], "SOURCE_FAILURE")
        self.assertEqual(len(ledger.outcomes), 1)
        self.assertEqual(result["__outcome__"]["outcome_kind"], "GAP_UNIVERSE")

        ledger, claim = claimed()
        times = iter((
            datetime(2026, 7, 26, 20, 1, tzinfo=timezone.utc),
            datetime(2026, 7, 26, 20, 1, tzinfo=timezone.utc),
            datetime(2026, 7, 26, 20, 10, tzinfo=timezone.utc),
        ))
        result = p.acquire_fixture_sources(ledger, claim, lambda *_: (200, universe(), {}), lambda: next(times))
        self.assertEqual(len(result["__source_manifests__"]), 8)
        self.assertEqual(len(ledger.outcomes), 1)
        self.assertEqual(result["__source_manifests__"][0]["source_status"], "SOURCE_FAILURE")

        ledger, claim = claimed()
        result = p.acquire_fixture_sources(ledger, claim, lambda *_: (_ for _ in ()).throw(RuntimeError("fixture")))
        self.assertEqual(len(result["__source_manifests__"]), 8)
        self.assertEqual(len(ledger.outcomes), 1)

        ledger, claim = claimed()
        inverted = universe(["100", "1", "50"] + [str(247 - i) for i in range(247)])
        result = p.acquire_fixture_sources(ledger, claim, lambda *_: (200, inverted, {}))
        first = result["__source_manifests__"][0]
        self.assertEqual((first["parse_status"], first["qa_status"]), ("PARSE_OK", "QA_FAILURE"))

        ledger, claim = claimed()
        bodies = {
            "CG_TOP250": universe(),
            "BN_FUT_EXCHANGE_INFO": b'{"symbols":[]}',
            "BN_FUT_PREMIUM_INDEX": b'[{"indexPrice":"bad","lastFundingRate":"NaN","markPrice":"bad","symbol":"BTCUSDT","time":-1}]',
        }
        result = p.acquire_fixture_sources(ledger, claim, lambda source, _: (200, bodies[source], {}))
        premium = result["__source_manifests__"][2]
        self.assertEqual((premium["parse_status"], premium["qa_status"]), ("SCHEMA_FAILURE", "QA_NOT_RUN"))
        self.assertEqual(result["__outcome__"]["outcome_kind"], "SNAPSHOT_PARTIAL")

    def test_snapshot_semantic_witness_and_contextual_provenance(self):
        claim, manifests, snapshot, context = complete_snapshot_fixture()
        p.validate_snapshot(snapshot, claim, manifests, context, False, "A" * 64)
        with self.assertRaises(p.PitError):
            p.validate_snapshot(snapshot, claim, manifests, {"writer_stop": False}, False, "A" * 64)
        broken = json.loads(json.dumps(snapshot))
        row = broken["assets"][0]["venues"][0]
        row.update(
            mapping_status="MAPPED", exchange_symbol="X0USDT", perp_exists=None,
            missing_reasons=["SCHEMA_FAILURE"], funding_rate="0.1",
            funding_observed_at_utc="2026-07-26T20:00:00.000Z",
            bid_price="1", ask_price="2", spread_bps="6666.66666667",
            mark_price="1", index_price="1",
        )
        with self.assertRaises(p.PitError):
            p.validate_snapshot(broken)
        empty = json.loads(json.dumps(snapshot))
        for asset in empty["assets"]:
            for venue in asset["venues"]:
                venue["source_raw_sha256s"] = []
        with self.assertRaises(p.PitError):
            p.validate_snapshot(empty, claim, manifests, context, False, "A" * 64)
        wrong_image = json.loads(json.dumps(snapshot))
        wrong_image["image_digest"] = "sha256:" + "f" * 64
        with self.assertRaisesRegex(p.PitError, "CLAIM_PROVENANCE"):
            p.validate_snapshot(wrong_image, claim, manifests, context, False, "A" * 64)

    def test_existing_perp_missing_ticker_is_snapshot_partial(self):
        _, _, snapshot, _ = complete_snapshot_fixture(binance_case="missing_ticker")
        row = snapshot["assets"][0]["venues"][0]
        self.assertIs(row["perp_exists"], True)
        self.assertIsNone(row["borrowable"])
        self.assertTrue(all(row[field] is None for field in (
            "funding_rate", "funding_observed_at_utc", "bid_price", "ask_price",
            "spread_bps", "mark_price", "index_price",
        )))
        self.assertEqual(row["missing_reasons"], ["NOT_OBSERVED_PUBLIC_ONLY", "SCHEMA_FAILURE"])
        self.assertEqual(
            (snapshot["outcome_kind"], snapshot["slot_status"], snapshot["reason_codes"]),
            ("SNAPSHOT_PARTIAL", "PARTIAL", ["SCHEMA_FAILURE"]),
        )
        inconsistent = json.loads(json.dumps(snapshot))
        inconsistent["assets"][0]["venues"][0]["funding_rate"] = "0.001"
        with self.assertRaises(p.PitError):
            p.validate_snapshot(inconsistent)

    def test_existing_perp_public_only_borrow_is_not_an_error(self):
        claim, manifests, snapshot, context = complete_snapshot_fixture(binance_case="public_perp")
        row = snapshot["assets"][0]["venues"][0]
        self.assertIs(row["perp_exists"], True)
        self.assertIsNone(row["borrowable"])
        self.assertEqual(
            (
                row["funding_rate"], row["funding_observed_at_utc"], row["bid_price"],
                row["ask_price"], row["spread_bps"], row["mark_price"], row["index_price"],
            ),
            ("0.001", "2024-07-03T09:46:40.123Z", "99", "100", "100.50251256", "100", "100"),
        )
        self.assertEqual(row["missing_reasons"], ["NOT_OBSERVED_PUBLIC_ONLY"])
        self.assertEqual(
            (snapshot["outcome_kind"], snapshot["slot_status"], snapshot["reason_codes"]),
            ("SNAPSHOT_COMPLETE", "COMPLETE", []),
        )
        p.validate_snapshot(snapshot, claim, manifests, context, False, "A" * 64)
        inconsistent = json.loads(json.dumps(snapshot))
        inconsistent["assets"][0]["venues"][0]["borrowable"] = False
        with self.assertRaises(p.PitError):
            p.validate_snapshot(inconsistent)

    def test_contextual_raw_rederivation_accepts_complete_and_partial(self):
        for case, kind in (("missing_ticker", "SNAPSHOT_PARTIAL"), ("public_perp", "SNAPSHOT_COMPLETE")):
            with self.subTest(case=case):
                claim, manifests, snapshot, context = complete_snapshot_fixture(binance_case=case)
                self.assertEqual(snapshot["outcome_kind"], kind)
                p.validate_snapshot(snapshot, claim, manifests, context, False, "A" * 64)

    def test_post_cg_failure_each_later_source_has_full_validated_snapshot_and_missing_assets_never_publishes(self):
        self.assertEqual(len(p.SOURCE_ORDER[1:]), 7)
        for source_id in p.SOURCE_ORDER[1:]:
            with self.subTest(source_id=source_id):
                claim, manifests, snapshot, context = complete_snapshot_fixture(failed_source=source_id)
                self.assertEqual(
                    (snapshot["outcome_kind"], snapshot["slot_status"], snapshot["reason_codes"]),
                    ("SNAPSHOT_PARTIAL", "PARTIAL", ["SOURCE_FAILURE"]),
                )
                self.assertEqual(len(snapshot["assets"]), 250)
                self.assertEqual(snapshot["qa"], {"qa_status": "QA_FAILURE", "reason_codes": ["SOURCE_FAILURE"]})
                if source_id == "BY_MARGIN_BORROWABLE":
                    manifest = next(item for item in manifests if item["source_id"] == source_id)
                    self.assertEqual((manifest["source_status"], manifest["parse_status"]), ("SOURCE_FAILURE", "PARSE_NOT_RUN"))
                self.assertEqual(
                    (
                        snapshot["attempt_started_at_utc"],
                        snapshot["capture_completed_at_utc"],
                        snapshot["materialized_at_utc"],
                    ),
                    (claim["value"]["claimed_at_utc"],) * 3,
                )
                p.validate_snapshot(snapshot, claim, manifests, context, False, "A" * 64)
                if source_id == "BN_FUT_EXCHANGE_INFO":
                    row = snapshot["assets"][0]["venues"][0]
                    self.assertEqual(
                        (row["perp_exists"], row["borrowable"], row["missing_reasons"]),
                        (None, None, ["NOT_OBSERVED_PUBLIC_ONLY", "SOURCE_FAILURE"]),
                    )

        writer_identity = "PIT Ledger Writer <pit@example.invalid>"
        with tempfile.TemporaryDirectory(prefix="pit-ledger-missing-assets-") as temp:
            remote, repo = Path(temp, "remote.git"), Path(temp, "repo")

            def git(cwd, *args):
                result = subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, check=False)
                self.assertEqual(result.returncode, 0, result.stderr)
                return result.stdout.strip()

            remote.mkdir()
            git(remote, "init", "--bare", "--quiet")
            repo.mkdir()
            git(repo, "init", "--quiet")
            git(repo, "config", "user.name", "seed")
            git(repo, "config", "user.email", "seed@example.invalid")
            git(repo, "commit", "--allow-empty", "--quiet", "-m", "H0")
            git(repo, "branch", "-M", p.GITHUB_BRANCH)
            git(repo, "remote", "add", "origin", str(remote))
            git(repo, "push", "--quiet", "-u", "origin", p.GITHUB_BRANCH)
            h0 = git(repo, "rev-parse", "HEAD")
            git(repo, "config", "user.name", "PIT Ledger Writer")
            git(repo, "config", "user.email", "pit@example.invalid")
            writer = p.GitWriter(repo, writer_identity)
            claim, manifests, snapshot, context = complete_snapshot_fixture(
                failed_source="BY_MARGIN_BORROWABLE", parent=h0, writer=writer_identity,
            )
            status, head = writer.publish(
                claim["relative_path"], p.canonical_bytes(claim["value"]), h0, "claim",
            )
            self.assertEqual(status, "PUBLISHED")
            files = {
                path: (raw, True) for path, raw in context.raw.items()
            }
            files.update({
                path: (p.canonical_bytes(manifest), False)
                for path, manifest in context.source_manifests.items()
            })
            failed = next(item for item in manifests if item["source_id"] == "BY_MARGIN_BORROWABLE")
            error_record = {
                "error_class": "SOURCE_FAILURE",
                "log_locator": claim["value"]["log_locator"],
                "log_sha256": p.sha256(p.run_log_bytes(claim)),
                "page_ordinal": 0,
                "raw_relative_path": None,
                "raw_sha256": None,
                "source_id": "BY_MARGIN_BORROWABLE",
            }
            self.assertEqual(p.sha256(p.canonical_bytes(error_record)), failed["error_record_sha256"])
            files[failed["error_record_relative_path"]] = (p.canonical_bytes(error_record), False)
            files[claim["value"]["log_locator"]] = (p.run_log_bytes(claim), True)
            status, head = writer.publish_many(files, head, "fixture artifacts")
            self.assertEqual(status, "PUBLISHED")

            stripped = dict(snapshot)
            stripped.pop("assets")
            before = writer.remote_head()
            with self.assertRaisesRegex(p.PitError, "INVALID_ASSET_COUNT"):
                writer.publish_terminal(
                    h0, claim, stripped["outcome_kind"], stripped["reason_codes"],
                    True, "A" * 64, stripped,
                )
            self.assertEqual(writer.remote_head(), before)
            self.assertIsNone(writer.read_at(before, p.PREFIX + "outcomes/" + p.claim_filesafe(claim["value"]["formal_slot_utc"]) + ".json"))

            context = writer.context(h0, before, claim["value"]["idempotency_key"])
            manifests = sorted(
                context.source_manifests.values(),
                key=lambda item: (p.SOURCE_ORDER.index(item["source_id"]), item["page_ordinal"]),
            )
            raw_pairs = [
                (item["raw_relative_path"], item["raw_sha256"])
                for item in manifests if item.get("raw_sha256")
            ]
            malformed = dict(stripped)
            malformed.update(
                previous_ledger_head=before,
                log_locator=claim["value"]["log_locator"],
                log_sha256=p.sha256(p.run_log_bytes(claim)),
                source_manifest_sha256s=sorted({p.sha256(p.canonical_bytes(item)) for item in manifests}),
                available_raw_count=len(raw_pairs),
                available_raw_relative_paths=[item[0] for item in raw_pairs],
                available_raw_sha256s=sorted({item[1] for item in raw_pairs}),
            )
            slot = claim["value"]["formal_slot_utc"]
            outcome_path = p.PREFIX + "outcomes/" + p.claim_filesafe(slot) + ".json"
            outcome_bytes = p.canonical_bytes(malformed)
            slot_manifest = {
                "claim_sha256": malformed["claim_sha256"],
                "error_record_sha256s": sorted({
                    item["error_record_sha256"] for item in manifests if item.get("error_record_sha256")
                }),
                "formal_slot_utc": slot,
                "idempotency_key": claim["value"]["idempotency_key"],
                "ledger_seq": 1,
                "outcome_relative_path": outcome_path,
                "outcome_sha256": p.sha256(outcome_bytes),
                "raw_sha256s": malformed["available_raw_sha256s"],
                "source_config_sha256": p.sha256(p.canonical_bytes(p.URLS)),
                "source_manifest_sha256s": malformed["source_manifest_sha256s"],
            }
            slot_manifest_path = p.slot_root(slot) + "slot-manifest.json"
            slot_manifest_bytes = p.canonical_bytes(slot_manifest)
            record = {
                "formal_slot_utc": slot,
                "idempotency_key": claim["value"]["idempotency_key"],
                "ledger_seq": 1,
                "outcome_relative_path": outcome_path,
                "outcome_sha256": p.sha256(outcome_bytes),
                "slot_manifest_relative_path": slot_manifest_path,
                "slot_manifest_sha256": p.sha256(slot_manifest_bytes),
            }
            index = p.canonical_bytes(record)
            checkpoint_path = p.PREFIX + "checkpoints/" + p.claim_filesafe(slot) + ".json"
            checkpoint = p.canonical_bytes({
                "formal_slot_utc": slot,
                "ledger_index_sha256": p.sha256(index),
                "ledger_seq": 1,
                "terminal_parent": before,
            })
            status, bad_head = writer.publish_many(
                {
                    p.LEDGER_INDEX_PATH: (index, True),
                    checkpoint_path: (checkpoint, False),
                    outcome_path: (outcome_bytes, False),
                    slot_manifest_path: (slot_manifest_bytes, False),
                },
                before,
                "malformed fixture terminal",
            )
            self.assertEqual(status, "PUBLISHED")
            calls = []
            validate_snapshot = p.validate_snapshot
            def observed_validation(*args, **kwargs):
                calls.append(True)
                return validate_snapshot(*args, **kwargs)
            p.validate_snapshot = observed_validation
            try:
                duplicate_status = writer.verify_duplicate(
                    claim["value"]["idempotency_key"], h0, bad_head, h0, "A" * 64,
                )
            finally:
                p.validate_snapshot = validate_snapshot
            self.assertEqual(duplicate_status, "EPOCH_WRITER_STOP")
            self.assertEqual(calls, [True])

    def test_no_perp_with_public_only_borrow_is_contextually_valid_only_when_consistent(self):
        claim, manifests, snapshot, context = complete_snapshot_fixture()
        row = snapshot["assets"][0]["venues"][0]
        self.assertIs(row["perp_exists"], False)
        self.assertIsNone(row["borrowable"])
        self.assertTrue(all(row[field] is None for field in (
            "funding_rate", "funding_observed_at_utc", "bid_price", "ask_price",
            "spread_bps", "mark_price", "index_price",
        )))
        self.assertEqual(
            row["missing_reasons"],
            ["NOT_APPLICABLE_NO_PERP", "NOT_OBSERVED_PUBLIC_ONLY"],
        )
        self.assertEqual(snapshot["reason_codes"], [])
        self.assertEqual(snapshot["outcome_kind"], "SNAPSHOT_COMPLETE")
        p.validate_snapshot(snapshot, claim, manifests, context, False, "A" * 64)

        for change in ("order", "borrowable", "missing_reason"):
            with self.subTest(change=change):
                inconsistent = json.loads(json.dumps(snapshot))
                target = inconsistent["assets"][0]["venues"][0]
                if change == "order":
                    target["missing_reasons"].reverse()
                elif change == "borrowable":
                    target["borrowable"] = False
                else:
                    target["missing_reasons"] = ["NOT_APPLICABLE_NO_PERP"]
                with self.assertRaises(p.PitError):
                    p.validate_snapshot(inconsistent)

    def test_missing_any_source_or_page_rejected(self):
        claim, manifests, snapshot, context = complete_snapshot_fixture(2)
        missing_source = [item for item in manifests if item["source_id"] != "BY_MARGIN_BORROWABLE"]
        source_context = p.GitContext(
            context.h0, context.head, context.key, context.claim, context.claim_bytes,
            {path: raw for path, raw in context.raw.items() if "BY_MARGIN_BORROWABLE" not in path},
            {path: item for path, item in context.source_manifests.items() if item["source_id"] != "BY_MARGIN_BORROWABLE"},
            context.log, context.outcome_count, context.claim_before_raw,
        )
        with self.assertRaises(p.PitError):
            p.validate_snapshot(snapshot, claim, missing_source, source_context, False, "A" * 64)
        missing_page = [
            item for item in manifests
            if not (item["source_id"] == "BY_LINEAR_INSTRUMENTS" and item["page_ordinal"] == 1)
        ]
        page_context = p.GitContext(
            context.h0, context.head, context.key, context.claim, context.claim_bytes,
            {path: raw for path, raw in context.raw.items() if "BY_LINEAR_INSTRUMENTS-1" not in path},
            {
                path: item for path, item in context.source_manifests.items()
                if not (item["source_id"] == "BY_LINEAR_INSTRUMENTS" and item["page_ordinal"] == 1)
            },
            context.log, context.outcome_count, context.claim_before_raw,
        )
        with self.assertRaises(p.PitError):
            p.validate_snapshot(snapshot, claim, missing_page, page_context, False, "A" * 64)

    def test_raw_rederivation_mismatch_rejected(self):
        claim, manifests, snapshot, context = complete_snapshot_fixture()
        changed = json.loads(json.dumps(snapshot))
        manifests = [dict(item) for item in manifests]
        target = next(item for item in manifests if item["source_id"] == "BN_FUT_EXCHANGE_INFO")
        old_hash = target["raw_sha256"]
        raw = json.dumps({"symbols": [binance_instrument("X0")]}, separators=(",", ":")).encode()
        target.update(raw_bytes=len(raw), raw_sha256=p.sha256(raw))
        changed["available_raw_sha256s"] = sorted(
            p.sha256(raw) if item == old_hash else item for item in changed["available_raw_sha256s"]
        )
        changed["source_manifest_sha256s"] = sorted({p.sha256(p.canonical_bytes(item)) for item in manifests})
        for asset in changed["assets"]:
            venue = asset["venues"][0]
            venue["source_raw_sha256s"] = sorted(
                p.sha256(raw) if item == old_hash else item for item in venue["source_raw_sha256s"]
            )
        raw_context = dict(context.raw)
        raw_context[target["raw_relative_path"]] = raw
        manifest_context = {
            path: next(
                item for item in manifests
                if (item["source_id"], item["page_ordinal"]) == (original["source_id"], original["page_ordinal"])
            )
            for path, original in context.source_manifests.items()
        }
        changed_context = p.GitContext(
            context.h0, context.head, context.key, context.claim, context.claim_bytes,
            raw_context, manifest_context, context.log, context.outcome_count, context.claim_before_raw,
        )
        with self.assertRaisesRegex(p.PitError, "RAW_REDERIVATION_MISMATCH"):
            p.validate_snapshot(changed, claim, manifests, changed_context, False, "A" * 64)

    def test_delayed_start_recovers_overdue_before_window_stop(self):
        class CalendarWriter:
            def __init__(self):
                self.recovered = []

            def remote_head(self):
                return "HEAD"

            def ledger_records(self, _):
                return []

            def recover_slot(self, _, claim, __, expected_i_impl):
                self.recovered.append((claim["value"]["formal_slot_utc"], expected_i_impl))
                return "PUBLISHED", "NEXT"

        writer = CalendarWriter()
        with self.assertRaisesRegex(p.PitError, "ACQUISITION_WINDOW_CLOSED"):
            p.prepare_live_slot(
                writer, "H0", "2026-07-26T19:30:00.000Z", "2026-07-26T20:00:00.000Z",
                datetime(2026, 7, 26, 20, 12, tzinfo=timezone.utc), "A" * 64, "writer",
            )
        self.assertEqual(writer.recovered, [("2026-07-26T19:30:00.000Z", "A" * 64)])

    def test_v8_container_and_branch_are_frozen(self):
        root = Path(__file__).parent
        docker = (root / "Dockerfile").read_text()
        self.assertFalse((root / ".github/workflows/pit-ledger.yml").exists())
        self.assertEqual(p.CONTRACT_ID, "PIT_LEDGER_PUBLIC_ONLY_V8")
        self.assertEqual(p.EPOCH_ID, "BASKET_PIT_LEDGER_TOP250_BINANCE_BYBIT_PUBLIC_V8")
        self.assertEqual(p.GITHUB_BRANCH, "pit-ledger-public-v8")
        self.assertIn("FROM --platform=linux/amd64 python:3.12-slim-bookworm@sha256:8a7e7cc04fd3e2bd787f7f24e22d5d119aa590d429b50c95dfe12b3abe52f48b", docker)
        self.assertIn("COPY Dockerfile pit_ledger.py /app/", docker)
        self.assertIn('ENTRYPOINT ["python","/app/pit_ledger.py","cloud-run"]', docker)
        p.acquisition_window("2026-07-26T20:00:00.000Z", datetime(2026, 7, 26, 20, 2, tzinfo=timezone.utc))
        p.acquisition_window("2026-07-26T20:30:00.000Z", datetime(2026, 7, 26, 20, 32, tzinfo=timezone.utc))
        for now in (datetime(2026, 7, 26, 19, 59, tzinfo=timezone.utc), datetime(2026, 7, 26, 20, 10, tzinfo=timezone.utc)):
            with self.assertRaises(p.PitError):
                p.acquisition_window("2026-07-26T20:00:00.000Z", now)

    def test_pinned_i_impl_and_atomic_terminal_crash_oracles_real_local_bare_remote(self):
        p.e2e_self_check()

    def test_self_check_subprocess_offline(self):
        env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
        result = subprocess.run(
            [os.sys.executable, "pit_ledger.py", "self-check"],
            cwd=Path(__file__).parent,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "SELF_CHECK PASS")


if __name__ == "__main__":
    unittest.main()
