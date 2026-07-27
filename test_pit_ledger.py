import json
import os
import subprocess
import unittest
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

    def test_three_r4_exact_byte_oracles(self):
        expected = (
            (242, "BB6A1F4C2A99D23E31C79809A1D25A38720A004831C34FE168EC681788EC2165"),
            (333, "8D13A204BDC833121A88F3C531B74A677987E0CE8A96C5610C9DA8C10CA2300D"),
            (257, "96701B35D8FC21BB3BD17BD3F27BA492E82C0959AEEEC9CB31BDDD58B7575CA4"),
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

    def test_binance_assetName_and_borrowable_tristate(self):
        assets = [{"assetName": "BTC", "isBorrowable": True}]
        pairs = [{"base": "BTC", "quote": "USDT", "isMarginTrade": True, "isSellAllowed": True}]
        self.assertIs(p.borrowable_binance("BTC", assets, pairs), True)
        self.assertIs(p.borrowable_binance("ETH", assets, pairs), False)
        self.assertIs(p.borrowable_binance("BTC", [{"asset": "BTC", "isBorrowable": True}], pairs), False)
        self.assertIsNone(p.borrowable_binance("BTC", assets * 2, pairs))

    def test_bybit_uta_four_margin_enums_and_unknown(self):
        currencies = [{"currency": "BTC", "borrowable": True}]
        expected = {"none": False, "normalSpotOnly": False, "utaOnly": True, "both": True}
        for enum, result in expected.items():
            spots = [{"baseCoin": "BTC", "quoteCoin": "USDT", "marginTrading": enum}]
            self.assertIs(p.borrowable_bybit("BTC", currencies, spots), result)
        self.assertIsNone(p.borrowable_bybit("BTC", currencies, [{"baseCoin": "BTC", "quoteCoin": "USDT", "marginTrading": "future"}]))

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
            "BN_MARGIN_ASSETS": b"[]",
            "BN_MARGIN_PAIRS": b"[]",
            "BY_LINEAR_TICKERS": b'{"result":{"list":[]},"time":1720000000123}',
            "BY_SPOT_INSTRUMENTS": b'{"result":{"list":[]}}',
            "BY_MARGIN_BORROWABLE": b'{"result":{"list":[]}}',
        }
        pages = [b'{"result":{"list":[],"nextPageCursor":"next"}}', b'{"result":{"list":[],"nextPageCursor":""}}']
        def fetch(source, url):
            body = pages.pop(0) if source == "BY_LINEAR_INSTRUMENTS" else bodies[source]
            return 200, body, {}
        parsed = p.acquire_fixture_sources(ledger, claim, fetch)
        kinds = [event.split(":", 1)[0] for event in ledger.events]
        self.assertEqual(kinds[0], "claim")
        self.assertEqual(kinds[1:], ["request", "raw", "parse", "manifest"] * 6 + ["request", "raw", "parse", "manifest"] * 2 + ["request", "raw", "parse", "manifest"] * 3)
        request_sources = [event.rsplit(":", 1)[-1] for event in ledger.events if event.startswith("request:")]
        self.assertEqual(request_sources, list(p.SOURCE_ORDER[:6]) + ["BY_LINEAR_INSTRUMENTS"] * 2 + list(p.SOURCE_ORDER[7:]))
        snapshot = p.build_snapshot(raw, parsed, claim)
        p.validate_snapshot(snapshot)
        self.assertEqual(snapshot["outcome_kind"], "SNAPSHOT_COMPLETE")
        self.assertEqual(len(snapshot["assets"]), 250)
        self.assertEqual(sum(len(asset["venues"]) for asset in snapshot["assets"]), 500)
        self.assertEqual([row["venue"] for row in snapshot["assets"][0]["venues"]], list(p.VENUES))
        self.assertIs(snapshot["assets"][0]["venues"][0]["borrowable"], False)
        self.assertEqual(snapshot["assets"][0]["venues"][0]["missing_reasons"], ["NOT_APPLICABLE_NO_PERP"])
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
        for source, url in p.URLS.items():
            p.validate_source_url(source, url)
            self.assertNotIn("v4", url.lower())
        for source, url in (("CG_TOP250", p.URLS["CG_TOP250"] + "&page=2"), ("BY_LINEAR_TICKERS", "http://api.bybit.com/")):
            with self.assertRaises(p.PitError):
                p.validate_source_url(source, url)
        self.assertEqual(set(p.SOURCE_ORDER), set(p.URLS))
        self.assertEqual(p.ENUMS["method"], {"GET"})

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
            "PIT_TARGET_WRITE_APPROVED": "YES",
            "PIT_SECRET_APPROVED": "YES",
            "PIT_KEY_PERMISSION_PROOF": "READ_ONLY_MARKET_DATA_NO_TRADE_BORROW_TRANSFER_WITHDRAW",
            "PIT_AUTHORIZED_WRITER": "writer",
            "PIT_I_IMPL": digest,
            "PIT_H0": "H0",
            "BINANCE_MARKET_DATA_API_KEY": "redacted-fixture",
        }
        p.require_live_authorization(full, Path(__file__).parent)
        for name in tuple(full):
            env = dict(full)
            env.pop(name)
            with self.assertRaises(p.PitError, msg=name):
                p.require_live_authorization(env, Path(__file__).parent)
        wrong = dict(full, PIT_KEY_PERMISSION_PROOF="TRADE")
        with self.assertRaises(p.PitError):
            p.require_live_authorization(wrong, Path(__file__).parent)
        for bad in ("A", "0" * 64):
            with self.assertRaises(p.PitError):
                p.require_live_authorization(dict(full, PIT_I_IMPL=bad), Path(__file__).parent)

    def test_manifest_recomputed_from_raw_bytes(self):
        root = Path(__file__).parent
        manifest = p.implementation_manifest(root)
        self.assertEqual(manifest, (root / "implementation_manifest.json").read_bytes())
        value = json.loads(manifest)
        self.assertEqual([item["path"] for item in value["files"]], list(p.IMPLEMENTATION_FILES))
        for item in value["files"]:
            raw = (root / item["path"]).read_bytes()
            self.assertEqual((item["bytes"], item["sha256"]), (len(raw), p.sha256(raw)))

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

    def test_snapshot_semantic_witness_and_contextual_provenance(self):
        writer, slot = "writer", "2026-07-26T20:00:00.000Z"
        ledger = p.Ledger(writer)
        claim = p.make_claim(slot, "A" * 64, writer, ledger.head)
        ledger.claim(claim, writer, ledger.head)
        bodies = {
            "CG_TOP250": universe(), "BN_FUT_EXCHANGE_INFO": b'{"symbols":[]}',
            "BN_FUT_PREMIUM_INDEX": b"[]", "BN_FUT_BOOK_TICKER": b"[]",
            "BN_MARGIN_ASSETS": b"[]", "BN_MARGIN_PAIRS": b"[]",
            "BY_LINEAR_INSTRUMENTS": b'{"result":{"list":[],"nextPageCursor":""}}',
            "BY_LINEAR_TICKERS": b'{"result":{"list":[]},"time":1720000000123}',
            "BY_SPOT_INSTRUMENTS": b'{"result":{"list":[]}}',
            "BY_MARGIN_BORROWABLE": b'{"result":{"list":[]}}',
        }
        parsed = p.acquire_fixture_sources(ledger, claim, lambda source, _: (200, bodies[source], {}))
        snapshot = p.build_snapshot(bodies["CG_TOP250"], parsed, claim)
        p.validate_snapshot(snapshot, claim, parsed["__source_manifests__"], {"writer_stop": False, "outcome_count": 1, "claim_before_request": True})
        broken = json.loads(json.dumps(snapshot))
        row = broken["assets"][0]["venues"][0]
        row.update(mapping_status="MAPPED", exchange_symbol=None, perp_exists=False, funding_rate="0.1", bid_price="1", ask_price="2")
        with self.assertRaises(p.PitError):
            p.validate_snapshot(broken)

    def test_schedule_and_branch_writer_are_frozen(self):
        workflow = (Path(__file__).parent / ".github/workflows/pit-ledger.yml").read_text()
        self.assertIn('cron: "2,32 * * * *"', workflow)
        self.assertIn("ref: pit-ledger-v1", workflow)
        self.assertIn("contents: write", workflow)
        self.assertIn("HEAD:refs/heads/pit-ledger-v1", workflow)
        p.acquisition_window("2026-07-26T20:00:00.000Z", datetime(2026, 7, 26, 20, 2, tzinfo=timezone.utc))
        p.acquisition_window("2026-07-26T20:30:00.000Z", datetime(2026, 7, 26, 20, 32, tzinfo=timezone.utc))
        for now in (datetime(2026, 7, 26, 19, 59, tzinfo=timezone.utc), datetime(2026, 7, 26, 20, 10, tzinfo=timezone.utc)):
            with self.assertRaises(p.PitError):
                p.acquisition_window("2026-07-26T20:00:00.000Z", now)

    def test_real_local_bare_remote_e2e(self):
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
