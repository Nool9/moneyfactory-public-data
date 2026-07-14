import gzip
import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import scrape_raw


class RawScrapeTest(unittest.TestCase):
    def test_versioned_basket_and_raw_gzip_capture(self):
        basket = json.loads(scrape_raw.BASKET.read_text(encoding="utf-8"))
        symbols = [row["symbol"] for row in basket["symbols"]]
        self.assertEqual((basket["version"], len(symbols), len(scrape_raw.urls(symbols))), (2, 25, 106))

        sources = {"one": "https://example.test/one", "two": "https://example.test/two"}
        with tempfile.TemporaryDirectory() as directory, patch.object(scrape_raw, "fetch_raw", side_effect=[b'{"raw":1}', b'[1,2]']):
            previous = Path.cwd()
            try:
                os.chdir(directory)
                Path("config").mkdir()
                Path("config/binance_small_caps_v2.json").write_text(json.dumps({"version": 2, "symbols": []}))
                manifest = scrape_raw.capture(datetime(2026, 7, 11, 12, 42, tzinfo=timezone.utc), sources)
                payload = json.loads(manifest.read_text())
                stored = Path(payload["sources"]["one"]["path"])
                self.assertEqual(gzip.decompress(stored.read_bytes()), b'{"raw":1}')
                self.assertFalse(Path("gaps/gaps.jsonl").exists())
                self.assertEqual(json.loads(Path("current/raw_checkpoint.json").read_text())["sources_ok"], 2)
                self.assertFalse(scrape_raw.capture_due(datetime(2026, 7, 11, 12, 59, tzinfo=timezone.utc)))
                self.assertIsNone(scrape_raw.capture(datetime(2026, 7, 11, 12, 59, tzinfo=timezone.utc), sources))
            finally:
                os.chdir(previous)

    def test_github_runner_skips_binance_after_451(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {"GITHUB_ACTIONS": "true"}), patch.object(scrape_raw, "fetch_raw", return_value=b'[]'):
            previous = Path.cwd()
            try:
                os.chdir(directory)
                Path("config").mkdir()
                Path("config/binance_small_caps_v2.json").write_text(json.dumps({"version": 2, "symbols": []}))
                manifest = scrape_raw.capture(datetime(2026, 7, 11, 13, 1, tzinfo=timezone.utc))
                payload = json.loads(manifest.read_text())
                self.assertEqual((set(payload["sources"]), payload["skipped"]["count"]), ({"coingecko_markets_001_250", "coingecko_markets_251_500"}, 4))
            finally:
                os.chdir(previous)


if __name__ == "__main__":
    unittest.main()
