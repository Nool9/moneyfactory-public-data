import gzip
import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import scrape_raw


class RawScrapeTest(unittest.TestCase):
    def test_versioned_basket_and_raw_gzip_capture(self):
        basket = json.loads(scrape_raw.BASKET.read_text(encoding="utf-8"))
        symbols = [row["symbol"] for row in basket["symbols"]]
        source_urls = scrape_raw.urls(symbols)
        self.assertEqual((basket["version"], len(symbols), len(source_urls)), (2, 25, 111))
        self.assertEqual(set(scrape_raw.ANNOUNCEMENTS), {"announcements_binance_cms", "announcements_kucoin", "announcements_okx", "announcements_bybit"})
        self.assertEqual(
            source_urls["coingecko_volume_001_250"],
            "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=volume_desc&per_page=250&page=1&sparkline=false",
        )
        self.assertNotIn("coingecko_volume_251_500", source_urls)

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
                self.assertEqual(payload["sources"]["one"]["first_seen"], "2026-07-11T12:42:00Z")
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
                self.assertEqual((set(payload["sources"]), payload["skipped"]["count"]), (set(scrape_raw.ANNOUNCEMENTS) | {"coingecko_markets_001_250", "coingecko_markets_251_500", "coingecko_volume_001_250"}, 4))
            finally:
                os.chdir(previous)

    def test_top250_content_is_captured_once_without_content_retry(self):
        response = MagicMock()
        response.read.return_value = b"not-json"
        response.__enter__.return_value = response
        source_urls = {
            "coingecko_volume_001_250": "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=volume_desc&per_page=250&page=1&sparkline=false"
        }
        with tempfile.TemporaryDirectory() as directory, patch.object(scrape_raw.urllib.request, "urlopen", return_value=response) as urlopen, patch.object(scrape_raw.time, "sleep"):
            previous = Path.cwd()
            try:
                os.chdir(directory)
                Path("config").mkdir()
                Path("config/binance_small_caps_v2.json").write_text(json.dumps({"version": 2, "symbols": []}))
                manifest = scrape_raw.capture(datetime(2026, 7, 21, 21, 1, tzinfo=timezone.utc), source_urls)
                payload = json.loads(manifest.read_text())
                self.assertEqual(urlopen.call_count, 1)
                self.assertEqual(payload["sources"]["coingecko_volume_001_250"]["status"], "ok")
                stored = Path(payload["sources"]["coingecko_volume_001_250"]["path"])
                self.assertEqual(gzip.decompress(stored.read_bytes()), b"not-json")
                self.assertFalse(Path("gaps/gaps.jsonl").exists())
            finally:
                os.chdir(previous)

    def test_raw_html_source(self):
        response = MagicMock()
        response.read.return_value = b"<html>raw</html>"
        response.__enter__.return_value = response
        with patch.object(scrape_raw.urllib.request, "urlopen", return_value=response):
            self.assertEqual(scrape_raw.fetch_raw("https://example.test", expect_json=False), b"<html>raw</html>")


if __name__ == "__main__":
    unittest.main()
