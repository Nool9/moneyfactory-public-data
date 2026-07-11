import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import scrape


class ScrapeTest(unittest.TestCase):
    def test_ephemeral_snapshot_schema(self):
        responses = [
            (b'[{"id":"coin","symbol":"c","name":"Coin","platforms":{}}]', [{"id": "coin", "symbol": "c", "name": "Coin", "platforms": {}}]),
            (b'[{"name":"THIN_USDT","funding_rate":"0.1","funding_next_apply":1,"trade_size":"1","mark_price":"2","status":"trading"}]', [{"name": "THIN_USDT", "funding_rate": "0.1", "funding_next_apply": 1, "trade_size": "1", "mark_price": "2", "status": "trading"}]),
            (b'{"asks":[{"p":"2","s":"1"}],"bids":[{"p":"1","s":"1"}]}', {"asks": [{"p": "2", "s": "1"}], "bids": [{"p": "1", "s": "1"}]}),
        ]
        with tempfile.TemporaryDirectory() as directory, patch.object(scrape, "fetch", side_effect=responses):
            previous = Path.cwd()
            try:
                os.chdir(directory)
                result = scrape.snapshot(datetime(2026, 7, 11, tzinfo=timezone.utc))
            finally:
                os.chdir(previous)
        self.assertEqual(set(result["sources"]), {"point_in_time_token_list", "small_exchange_funding", "thin_orderbook"})
        self.assertEqual(result["sources"]["thin_orderbook"]["contract"], "THIN_USDT")

    def test_rejects_non_list_source(self):
        with patch.object(scrape, "fetch", return_value=(b"{}", {})), self.assertRaises(ValueError):
            scrape.snapshot(datetime(2026, 7, 11, tzinfo=timezone.utc))


if __name__ == "__main__":
    unittest.main()
