import unittest
from unittest.mock import patch

import scrape


class ScrapeTest(unittest.TestCase):
    def test_rejects_non_json_list(self):
        with patch.object(scrape, "fetch", return_value=(b"{}", {})), self.assertRaises(ValueError):
            scrape.main()


if __name__ == "__main__":
    unittest.main()
