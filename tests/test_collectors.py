import json
import unittest
from datetime import datetime, timezone

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../ingestion')))

from noaa_collect import utc_timestamp

class TestNOAACollector(unittest.TestCase):
    def test_utc_timestamp_valid(self):
        ts_str, dt = utc_timestamp("2026-09-25T13:33:33Z")
        self.assertEqual(ts_str, "2026-09-25T13:33:33Z")

if __name__ == "__main__":
    unittest.main()
