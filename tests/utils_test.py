import unittest
import httpretty
import decimal

import stats.utils as utils

class TestUtils(object):
    def test_overall_rank(self):
        row = { "dataset_rank": 1, "formats_rank": 1, "open_formats_rank": 1, "last_update_rank": 1, "open_datasets_rank": 1}
        assert utils.overall_rank(row) == decimal.Decimal(1.0)

