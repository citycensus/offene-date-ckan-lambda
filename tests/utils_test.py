import unittest
import httpretty
import decimal

import analysis.utils as utils

class TestUtils(object):
    def test_overall_rank(self):
        row = { "dataset_rank": 1, "formats_rank": 1, "open_formats_rank": 1, "last_update_rank": 1, "open_datasets_rank": 1, "category_variance_rank": 1, "start_rank": 1, "category_rank": 1, "update_start_rank": 1}
        assert utils.overall_rank(row) == decimal.Decimal(1.0)

    def test_openness_score_no_l_f(self):
        row = { "open_license_and_format_count": 0}
        assert utils.openness_score(row) == None

    def test_openness_score_no_datasets(self):
        row = { "open_license_and_format_count": 2, "datasets": 0}
        assert utils.openness_score(row) == None

    def test_openness_score_no_datasets(self):
        row = { "open_license_and_format_count": 2, "datasets": 2}
        assert utils.openness_score(row) == 100
