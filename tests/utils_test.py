import unittest
import httpretty
import decimal

import analysis.utils as utils

class TestUtils(object):
    def test_overall_rank(self):
        row = { "dataset_rank_std": 1, "formats_rank_std": 1, "open_formats_rank_std": 1, "last_update_rank_std": 1, "open_datasets_rank_std": 1, "category_variance_rank_std": 1, "start_rank_std": 1, "category_rank_std": 1, "update_start_rank_std": 1, "dataset_score_rank_std": 1, "category_score_rank_std": 1}
        assert int(utils.overall_rank(row)) == 1

    def test_openness_score_no_l_f(self):
        row = { "open_license_and_format_count": 0}
        assert utils.openness_score(row) == None

    def test_openness_score_no_datasets(self):
        row = { "open_license_and_format_count": 2, "datasets": 0}
        assert utils.openness_score(row) == None

    def test_openness_score_no_datasets(self):
        row = { "open_license_and_format_count": 2, "datasets": 2}
        assert utils.openness_score(row) == 100
