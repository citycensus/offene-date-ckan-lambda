import unittest
import httpretty
import decimal
import datetime

from analysis.package_stats import PackageStats

class TestPackageStats(unittest.TestCase):
    def setUp(self):
        today = datetime.datetime.today()
        self.today_formatted = today.strftime("%Y-%m-%dT%H:%M:%S.%f")
        data = [{ "name": "berlin", "license_id": "cc-by", "metadata_modified": self.today_formatted, "groups": [], "resources": [{"name": "einwohner","format": "CSV", "created": self.today_formatted}], "extras": [{ 'key': "metadata_modified", 'value': self.today_formatted}]}]
        self.package = PackageStats(data)
    def tearDown(self):
        self.package = None

    def test_package_count(self):
        assert self.package.package_count == 1

    """
    def test_stats(self):
        stats = {
            "open_license_and_format_count": 0,
            "open_datasets": 0,
            "format_count": 0,
            "open_format_count": 0,
            "open_formats_datasets": 0,
            "groups": 0,
            "group_score": 0,
            "group_variance": None,
            "days_since_update": None,
            "days_since_start": None,
            "days_between_start_and_last_update": None,
            "dataset_score": 0,
        }
        assert self.package.stats == stats
    """

    def test_dataset_score(self):
        assert self.package.dataset_score == 3

    def test_open_data_count_count(self):
        self.package._overall_stats()
        assert self.package.open_datasets == decimal.Decimal('1')

    def test_open_data_count(self):
        data = [{ "name": "berlin", "license_id": "", "metadata_modified": "", "groups": [], "resources": [{"format": "", "created": self.today_formatted}], "extras": [{ 'key': "metadata_modified", 'value': self.today_formatted}]}]
        self.package = PackageStats(data)
        self.package._overall_stats()
        assert self.package.open_datasets == 0

    def test_machine_readable_per_package_count(self):
        data = [{ "name": "berlin", "license_id": "", "metadata_modified": "", "groups": [], "resources": [{"format": "csv", "created": self.today_formatted}], "extras": [{ 'key': "metadata_modified", 'value': self.today_formatted}]}]
        self.package = PackageStats(data)
        self.package._overall_stats()
        print(self.package._package_resource_table())
        assert self.package.open_formats_datasets == 1

    def test_machine_readable_per_package_count_multiple(self):
        data = [{ "name": "berlin-1", "license_id": "", "metadata_modified": "", "groups": [], "resources": [{"format": "csv", "created": self.today_formatted}], "extras": [{ 'key': "metadata_modified", 'value': self.today_formatted}]},{ "name": "berlin", "license_id": "", "metadata_modified": "", "groups": [], "resources": [{"format": "csv", "created": self.today_formatted}], "extras": [{ 'key': "metadata_modified", 'value': self.today_formatted}]}]
        self.package = PackageStats(data)
        self.package._overall_stats()
        print(self.package._package_resource_table())
        assert self.package.open_formats_datasets == 2

    def test_open_format_single_package_count(self):
        self.package._overall_stats()
        assert self.package.open_format_count == 1

    def test_group_count(self):
        data = [{ "name": "berlin", "license_id": "", "metadata_modified": "", "groups": [{ "title": "test"}], "resources": [{"format": "", "created": self.today_formatted}], "extras": [{ 'key': "metadata_modified", 'value': self.today_formatted}]}]
        self.package = PackageStats(data)
        self.package.get_org_groups_aggregate()
        assert self.package.groups == 1

    def test_group_score(self):
        data = [{ "name": "berlin", "license_id": "", "metadata_modified": "", "groups": [{ "title": "test"}], "resources": [{"format": "", "created": self.today_formatted}], "extras": [{ 'key': "metadata_modified", 'value': self.today_formatted}]}]
        self.package = PackageStats(data)
        self.package.get_org_groups_aggregate()
        assert self.package.group_score == 0

    def test_group_score_max(self):
        data = [{ "name": "berlin", "license_id": "", "metadata_modified": "", "groups": [{ "title": "b"},{ "title": "be"},{ "title": "g"},{ "title": "ge"},{ "title": "i"},{ "title": "k"},{ "title": "o"},{ "title": "p"},{ "title": "s"},{ "title": "t"},{ "title": "u"},{ "title": "v"},{ "title": "ges"},{ "title": "w"}], "resources": [{"format": "", "created": self.today_formatted}], "extras": [{ 'key': "metadata_modified", 'value': self.today_formatted}]}]
        self.package = PackageStats(data)
        self.package.get_org_groups_aggregate()
        assert self.package.group_score == 1

    def test_update_score(self):
        data = [{ "name": "berlin", "license_id": "", "groups": [{ "title": "test"}], "resources": [{"format": "", "created": self.today_formatted}], "extras": [ { "key": "metadata_modified", "value": self.today_formatted}]}]
        self.package = PackageStats(data)
        assert self.package.dataset_scores[0]['update_time'] == 1

    def test_raw_stats(self):
        raw_stats = self.package.raw_stats()
        assert raw_stats[0] == { 'format': 1, 'groups':'', 'id': 'berlin', 'license': 1, 'update_time': 1, 'overall': 3}

    def test_raw_stats_empty_data(self):
        self.package = PackageStats([])
        raw_stats = self.package.raw_stats()
        assert raw_stats == []

    def test_raw_stats_groups(self):
        data = [{ "name": "berlin", "license_id": "", "groups": [{ "title": "Bildung"},{ "title": "Arbeit"}], "resources": [{"format": "", "created": self.today_formatted}], "extras": [ { "key": "metadata_modified", "value": self.today_formatted}]}]
        package = PackageStats(data)
        raw_stats = package.raw_stats()
        assert raw_stats[0]["groups"] == 'Bildung,Arbeit'
    """
    def test_open_format_multiple_package_count(self):
                body='{"success": true, "result": { "name": "berlin", "license_id": "cc-by", "resources": [{"name": "einwohner", "format": "CSV"}, { "name": "", "format": "JSON"}]}}',
                body='{"success": true, "result": { "name": "berlin", "license_id": "cc-by", "resources": [{"name": "einwohner", "format": "CSV"}]}}',
        assert self.org.stats["open_format_count"] == 3
        """
