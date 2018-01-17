import unittest
import httpretty
import decimal
import agate

from analysis.organisation import Organisation

class TestOrganisation(unittest.TestCase):
    def setUp(self):
        self.org = Organisation('berlin')
    def tearDown(self):
        self.org = None

    def test_row(self):
        result = {
                'id': 'berlin',
                'name': '',
                'created_at': '',
                'portal': '',
                'datasets': 0,
                'latitude': 0,
                'longitude': 0,
                'contact_person': '',
                'contact_email': '',
                'city_type': '',
                'format_count': 0,
                'open_formats': 0,
                'open_formats_datasets': 0,
                "open_datasets": 0,
                'days_since_last_update': None,
                'days_since_start': None,
                'days_between_start_and_last_update': None,
                'category_count': 0,
                'category_variance': None,
                'category_score': 0,
                "open_license_and_format_count": 0,
                "dataset_score": 0,
            }
        assert self.org.row() == result

    @httpretty.activate
    def test_open_data_count_count(self):
        httpretty.register_uri(httpretty.POST, "https://offenedaten.de/api/action/organization_show?id=berlin&include_datasets=True",
                body='{"success": true, "result": { "name": "berlin", "packages": [{"name": "berlin"}], "extras": [{ "key": "city_type", "value": "Stadt"}]}}',
            content_type="application/json")
        httpretty.register_uri(httpretty.POST, "https://offenedaten.de/api/action/package_show?id=berlin",
                body='{"success": true, "result": { "name": "berlin", "license_id": "cc-by", "groups": [], "resources": [{"name": "einwohner","format": "", "created": "2017-12-20T14:05:18.921473"}]}}',
            content_type="application/json")
        self.org.collect_stats()
        assert self.org.row()["open_datasets"] == decimal.Decimal('1')

    @httpretty.activate
    def test_open_data_count(self):
        httpretty.register_uri(httpretty.POST, "https://offenedaten.de/api/action/organization_show?id=berlin&include_datasets=True",
            body='{"success": true, "result": { "name": "berlin", "packages": [], "extras": [{ "key": "city_type", "value": "Stadt"}]}}',
            content_type="application/json")
        httpretty.register_uri(httpretty.POST, "https://offenedaten.de/api/action/package_show?id=berlin",
                body='{"success": true, "result": { "name": "berlin", "groups": [], "resources": ["format": "", "created": ""]}}',
            content_type="application/json")
        self.org.collect_stats()
        assert self.org.row()["open_datasets"] == 0

    @httpretty.activate
    def test_open_format_single_package_count(self):
        httpretty.register_uri(httpretty.POST, "https://offenedaten.de/api/action/organization_show?id=berlin&include_datasets=True",
            body='{"success": true, "result": { "name": "berlin", "packages": [{"name": "berlin"}], "extras": [{ "key": "city_type", "value": "Stadt"}]}}',
            content_type="application/json")
        httpretty.register_uri(httpretty.POST, "https://offenedaten.de/api/action/package_show?id=berlin",
                body='{"success": true, "result": { "name": "berlin", "license_id": "cc-by", "groups": [], "resources": [{"name": "einwohner", "format": "CSV", "created": "2017-12-20T14:05:18.921473"}]}}',
            content_type="application/json")
        self.org.collect_stats()
        assert self.org.row()["open_formats"] == 1
    @httpretty.activate
    def test_open_format_multiple_package_count(self):
        httpretty.register_uri(httpretty.POST, "https://offenedaten.de/api/action/organization_show?id=berlin&include_datasets=True",
                body='{"success": true, "result": { "name": "berlin", "packages": [{"name": "berlin"}, { "name": "berlin-2"}], "extras": [{ "key": "city_type", "value": "Stadt"}]}}',
            content_type="application/json")
        httpretty.register_uri(httpretty.POST, "https://offenedaten.de/api/action/package_show?id=berlin",
                body='{"success": true, "result": { "name": "berlin", "license_id": "cc-by", "groups": [], "resources": [{"name": "einwohner", "format": "CSV"}, { "name": "", "format": "JSON", "created": "2017-12-20T14:05:18.921473"}]}}',
            content_type="application/json")
        httpretty.register_uri(httpretty.POST, "https://offenedaten.de/api/action/package_show?id=berlin-2",
                body='{"success": true, "result": { "name": "berlin", "license_id": "cc-by", "groups": [], "resources": [{"name": "einwohner", "format": "CSV", "created": "2017-12-20T14:05:18.921473"}]}}',
            content_type="application/json")
        self.org.collect_stats()
        assert self.org.row()["open_formats"] == 3

    @httpretty.activate
    def test_raw_stats_no_packages(self):
        httpretty.register_uri(httpretty.POST, "https://offenedaten.de/api/action/organization_show?id=berlin&include_datasets=True",
            body='{"success": true, "result": { "name": "berlin", "packages": [], "extras": [{ "key": "city_type", "value": "Stadt"}]}}',
            content_type="application/json")
        self.org.collect_stats()
        assert self.org.get_package_raw_stats() == []

    @httpretty.activate
    def test_raw_stats_table_no_packages(self):
        httpretty.register_uri(httpretty.POST, "https://offenedaten.de/api/action/organization_show?id=berlin&include_datasets=True",
            body='{"success": true, "result": { "name": "berlin", "packages": [], "extras": [{ "key": "city_type", "value": "Stadt"}]}}',
            content_type="application/json")
        self.org.collect_stats()
        assert len(self.org.raw_stats_table()) == 0
