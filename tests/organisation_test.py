import unittest
import httpretty
import decimal

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
                body='{"success": true, "result": { "name": "berlin", "license_id": "cc-by", "resources": [{"name": "einwohner"}]}}',
            content_type="application/json")
        self.org.collect_stats()
        assert self.org.stats["open_datasets"] == decimal.Decimal('1')

    @httpretty.activate
    def test_open_data_count(self):
        httpretty.register_uri(httpretty.POST, "https://offenedaten.de/api/action/organization_show?id=berlin&include_datasets=True",
            body='{"success": true, "result": { "name": "berlin", "packages": [], "extras": [{ "key": "city_type", "value": "Stadt"}]}}',
            content_type="application/json")
        httpretty.register_uri(httpretty.POST, "https://offenedaten.de/api/action/package_show?id=berlin",
                body='{"success": true, "result": { "name": "berlin", "resources": []}}',
            content_type="application/json")
        self.org.collect_stats()
        assert self.org.stats["open_datasets"] == 0
