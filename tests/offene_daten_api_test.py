import unittest
import httpretty

from analysis.offene_daten_api import OffeneDatenAPI

httpretty.HTTPretty.allow_net_connect = False

class TestOffeneDatenAPI(unittest.TestCase):
    def setUp(self):
        self.od = OffeneDatenAPI()
    def tearDown(self):
        self.od = None

    @httpretty.activate
    def test_get_all_cities(self):
        httpretty.register_uri(httpretty.POST, "https://offenedaten.de/api/action/organization_list",
                body='{"success": true, "result": []}',
                content_type="application/json")
        assert self.od.get_all_orgs() == []

    @httpretty.activate
    def get_all_orgs_single_test(self):
        httpretty.register_uri(httpretty.POST, "https://offenedaten.de/api/action/organization_list",
                body='{"success": true, "result": ["berlin"]}',
                content_type="application/json")
        assert self.od.get_all_orgs() == ['berlin']

    @httpretty.activate
    def get_org_data_test(self):
        httpretty.register_uri(httpretty.POST, "https://offenedaten.de/api/action/organization_show?id=berlin&include_datasets=False",
                body='{"success": true, "result": { "count": 0, name": "berlin", "extras": [{ "key": "city_type", "value": "Stadt"}]}}',
                content_type="application/json")
        assert self.od.get_org_data('berlin') == { "count": 0, "name": "berlin", "extras": [{ "key": "city_type", "value": "Stadt"}]}

    @httpretty.activate
    def get_org_data_include_datasets_test(self):
        httpretty.register_uri(httpretty.POST, "https://offenedaten.de/api/action/organization_show?id=berlin&include_datasets=True",
                body='{"success": true, "result": { "count": 0, "name": "berlin", "packages": [], "extras": [{ "key": "city_type", "value": "Stadt"}]}}',
                content_type="application/json")
        assert self.od.get_org_data('berlin') == { "count": 0, "name": "berlin", "packages": [], "extras": [{ "key": "city_type", "value": "Stadt"}]}


    @httpretty.activate
    def test_get_org_data_include_datasets_multiple_pages(self):
        httpretty.register_uri(httpretty.POST, "https://offenedaten.de/api/action/organization_show?id=berlin&include_datasets=True",
                body='{"success": true, "result": { "package_count": 1002, "name": "berlin", "packages": [{"id": "1"}], "extras": [{ "key": "city_type", "value": "Stadt"}]}}',
                content_type="application/json")
        httpretty.register_uri(httpretty.POST, "https://offenedaten.de/api/action/package_search",
                body='{"success": true, "result": { "count": 1002, "results": [{"id": "2"}]',
                content_type="application/json")
        print(httpretty.last_request())
        assert len(self.od.get_org_data('berlin', True)["packages"]) == 10

    @httpretty.activate
    def get_package_data_test(self):
        httpretty.register_uri(httpretty.POST, "https://offenedaten.de/api/action/package_show?id=berlin-data",
                body='{"success": true, "result": { "name": "berlin", "resources": []}}',
                content_type="application/json")
        assert self.od.get_package_data('berlin') == { "name": "berlin", "resources": []}

