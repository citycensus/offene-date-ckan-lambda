import unittest
import httpretty

import orgs

class TestOrgs(object):
    def test_resource_data_type_empty(self):
        result = orgs.resource_data_types({"resources": []})
        assert result == []

    def test_resource_data_type_(self):
        result = orgs.resource_data_types({"resources": [{ "format": "csv"}, { "format": "csv"}]})
        assert result == ['csv']

    def test_get_datasets_detail(self):
        data = orgs.get_datasets_detail({"packages": [1,2,3]})
        assert len(data) == 3

    def test_get_org_resoureces(self):
        data = { "packages": [{ "resources": [{"name": "test"}, { "name": "San Diego"}]}, { "resources": [{"name": "SD"}]}]}
        result = orgs.get_org_resources(data)
        assert len(result) == 3

    @httpretty.activate
    def test_org_data(self):
        httpretty.register_uri(httpretty.POST, "https://offenedaten.de/api/action/organization_list",
                body='{"success": true, "result": []}',
                content_type="application/json")
        od = orgs.OffeneDaten()
        table = od.create_org_table()
        assert (httpretty.has_request())

    @httpretty.activate
    def test_org_table(self):
        packages = '[{ "resources": [{"name": "defibrillatoren-in-moers"}]}]'
        httpretty.register_uri(httpretty.POST, "https://offenedaten.de/api/action/organization_list",
                body='{"success": true, "result": ["sd"]}',
                content_type="application/json")
        httpretty.register_uri(httpretty.POST, "https://offenedaten.de/api/action/organization_show?id=sd",
                body='{"success": true, "result": {"display_name": "SD", "created": "", "package_count": 0, "packages": [{ "name": "defibrillatoren-in-moers"}], "extras": [{"key": "city_type", "value": "Stadt"}]}}',
                content_type="application/json")
        httpretty.register_uri(httpretty.POST, "https://offenedaten.de/api/action/package_show?id=defibrillatoren-in-moers",
                body='{"success": true, "result": {"isopen": true, "resources": [{"format": "xls", "created": "2013-04-19T17:41:57.186024"}]}}',
                content_type="application/json")
        od = orgs.OffeneDaten()
        table = od.create_org_table()
        assert len(table) == 1
        assert (httpretty.has_request())

    @httpretty.activate
    def test_packages_table(self):
        packages = '[{ "resources": [{"name": "defibrillatoren-in-moers"}]}]'
        httpretty.register_uri(httpretty.POST, "https://offenedaten.de/api/action/organization_list",
                body='{"success": true, "result": ["sd"]}',
                content_type="application/json")
        httpretty.register_uri(httpretty.POST, "https://offenedaten.de/api/action/organization_show?id=sd",
                body='{"success": true, "result": {"display_name": "SD", "created": "", "package_count": 0, "packages": [{ "name": "defibrillatoren-in-moers"}], "extras": [{"key": "city_type", "value": "Stadt"}]}}',
                content_type="application/json")
        httpretty.register_uri(httpretty.POST, "https://offenedaten.de/api/action/package_show?id=defibrillatoren-in-moers",
                body='{"success": true, "result": {"isopen": true, "resources": [{"format": "xls", "created": "2013-04-19T17:41:57.186024"}, {"format": "xls", "created": "2013-04-19T17:41:57.186024"}, {"format": "XML", "created": "2017-04-19T17:41:57.186024"}]}}',
                content_type="application/json")
        od = orgs.OffeneDaten()
        table = od.create_org_table()
        od.compute_ranks()
        od.orgs_table.print_table(max_columns=None)
        assert table.rows[0]["format_count"] == 2
        assert (httpretty.has_request())
