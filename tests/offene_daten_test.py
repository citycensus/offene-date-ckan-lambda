import unittest
import httpretty
import agate
import decimal

from analysis.offene_daten import OffeneDaten,open_formats_count

class TestOffeneDaten(object):
    def test_init(self):
        od = OffeneDaten()
        assert od.org_data == []

    @httpretty.activate
    def test_get_all_cities(self):
        httpretty.register_uri(httpretty.POST, "https://offenedaten.de/api/action/organization_list",
                body='{"success": true, "result": []}',
                content_type="application/json")
        od = OffeneDaten()
        assert od.get_all_cities() == []
        assert od.org_data == []

    @httpretty.activate
    def test_get_all_not_a_city(self):
        httpretty.register_uri(httpretty.POST, "https://offenedaten.de/api/action/organization_list",
                body='{"success": true, "result": ["land"]}',
                content_type="application/json")
        httpretty.register_uri(httpretty.POST, "https://offenedaten.de/api/action/organization_show?id=land&include_datasets=False",
                body='{"success": true, "result": { "name": "land", "extras": [{ "key": "city_type", "value": "Land"}]}}',
                content_type="application/json")
        od = OffeneDaten()
        assert od.get_all_cities() == []

    @httpretty.activate
    def test_get_all_city(self):
        httpretty.register_uri(httpretty.POST, "https://offenedaten.de/api/action/organization_list",
                body='{"success": true, "result": ["berlin"]}',
                content_type="application/json")
        httpretty.register_uri(httpretty.POST, "https://offenedaten.de/api/action/organization_show?id=berlin&include_datasets=False",
                body='{"success": true, "result": { "name": "berlin", "extras": [{ "key": "city_type", "value": "Stadt"}]}}',
                content_type="application/json")
        od = OffeneDaten()
        assert od.get_all_cities() == [{ "name": "berlin", "extras": [{ "key": "city_type", "value": "Stadt"}]}]

    @httpretty.activate
    def test_get_all_cities_multiple(self):
        httpretty.register_uri(httpretty.POST, "https://offenedaten.de/api/action/organization_list",
                body='{"success": true, "result": ["land", "berlin"]}',
                content_type="application/json")
        httpretty.register_uri(httpretty.POST, "https://offenedaten.de/api/action/organization_show?id=berlin&include_datasets=False",
                body='{"success": true, "result": { "name": "berlin", "extras": [{ "key": "city_type", "value": "Stadt"}]}}',
                content_type="application/json")
        httpretty.register_uri(httpretty.POST, "https://offenedaten.de/api/action/organization_show?id=land&include_datasets=False",
                body='{"success": true, "result": { "name": "land", "extras": [{ "key": "city_type", "value": "Land"}]}}',
                content_type="application/json")
        od = OffeneDaten()
        assert od.get_all_cities() == [{ "name": "berlin", "extras": [{ "key": "city_type", "value": "Stadt"}]}]

    def test_open_formats_count(self):
        assert open_formats_count({'format': None}) == False

    def test_open_formats_count(self):
        assert open_formats_count({'format': 'CSV'}) == True

    def test_get_org_format_aggregate(self):
        od = OffeneDaten()
        rows = [{'format': 'CSV'}, {'format': ''},{'format': None}, {'format': 'PDF'},{'format': 'JSON'}]
        table = agate.Table.from_object(rows)
        result = od.get_org_format_aggregates(table)
        assert result["open_formats"] == 2
    def test_get_org_format_aggregate_detailed(self):
        od = OffeneDaten()
        rows = [{'format': 'CSV'}, {'format': 'csv'},{'format': 'json'}, {'format': 'PDF'},{'format': 'JSON'}]
        table = agate.Table.from_object(rows)
        result = od.get_org_format_aggregates(table)
        assert result["open_formats_datasets"] == 4

    def test_get_org_group_aggregates_group_count(self):
        od = OffeneDaten()
        group1 = { 'title': 'Bildung'}
        group2 = { 'title': 'Wirtschaft'}
        rows = [{'groups': [group1]},{ 'groups': [group1, group2]} ]
        result = od.get_org_groups_aggregate(rows)
        assert result["groups"] == 2

    def test_get_org_group_aggregates_group_datasets(self):
        od = OffeneDaten()
        group1 = { 'title': 'Bildung'}
        group2 = { 'title': 'Wirtschaft'}
        rows = [{'groups': [group1]},{ 'groups': [group1, group2]} ]
        result = od.get_org_groups_aggregate(rows)
        assert result["groups_dataset_variance"] == decimal.Decimal('0.5')

    def test_get_open_datasets(self):
        od = OffeneDaten()
        package_data = [{"isopen": True},{"isopen": False}, {"isopen": True}]
        assert len(od.get_open_datasets(package_data)) == 2

    def test_get_open_formats_and_license(self):
        od = OffeneDaten()
        package_data = [{"isopen": True, "resources": [{"format": "CSV"}]},{"isopen": False, "resources": []}, {"isopen": True, "resources": [{"format": "XLS"}]}]
        assert len(od.get_open_formats_and_license(package_data)) == 1

