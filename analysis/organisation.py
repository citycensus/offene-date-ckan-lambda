from analysis.offene_daten_api import OffeneDatenAPI
import analysis.utils as utils
import agate

class Organisation(object):
    def __init__(self, org_id):
        self.API = OffeneDatenAPI()
        self.org_id = org_id
        self.org_data = {}
        self.display_name = ''
        self.created = ''
        self.package_data = []
        self.package_count = len(self.package_data)
        self.package_resources = []
        self.stats = {
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

    def row(self):
        return {
                'id': self.org_id,
                'name': self.display_name,
                'created_at': self.created,
                'portal': '',
                'datasets': self.package_count,
                'latitude': 0,
                'longitude': 0,
                'contact_person': '',
                'contact_email': '',
                'city_type': '',
                'format_count':self.stats["format_count"],
                'open_formats': self.stats["open_format_count"],
                'open_formats_datasets': self.stats["open_formats_datasets"],
                "open_datasets": self.stats["open_datasets"],
                'days_since_last_update': self.stats['days_since_update'],
                'days_since_start': self.stats["days_since_start"],
                'days_between_start_and_last_update': self.stats["days_between_start_and_last_update"],
                'category_count': self.stats["groups"],
                'category_variance': self.stats["group_variance"],
                'category_score': self.stats["group_score"],
                "open_license_and_format_count": self.stats["open_license_and_format_count"],
                "dataset_score": self.stats["dataset_score"],
            }

    def table(self):
        agate.Table.from_object(filter(None, rows), column_types=utils.ORG_COLUMN_TYPE)

    def get_org_data(self, include_datasets=False):
        self.org_data = self.API.get_org_data(self.org_id, include_datasets)
        print(self.org_data)

    def collect_packages_and_resources(self):
        if self.org_data == {}:
            self.get_org_data(True)
        self.package_data = [self.API.get_package_data(name) for name in self._get_package_names()]
        if len(self.package_data) > 0:
            self._get_package_resources()

    def collect_stats(self):
        if len(self.package_data) == 0:
            self.collect_packages_and_resources()
        if len(self.package_data) > 0:
            if len(self.package_resources) > 0:
                package_table = self._package_table()
                resources_table = self._package_resource_table()
                self._overall_package_stats()
                """

                package_stats = self.get_package_stats(p_table)
                format_aggregates = self.get_org_format_aggregates(package_table)
                date_aggregates = self.get_package_date_aggregates(package_table)
                group_aggregates = self.get_org_groups_aggregate(package_data)
                dataset_open_stats = self.get_open_stats(package_data)
                dataset_package_stats = self.get_package_stats_aggregates(package_data)
                days_since_last_update = None
                days_since_start = None
                if date_aggregates[ "max_date" ]:
                    time_delta = datetime.datetime.today()- date_aggregates[ "max_date" ]
                    days_since_last_update = time_delta.days
                if date_aggregates[ "min_date" ]:
                    time_delta_start = datetime.datetime.today()- date_aggregates["min_date"]
                    days_since_start = time_delta_start.days
                if date_aggregates[ "max_date" ] and date_aggregates[ "min_date" ]:
                    time_delta_start_update = date_aggregates['max_date'] - date_aggregates["min_date"]
                    days_between_start_and_last_update = time_delta_start_update.days
                stats["open_license_and_format_count"] = dataset_open_stats.get("open_data_count",0)
                stats["open_datasets"] = package_stats.get("open_data_count",0)
                stats["format_count"] = format_aggregates.get("different_formats",0)
                stats["open_format_count"] = format_aggregates.get("open_formats",0)
                stats["open_formats_datasets"] = format_aggregates.get("open_formats_datasets",0)
                stats["groups"] = group_aggregates.get("groups",0)
                stats["group_variance"] = group_aggregates.get("groups_dataset_variance",None)
                stats["group_score"] = group_aggregates.get("group_score",0)
                stats["days_since_update"] = days_since_last_update
                stats["days_since_start"] = days_since_start
                stats["days_between_start_and_last_update"] = days_between_start_and_last_update
                stats["dataset_score"] = dataset_package_stats["package_score"]
                """

    def _get_package_resources(self):
        self.package_resources = [resource for p in self.package_data for resource in p["resources"] ]

    def _package_resource_table(self):
        return agate.Table.from_object(filter(None, self.package_resources))

    def _package_table(self):
        return agate.Table.from_object(filter(None, self.package_data))

    def _overall_package_stats(self):
        count_open_licenses = agate.Summary('license_id', agate.Number(), lambda r: sum(license_id in utils.OPEN_LICENSES for license_id in r.values()))
        self.overall_package_stats = self._package_table().aggregate([
            ('open_data_count', count_open_licenses)
            ])
        self.stats['open_datasets'] = self.overall_package_stats.get("open_data_count",0)

    def _get_package_names(self):
        return [p["name"] for p in self.org_data["packages"]]
