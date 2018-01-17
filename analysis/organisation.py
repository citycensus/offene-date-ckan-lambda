from analysis.offene_daten_api import OffeneDatenAPI
from analysis.package_stats import PackageStats
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
        self.package_stats = PackageStats(self.package_data)

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
                'format_count':self.package_stats.format_count,
                'open_formats': self.package_stats.open_format_count,
                'open_formats_datasets': self.package_stats.open_formats_datasets,
                "open_datasets": self.package_stats.open_datasets,
                'days_since_last_update': self.package_stats.days_since_last_update,
                'days_since_start': self.package_stats.days_since_start,
                'days_between_start_and_last_update': self.package_stats.days_between_start_and_last_update,
                'category_count': self.package_stats.groups,
                'category_variance': self.package_stats.groups_dataset_variance,
                'category_score': self.package_stats.group_score,
                "open_license_and_format_count": self.package_stats.open_license_and_format_count,
                "dataset_score": self.package_stats.dataset_score,
            }

    def table(self):
        return agate.Table.from_object(filter(None, [self.row()]), column_types=utils.ORG_COLUMN_TYPE)

    def raw_stats_table(self):
        if len(self.get_package_raw_stats()) > 0:
            return agate.Table.from_object(filter(None, self.package_stats.raw_stats()), column_types=utils.RAW_STATS_COLUMN_TYPES)
        return agate.Table.from_object([])

    def get_org_data(self, include_datasets=False):
        self.org_data = self.API.get_org_data(self.org_id, include_datasets)

    def collect_packages_and_resources(self):
        if self.org_data == {}:
            self.get_org_data(True)
        self.package_data = [self.API.get_package_data(name) for name in self._get_package_names()]
        self.package_count = len(self.package_data)

    def get_package_raw_stats(self):
        return self.package_stats.dataset_scores

    def collect_stats(self):
        if len(self.package_data) == 0:
            self.collect_packages_and_resources()
            self.package_stats = PackageStats(self.package_data)
        if len(self.package_data) > 0:
            self.package_stats._overall_stats()
            self.package_stats.get_org_groups_aggregate()
            print(self.package_stats)
            """

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

    def _get_package_names(self):
        return [p["name"] for p in self.org_data["packages"]]

