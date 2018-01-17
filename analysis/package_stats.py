from analysis.offene_daten_api import OffeneDatenAPI
import analysis.utils as utils
import agate
import datetime

class PackageStats(object):
    def __init__(self, data):
        self.data = data
        self.package_count = len(self.data)
        self.package_resources = [resource for p in self.data for resource in p["resources"] ]
        self.package_groups = [group for p in self.data for group in self.get_group_title_from_package(p)]
        self.dataset_scores = [self.score_for_package(package) for package in self.data]
        self.open_license_and_format_count = 0
        self.open_datasets = 0
        self.format_count = 0
        self.open_format_count = 0
        self.open_formats_datasets = 0
        self.groups = 0
        self.group_score = 0
        self.groups_dataset_variance = None
        self.days_since_last_update = None
        self.days_since_start = None
        self.days_between_start_and_last_update = None
        self.dataset_score = 0
        if len(self.dataset_scores) > 0:
            self.dataset_score = sum([d["overall"] for d in self.dataset_scores])/len(self.dataset_scores)

    def raw_stats(self):
        if len(self.dataset_scores) > 0:
            return [utils.join_groups(stat) for stat in self.dataset_scores]
        return []

    def _package_resource_table(self):
        return agate.Table.from_object(filter(None, self.package_resources))

    def _package_table(self):
        return agate.Table.from_object(filter(None, self.data))

    def _group_table(self):
        return agate.Table.from_object(filter(None, self.package_groups))

    def score_for_groups(self, group_count):
        if(group_count == len(utils.GROUPS)):
            return 1
        elif(group_count >= (len(utils.GROUPS)/2)):
            return 0.5
        return 0

    def score_for_update(self, update_date):
        try:
            update_datetime = datetime.datetime.strptime(update_date, "%Y-%m-%dT%H:%M:%S.%f")
        except ValueError:
            return 0
        except TypeError:
            return 0
        else:
            today = datetime.datetime.today()
            update_date_delta = today - update_datetime
            if update_date_delta.days < 7:
                return 1
            if update_date_delta.days < 30:
                return 0.5
        return 0
    def score_for_license(self, license):
        if license.lower() in utils.OPEN_LICENSES:
            return 1
        return 0
    def score_for_format(self,file_format):
        file_format = file_format.lower()
        if file_format in utils.OPEN_FORMATS:
            if file_format in utils.MACHINE_READABLE_FORMATS:
                return 1
            return 0.5
        if file_format in utils.MACHINE_READABLE_FORMATS:
            return 0.5
        return 0

    def find_in_extras(self, package, key, default_value = 0):
        if 'extras' in package:
            for extra in package['extras']:
                if extra['key'] == key:
                    return extra['value']
        return default_value

    def score_for_package(self, package):
        score_license = self.score_for_license(package["license_id"])
        score_update_time = self.score_for_update(self.find_in_extras(package, "metadata_modified" ))
        score_formats = [self.score_for_format(resource["format"]) for resource in package["resources"]]
        score_formats_score = 0 if len(score_formats) < 1 else max(score_formats)
        return { "id": package["name"], "groups": self.get_group_title_from_package(package), "license": score_license, "format": score_formats_score, "update_time": score_update_time, "overall": score_license + score_formats_score + score_update_time}

    def _overall_stats(self):
        count_open_licenses = agate.Summary('license_id', agate.Number(), lambda r: sum(license_id in utils.OPEN_LICENSES for license_id in r.values()))

        self.overall_package_stats = self._package_table().aggregate([
            ('open_data_count', count_open_licenses),
            ])
        self.resource_stats = self._package_resource_table().compute([
            ('open_format', agate.Formula(agate.Boolean(), open_formats_count)),
            ])
        if len(self._package_resource_table()) > 0:
            self.resource_stats = self.resource_stats.aggregate([
                ('open_format_count', agate.Count('open_format', True)),
                ('min_date', agate.Min('created')),
                ('max_date', agate.Max('created'))
                ])
            format_table = self._package_resource_table().group_by("format").aggregate([
                ('count', agate.Count()),
                ])
            count = format_table.aggregate([
                ('different_formats', agate.Count()),
            ])
            self.open_datasets = self.overall_package_stats.get("open_data_count",0)
            self.open_format_count = self.resource_stats.get("open_format_count",0)
            self.format_count = count.get("different_formats", 0)
            self.compute_dates()

    def compute_dates(self):
        if self.resource_stats[ "max_date" ]:
            time_delta = datetime.datetime.today() - self.resource_stats[ "max_date" ]
            self.days_since_last_update = time_delta.days

    def get_group_title_from_package(self, package):
        if "groups" in package:
            groups = package["groups"]
            if len(groups) > 0:
                return [group['title'] for group in groups]
        return []

    def get_org_groups_aggregate(self):
        self.groups = len(set(self.package_groups))
        group_table = self._group_table()
        if(len(group_table) > 0):
            group_aggregates = group_table.group_by('a').aggregate([ ('count', agate.Count())])
            if len(group_aggregates) > 0:
                self.groups_dataset_variance = 1
                if len(group_aggregates) > 1:
                    self.groups_dataset_variance = group_aggregates.aggregate(agate.Variance('count'))
                    self.group_score = self.score_for_groups(len(group_aggregates))


def open_formats_count(row):
    file_format = row['format']
    if file_format:
        return file_format.lower() in utils.OPEN_FORMATS
    return False
