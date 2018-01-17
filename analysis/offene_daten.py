# -*- coding: latin-1 -*-

import datetime
import decimal
from ckanapi import RemoteCKAN
import csv
import agate
import simplejson as json
import boto3
import analysis.utils as utils
from analysis.offene_daten_api import OffeneDatenAPI
from analysis.organisation import Organisation

stadt_types = ('Stadt', 'Landeshauptstadt', 'Freie und Hansestadt', 'Hansestadt', u'Universitätsstadt', 'Verbandsgemeinde', 'Kreisstadt') #Landkreis ?
number = agate.Number()

def open_formats_count(row):
    file_format = row['format']
    if file_format:
        return file_format.lower() in utils.OPEN_FORMATS
    return False

class OffeneDaten(object):
    def __init__(self):
        self.od = RemoteCKAN('https://offenedaten.de')
        self.org_data = []
        self.orgs_table = []
        self.orgs = []

    def get_all_cities(self):
        self.get_all_orgs()
        return filter(None, [self.get_city_org(org) for org in self.orgs])

    def get_data_for_org(self, org_id):
        org = Organisation(org_id)
        org.collect_stats()
        return {'table': org.table(), 'raw_stats_table': org.raw_stats_table()}

    def get_data_for_orgs(self, org_ids):
        self.orgs = org_ids
        rows = filter(None, [self.transform_org(org) for org in self.orgs])
        if len(rows) > 0:
            self.orgs_table = agate.Table.from_object(filter(None, rows), column_types=utils.ORG_COLUMN_TYPE)
            return self.orgs_table

    def get_data_for_all_orgs(self):
        self.get_all_orgs()
        return self.get_data_for_orgs(self.org_data)

    """
    private functions follow
    """
    def get_all_orgs(self):
        self.orgs = self.od.action.organization_list()

    def get_org_data(self, org_id, include_datasets = False):
        return self.od.action.organization_show(id=org_id, include_datasets=include_datasets)

    def get_city_org(self, org_name):
        org = self.get_org_data(org_name)
        if self.is_city(org):
            return org
        return None

    def is_city(self,city_data):
        for extra in city_data['extras']:
            if extra['key'] == 'city_type':
                if extra['value'] in stadt_types:
                    return True
                else:
                    return False
        return False

    def transform_org(self, org_name):
        org_data = self.get_org_data(org_name, True)
        org = self.get_org_detail(org_data)
        if org:
            stats = self.collect_org_stats(org_data)
            if "format_count" in stats:
                org['format_count'] = stats["format_count"]
                org['open_formats'] = stats["open_format_count"]
                org['open_formats_datasets'] = stats["open_formats_datasets"]
                org["open_datasets"] = stats["open_datasets"]
                org['days_since_last_update'] = stats['days_since_update']
                org['days_since_start'] = stats["days_since_start"]
                org['days_between_start_and_last_update'] = stats["days_between_start_and_last_update"]
                org['category_count'] = stats["groups"]
                org['category_variance'] = stats["group_variance"]
                org['category_score'] = stats["group_score"]
                org["open_license_and_format_count"] = stats["open_license_and_format_count"]
                org["dataset_score"] = stats["dataset_score"]
            return(org)

    def create_org_table_for_org(self, org_id):
        org = self.get_org_data(org_id, True)
        rows = [self.transform_org(org)]
        if len(rows) > 0:
            self.orgs_table = agate.Table.from_object(filter(None, rows), column_types=utils.ORG_COLUMN_TYPE)
            return self.orgs_table

    def create_org_table(self):
        if len(self.org_data) < 1:
            self.collect_org_data()
        rows = [self.transform_org(org) for org in self.org_data]
        if len(rows) > 0:
            self.orgs_table = agate.Table.from_object(filter(None, rows), column_types=utils.ORG_COLUMN_TYPE)
            return self.orgs_table

    def compute_ranks(self):
        self.orgs_table = self.orgs_table.compute([
            ('dataset_rank', agate.Rank('datasets')),
            ('formats_rank', agate.Rank('format_count')),
            ('open_formats_rank', agate.Rank('datasets')),
            ('last_update_rank', agate.Rank('days_since_last_update')),
            ('open_datasets_rank', agate.Rank('open_datasets')),
        ])
        self.orgs_table = self.orgs_table.compute([
            ('overall_rank_data', agate.Formula(number, overall_rank)),
            ])
        self.orgs_table = self.orgs_table.compute([
            ('overall_rank', agate.Rank('overall_rank_data', reverse=True)),
            ])
    def collect_org_data(self):
        if len(self.orgs) < 1:
            self.get_all_orgs()
        self.org_data = [self.get_org_data(org, True) for org in self.orgs]

    def collect_org_stats(self, org_data):
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
        if "packages" in org_data:
            stats = self.add_package_stats(stats, org_data)
        return stats

    def add_package_stats(self, stats, org_data):
        package_data = [self.get_package_data(name) for name in self.get_package_names(org_data["packages"])]
        if len(package_data) > 0:
            package_table = self.create_org_resources(package_data)
            if len(package_table) > 0:
                p_table = agate.Table.from_object(filter(None, package_data))
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

        return stats

    def get_package_names(self, packages):
        return [p["name"] for p in packages]

    def get_open_stats(self, package_data):
        table = agate.Table.from_object(self.get_open_formats_and_license(package_data))
        return table.aggregate([
            ('open_data_count', agate.Count())
        ])
    def get_package_stats(self, package_table):
        count_open_licenses = agate.Summary('license_id', agate.Number(), lambda r: sum(license_id in utils.OPEN_LICENSES for license_id in r.values()))
        return package_table.aggregate([
            ('open_data_count', count_open_licenses)
            ])
    def get_package_data(self, package_name):
        return self.od.action.package_show(id=package_name)

    def get_package_stats_aggregates(self, package_data):
        data = [self.score_for_package(package) for package in package_data]
        if len(data) > 0:
            return { "package_score": sum(data)/len(data)}
        return {"package_score": 0}
    def get_package_date_aggregates(self, package_table):
        return package_table.aggregate([
            ('min_date', agate.Min('created')),
            ('max_date', agate.Max('created'))
            ])
    def get_org_format_aggregates(self, package_table):
        # format can not exist!?!
        format_table = package_table.group_by("format").aggregate([
            ('count', agate.Count()),
            ])
        open_format_table = package_table.compute([
            ('open_format', agate.Formula(agate.Boolean(), open_formats_count))
        ])
        open_format_table_aggregates = open_format_table.aggregate([
            ('open_formats', agate.Count('open_format', True)),
            ])
        new_table = format_table.compute([
            ('open_format', agate.Formula(agate.Boolean(), open_formats_count))
        ])
        count = new_table.aggregate([
            ('different_formats', agate.Count()),
            ('open_formats', agate.Count('open_format', True)),
        ])
        count["open_formats_datasets"] = open_format_table_aggregates["open_formats"]
        return count

    def get_org_groups_aggregate(self, package_data):
        package_groups = [group for p in package_data for group in self.get_group_title_from_package(p)]
        group_table = agate.Table.from_object(package_groups)
        result = {}
        if(len(group_table) > 0):
            group_aggregates = group_table.group_by('a').aggregate([ ('count', agate.Count())])
            if len(group_aggregates) > 0:
                result = { "groups": 1, "groups_dataset_variance": 1}
                if len(group_aggregates) > 1:
                    result = {
                            "groups": len(set(package_groups)),
                            "groups_dataset_variance": group_aggregates.aggregate(agate.Variance('count')),
                            "group_score": self.score_for_groups(len(group_aggregates))
                        }
        return result

    def get_group_title_from_package(self, package):
        groups = package["groups"]
        if len(groups) > 0:
            return [group['title'] for group in groups]
        return []

    def create_org_resources(self, package_data):
        resources = [resource for p in package_data for resource in p["resources"] ]
        return agate.Table.from_object(filter(None, resources))

    def get_open_datasets(self, package_data):
        return filter(lambda x: x["isopen"], package_data)

    def get_open_formats_and_license(self, package_data):
        open_license = self.get_open_datasets(package_data)
        return filter(lambda x: utils.has_open_format_in_resources(x), open_license)

    def get_org_detail(self,od_org):
        org = {
                'id': od_org['name'],
                'name': od_org['display_name'],
                'created_at': od_org['created'],
                'portal': '',
                'datasets': od_org['package_count'],
                'latitude': 0,
                'longitude': 0,
                'contact_person': '',
                'contact_email': '',
                'city_type': ''
            }
        for extra in od_org['extras']:
            if extra['key'] == 'latitude':
                org['latitude']= extra['value']
            elif extra['key'] == 'longitude':
                org['longitude'] = extra['value']
            elif extra['key'] == 'contact_person':
                org['contact_person'] = extra['value']
            elif extra['key'] == 'contact_email':
                org['contact_email'] = extra['value']
            elif extra['key'] == 'open_data_portal':
                org['portal'] = extra['value']
            elif extra['key'] == 'city_type':
                org['city_type'] = extra['value']
        return(org)

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
    def score_for_package(self, package):
        score_license = self.score_for_license(package["license_id"])
        score_update_time = self.score_for_update(package["metadata_modified"])
        score_formats = [self.score_for_format(resource["format"]) for resource in package["resources"]]
        score_formats_score = 0 if len(score_formats) < 1 else max(score_formats)
        return score_license + score_formats_score + score_update_time

