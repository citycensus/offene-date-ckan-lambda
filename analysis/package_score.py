import analysis.utils as utils
import agate
import datetime

class PackageScore(object):
    def __init__(self, data):
        self.package = data
        self.score = 0
        self.id = ""
        self.groups = []
        self.license_score = 0
        self.format_score = 0
        self.update_time_score = 0
        self.has_open_format = False
        self.has_machine_readable_format = False
        self.has_open_and_machine_readable_format = False

    def score_package(self):
        self.groups = self.get_group_title_from_package(self.package)
        self.license_score = self.score_for_license(self.package["license_id"])
        self.update_time_score = self.score_for_update(self.find_in_extras(self.package, "metadata_modified" ))
        score_formats = [self.score_for_format(resource["format"]) for resource in self.package["resources"]]
        formats_om = self.has_machine_readable_and_open_formats(self.package["resources"])
        self.has_open_format = formats_om['open']
        self.has_machine_readable_format= formats_om['machine_readable']
        self.has_open_and_machine_readable_format = formats_om['open_machine']
        self.format_score = 0 if len(score_formats) < 1 else max(score_formats)
        self.score = self.license_score + self.update_time_score + self.format_score

    def to_object(self):
        return(
            {
                "id": self.package["name"],
                "groups": self.groups,
                "license": self.license_score,
                "format": self.format_score,
                "update_time": self.update_time_score,
                "overall": self.score,
                "has_open_format": self.has_open_format,
                "has_machine_readable": self.has_machine_readable_format,
                "has_open_machine_formats": self.has_open_and_machine_readable_format
            })

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
            print('updatetimedelta')
            print(update_date_delta.days)
            if update_date_delta.days < 7:
                return 1
            if update_date_delta.days < 30:
                return 0.5
        return 0

    def find_in_extras(self, package, key, default_value = 0):
        if 'extras' in package:
            for extra in package['extras']:
                if extra['key'] == key:
                    return extra['value']
        return default_value

    def has_machine_readable_and_open_formats(self, resources):
        open_formats = False
        machine_readable_formats = False
        open_an_machine_readable_formats = False
        for resource in resources:
            if resource["format"] in utils.OPEN_FORMATS:
                open_formats = True
            if resource["format"] in utils.MACHINE_READABLE_FORMATS:
                machine_readable_formats = True
            if resource["format"] in utils.MACHINE_READABLE_FORMATS and resource["format"] in utils.OPEN_FORMATS:
                open_an_machine_readable_formats = True
        return { 'open': open_formats, 'machine_readable': machine_readable_formats, 'open_machine': open_an_machine_readable_formats }

    def get_group_title_from_package(self, package):
        if "groups" in package:
            groups = package["groups"]
            if len(groups) > 0:
                return [group['title'] for group in groups]
        return []

    def find_in_extras(self, package, key, default_value = 0):
        if 'extras' in package:
            for extra in package['extras']:
                if extra['key'] == key:
                    return extra['value']
        return default_value

