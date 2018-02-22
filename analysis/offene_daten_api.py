from ckanapi import RemoteCKAN

class OffeneDatenAPI(object):
    def __init__(self):
        self.od = RemoteCKAN('https://offenedaten.de')

    def get_package_data(self, package_name):
        return self.od.action.package_show(id=package_name)

    def get_all_orgs(self):
        return self.od.action.organization_list()

    def get_org_data(self, org_id, include_datasets = False):
        datasets = self.od.action.organization_show(id=org_id, include_datasets=include_datasets)
        if include_datasets and datasets["package_count"] > 1000:
            pages = datasets["package_count"] / 1000
            for page in range(1,pages+1):
                additional_datasets = self.get_org_packages(org_id, page)
                datasets["packages"] = datasets["packages"] + additional_datasets['results']
        return datasets

    def get_org_packages(self, org_id, page):
        search = "organization:{}".format(org_id)
        return self.od.action.package_search(fq=search, rows=1000, start=(1000*page))

