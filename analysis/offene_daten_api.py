from ckanapi import RemoteCKAN

class OffeneDatenAPI(object):
    def __init__(self):
        self.od = RemoteCKAN('https://offenedaten.de')

    def get_package_data(self, package_name):
        return self.od.action.package_show(id=package_name)

    def get_all_orgs(self):
        return self.od.action.organization_list()

    def get_org_data(self, org_id, include_datasets = False):
        return self.od.action.organization_show(id=org_id, include_datasets=include_datasets)

