from ckanapi import RemoteCKAN
import csv
ua = 'ckanapiexample/1.0 (+http://example.com/my/website)'

od = RemoteCKAN('https://offenedaten.de')
orgs = od.action.organization_list()
with open('orgs.csv', 'wb') as csvfile:
    spamwriter = csv.writer(csvfile)
    for org in orgs:
	od_org = od.action.organization_show(id=org)
        is_city = False
	for extra in od_org['extras']:
            if extra['key'] == 'latitude':
                latitude= extra['value']
            if extra['key'] == 'longitude':
                longitude = extra['value']
            if extra['key'] == 'open_data_portal':
                portal = extra['value']
            if extra['key'] == 'city_type':
                if extra['value'] == 'Stadt':
                    is_city = True
        if is_city:
            spamwriter.writerow([od_org['display_name'].encode('utf-8'),od_org['created'].encode('utf-8'), portal, latitude, longitude])
