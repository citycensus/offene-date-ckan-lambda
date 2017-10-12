import sys, os, json

here = os.path.dirname(os.path.realpath(__file__))
env_path = os.path.join(here, "./od/lib/python2.7/site-packages/")
sys.path.append(env_path)

from ckanapi import RemoteCKAN
import csv
import agate
import simplejson as json
import boto3

ua = 'ckanapiexample/1.0 (+http://example.com/my/website)'
stadt_types = ('Stadt', 'Landeshauptstadt', 'Freie und Hansestadt')
od = RemoteCKAN('https://offenedaten.de')

def upload_file_to_s3(bucket_file, local_file):
    s3 = boto3.resource('s3')
    s3.Object('open-data-germany-orgs', bucket_file).put(Body=open(local_file, 'rb'))

def get_org_detail(org_id):
    od_org = od.action.organization_show(id=org_id)
    is_city = False
    org = {
            'name': od_org['display_name'],
            'created_at': od_org['created'],
            'portal': '',
            'datasets': od_org['package_count'],
            'latitude': 0,
            'longitude': 0,
            'contact_person': '',
            'contact_email': ''
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
            if extra['value'] in stadt_types:
                is_city = True
    if is_city:
        return(org)

def organisations(event, context):
    orgs = od.action.organization_list()
    rows = [get_org_detail(org) for org in orgs]
    table = agate.Table.from_object(filter(None, rows))
    table.to_csv('/tmp/open_data_germany.csv')
    upload_file_to_s3('open_data_cities.csv','/tmp/open_data_germany.csv')
    aggregates = table.aggregate([
            ('count', agate.Count()),
            ('sum', agate.Sum('datasets'))
            ])
    with open('/tmp/summary.json', 'w') as f:
        json.dump(aggregates, f)
    upload_file_to_s3('open_data_cities_summary.json','/tmp/summary.json')
    body = {
        "message": "Go Serverless v1.0! Your function executed successfully!",
        "input": event
    }

    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }

    return response
organisations({},{})
