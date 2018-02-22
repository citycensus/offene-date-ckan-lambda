# -*- coding: latin-1 -*-
import sys, os, json

here = os.path.dirname(os.path.realpath(__file__))
env_path = os.path.join(here, "./od/lib/python2.7/site-packages/")
sys.path.append(env_path)

from analysis.offene_daten import OffeneDaten
from analysis.offene_daten_api import OffeneDatenAPI
import simplejson as json
import boto3


'''
data = API.get_org_data("hamburg")
split_up
start lambda for every split
save the result in s3

receive s3 events, check if all files are done

self.package_data = [self.API.get_package_data(name) for name in self._get_package_names()]
'''

def get_package_ids(city):
    API = OffeneDatenAPI()
    data = API.get_org_data(city, True)

    return [p["name"] for p in data["packages"]]


def chunk_up(items, chunk):
    return zip(*[iter(items)]*chunk)

def start_map_reduce(event, context):
    city = event['city_name']
    job_bucket = '{}-open-data-sources'.format(city)
    client = boto3.client('lambda')
    all_package_ids = get_package_ids(city)
    chunk = 500
    chunk_package_ids = chunk_up(all_package_ids, chunk)

    job_info = { 'city': city, 'chunks': len(chunk_package_ids) }
    with open('/tmp/jobs.json', 'w') as f:
        json.dump(job_info, f)

    s3 = boto3.resource('s3')
    s3.Object(job_bucket, 'jobs.json').put(Body=open('/tmp/jobs.json', 'rb'))

    for index, package_ids in enumerate(chunk_package_ids):

        resp = client.invoke(
            FunctionName='open-data-germany-ckan-dev-mapper',
            InvocationType = 'Event',
            Payload =  json.dumps({
                "bucket": '',
                "package_ids": package_ids,
                "jobBucket": job_bucket,
                "jobId": city,
                "mapperId": index
            })
        )
        print(resp)
        return {}

def mapper(event, context):
    API = OffeneDatenAPI()
    job_bucket = event['jobBucket']
    src_bucket = event['bucket']
    package_ids = event['package_ids']
    job_id = event['jobId']
    mapper_id = event['mapperId']

    package_data = [API.get_package_data(name) for name in package_ids]
    p_filename = '/tmp/package_data.json'
    with open(p_filename, 'w') as f:
        json.dump(package_data, f)
    s3 = boto3.resource('s3')
    s3.Object(job_bucket, '{}_package_data_{}.json'.format(job_id, mapper_id)).put(Body=open(p_filename, 'rb'))

def reducer(event, context):
    city = event['city_name']
    print('single city import started: ', city)
    org = Organisation(org_id)
    #org.package_data = collectdatafrombucketfiles
    org.collect_stats()
    result['table'] = org.table()
    result['raw_stats_table'] = org.raw_stats_table()
    if len(result["table"]) > 0:
        result["table"].to_csv(city_filename('/tmp',city, 'csv'))
        result["raw_stats_table"].to_csv('/tmp/{}_raw_data.csv'.format(city))
        raw_means = means(result["raw_stats_table"])
        with open('/tmp/{}_raw_data_means.json'.format(city), 'w') as f:
            json.dump(raw_means, f)
        utils.upload_file_to_s3(city_filename('cities',city, 'csv'),city_filename('/tmp',city, 'csv'))
        utils.upload_file_to_s3('package_stats/{}.csv'.format(city),'/tmp/{}_raw_data.csv'.format(city))
        utils.upload_file_to_s3('package_stats_means/{}.json'.format(city),'/tmp/{}_raw_data_means.json'.format(city))
    data = {
            'job_name': 'collect_single_city',
            'date': datetime.date.today().strftime('%Y-%m-%d'),
            'city': city
            }
    with open(city_filename('/tmp',city, 'json'), 'w') as f:
        json.dump(data, f)
    utils.upload_file_to_s3(city_filename('cities',city, 'json'),city_filename('/tmp',city, 'json'))
    body = {
        "message": "Go Serverless v1.0! Your function executed successfully!",
        "input": event
    }

    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }

    return response

def coordinator(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']


    obj = s3.get_object(Bucket=bucket, Key='jobs.json')
    job_data = json.loads(obj['Body'].read())
    mappers = job_data["chunks"]
    job_id = job_data["city"]

    files = s3_client.list_objects(Bucket=bucket, Prefix=job_id)["Contents"]


    mapper_keys = get_mapper_files(files)
    resp = lambda_client.invoke(
        FunctionName='open-data-germany-ckan-dev-reducer',
        InvocationType = 'Event',
        Payload =  json.dumps({
            "bucket": bucket,
            "keys": batch,
            "jobBucket": bucket,
            "jobId": job_id,
            "nReducers": n_reducers,
            "stepId": step_id,
            "reducerId": i
        })
    )
