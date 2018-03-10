# -*- coding: latin-1 -*-
import sys, os, json

here = os.path.dirname(os.path.realpath(__file__))
env_path = os.path.join(here, "./od/lib/python2.7/site-packages/")
sys.path.append(env_path)

from analysis.offene_daten import OffeneDaten
from analysis.organisation import Organisation
from analysis.offene_daten_api import OffeneDatenAPI
import simplejson as json
import boto3

def get_package_ids(city):
    API = OffeneDatenAPI()
    data = API.get_org_data(city, True)

    return [p["name"] for p in data["packages"]]

def chunk_up(items, chunk):
    return zip(*[iter(items)]*chunk)

def start_map_reduce(event, context):
    city = event['city_name']
    job_bucket = 'hamburg-open-data-sources'
    client = boto3.client('lambda')
    all_package_ids = get_package_ids(city)
    chunk = 500
    chunk_package_ids = chunk_up(all_package_ids, chunk)

    job_info = { 'city': city, 'chunks': len(chunk_package_ids) }
    s3 = boto3.resource('s3')
    s3.Object(job_bucket, 'jobs/{}.json'.format(city)).put(Body=json.dumps(job_info))

    for index, package_ids in enumerate(chunk_package_ids):

        resp = client.invoke(
            FunctionName='open-data-germany-ckan-dev-mapper',
            InvocationType = 'Event',
            Payload =  json.dumps({
                "package_ids": package_ids,
                "jobBucket": job_bucket,
                "jobId": city,
                "mapperId": index
            })
        )
        print(resp)
    return { 'city': city, 'chunks': len(chunk_package_ids)}

def mapper(event, context):
    API = OffeneDatenAPI()
    job_bucket = event['jobBucket']
    package_ids = event['package_ids']
    job_id = event['jobId']
    mapper_id = event['mapperId']

    package_data = [API.get_package_data(name) for name in package_ids]
    s3 = boto3.resource('s3')
    s3.Object(job_bucket, '{}/package_data_{}.json'.format(job_id, mapper_id)).put(Body=json.dumps(package_data))
    return { 'city': job_id, 'index': mapper_id}

def reducer(event, context):
    city = event['city_name']
    bucket = event['bucket']
    print('single city import started: ', city)
    data = get_data(bucket, city)
    org = Organisation(city)
    org.get_org_data()
    org.set_package_data(data)
    org.collect_stats()
    result = {'table': org.table(), 'raw_stats_table': org.raw_stats_table()}
    print(result['raw_stats_table'])
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

def get_data(bucket, job_id):
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects')
    pages = paginator.paginate(Bucket=bucket, Prefix='{}/'.format(job_id))
    data = []
    for page in pages:
        for job in page['Contents']:
            if '.json' in job['Key']:
                obj = s3_client.get_object(Bucket=bucket, Key=job['Key'])
                job_data = json.loads(obj['Body'].read())
                data = data + job_data
    return data

def get_jobs(bucket):
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects')
    jobs = paginator.paginate(Bucket=bucket, Prefix='jobs/')
    job_list = []
    for page in jobs:
        for job in page['Contents']:
            if '.json' in job['Key']:
                obj = s3_client.get_object(Bucket=bucket, Key=job['Key'])
                job_data = json.loads(obj['Body'].read())
                job_list.append(job_data)
    return job_list

def file_counts(bucket, job_id):
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects')
    pages = paginator.paginate(Bucket=bucket, Prefix='{}/'.format(job_id))
    return reduce(lambda page_count, s: page_count+s, [len(page['Contents']) for page in pages])

def coordinator(event, context):
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects')
    bucket = event['Records'][0]['s3']['bucket']['name']

    for job in get_jobs(bucket):
        job_files = file_counts(bucket, job['city'])
        if job_files == job['chunks']:
            resp = lambda_client.invoke(
                FunctionName='open-data-germany-ckan-dev-reducer',
                InvocationType = 'Event',
                Payload =  json.dumps({
                    "bucket": bucket,
                    "city_name": job['city'],
                })
            )
