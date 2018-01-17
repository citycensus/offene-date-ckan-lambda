# -*- coding: latin-1 -*-
import sys, os, json

here = os.path.dirname(os.path.realpath(__file__))
env_path = os.path.join(here, "./od/lib/python2.7/site-packages/")
sys.path.append(env_path)

import analysis.utils as utils
from analysis.offene_daten import OffeneDaten
import simplejson as json
import boto3
import datetime
import agate
import csv

def collect_cities(city_name):
    client = boto3.client('lambda')
    payload = { "city_name": city_name }
    response = client.invoke(
        FunctionName='open-data-germany-ckan-dev-single_city',
        InvocationType='Event',
        Payload=json.dumps(payload)
    )
    print('single city service started: ', city_name)
    print(response)

def compute_ranks(event, context):
    rank_table = utils.compute_ranks(utils.collect_all_cities())
    rank_table.to_csv('/tmp/rank_table.csv')
    utils.upload_file_to_s3('open_data_germany_ranks.csv','/tmp/rank_table.csv')
    ranks = utils.filter_ranks(rank_table)
    ranks.to_json('/tmp/ranks.json')
    ranks.to_csv('/tmp/ranks.csv')
    today = datetime.date.today().strftime('%Y-%m-%d')
    utils.upload_file_to_s3('ranks/{}.json'.format(today),'/tmp/ranks.json')
    utils.upload_file_to_s3('ranks/{}.csv'.format(today),'/tmp/ranks.csv')
    body = {
        "message": "Go Serverless v1.0! Your function executed successfully!",
        "input": event
    }

    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }

    return response

def get_all_cities(event, context):
    od = OffeneDaten()
    cities = od.get_all_cities()
    cityCount = len(cities)
    city_names = [city["name"] for city in cities]
    data = {
            'job_name': 'get_all_cities',
            'date': datetime.date.today().strftime('%Y-%m-%d'),
            'org_count': cityCount,
            'cities': city_names
            }
    with open('/tmp/get_all_cities.json', 'w') as f:
        json.dump(data, f)
    utils.upload_file_to_s3('get_all_cities.json','/tmp/get_all_cities.json')
    # start the lambda services
    responses = [collect_cities(city) for city in city_names]
    body = {
        "message": "Go Serverless v1.0! Your function executed successfully!",
        "input": event
    }

    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }

    return response

def city_filename(folder, city, file_format):
    return '{}/{}.{}'.format(folder, city, file_format)

def single_city(event, context):
    city = event['city_name']
    print('single city import started: ', city)
    od = OffeneDaten()
    result = od.get_data_for_org(city)
    if len(result["table"]) > 0:
        result["table"].to_csv(city_filename('/tmp',city, 'csv'))
        result["raw_stats_table"].to_csv('/tmp/{}_raw_data.csv'.format(city))
        utils.upload_file_to_s3(city_filename('cities',city, 'csv'),city_filename('/tmp',city, 'csv'))
        utils.upload_file_to_s3('cities/package_stats/{}.csv'.format(city),'/tmp/{}_raw_data.csv'.format(city))
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
