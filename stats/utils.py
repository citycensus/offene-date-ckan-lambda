# -*- coding: latin-1 -*-

import boto3
import agate
import agateremote
import decimal


number = agate.Number()
text = agate.Text()
OD_BUCKET = 'open-data-germany-orgs'
ORG_COLUMN_TYPE = { 'name': text, 'format_count': number, 'open_formats': number, 'datasets': number, 'open_datasets': number, 'days_since_last_update': number, 'portal': text, 'contact_email': text, 'contact_person': text, 'latitude': number, 'longitude': number, 'city_type': text }

OPEN_FORMATS = ['csv', 'xml', 'json', 'geojson', 'gml', 'rss','txt']

def upload_file_to_s3(bucket_file, local_file):
    s3 = boto3.resource('s3')
    s3.Object(OD_BUCKET, bucket_file).put(Body=open(local_file, 'rb'))

def transform_file(s3_file):
    client = boto3.client('s3')
    if '.csv' in s3_file['Key']:
        url = client.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': OD_BUCKET,
                'Key': s3_file['Key']
            }
        )
        return agate.Table.from_url(url,column_types=ORG_COLUMN_TYPE)
    return None

def collect_all_cities():
    client = boto3.client('s3')
    data = client.list_objects(Bucket=OD_BUCKET, Prefix='cities')
    tables = filter(None,[transform_file(s3_file) for s3_file in data['Contents']])
    return agate.Table.merge(tables)

"""
5 Anzahl
10 Anzahl pro Kategorie
30 Zeit seit dem letzten Update
10 Start
10 letztes Update - Start
20 Anzahl offener Daten
5 Anzahl verschiedener Formate
10 Anzahl offener Formate (csv,geojson)
"""
def overall_rank(row):
    #return decimal.Decimal(0.05)*row['dataset_rank']+ decimal.Decimal(0.05)*row['formats_rank']+ decimal.Decimal(0.1)*row['open_formats_rank']+ decimal.Decimal(0.3)*row['last_update_rank']+ decimal.Decimal(0.2)*row['open_datasets_rank']
    return decimal.Decimal(0.15)*row['dataset_rank']+ decimal.Decimal(0.15)*row['formats_rank']+ decimal.Decimal(0.2)*row['open_formats_rank']+ decimal.Decimal(0.2)*row['last_update_rank']+ decimal.Decimal(0.3)*row['open_datasets_rank']

def compute_ranks(table):
    table = table.compute([
        ('dataset_rank', agate.Rank('datasets', reverse=True)),
        ('formats_rank', agate.Rank('format_count', reverse=True)),
        ('open_formats_rank', agate.Rank('open_formats', reverse=True)),
        ('last_update_rank', agate.Rank('days_since_last_update')),
        ('open_datasets_rank', agate.Rank('open_datasets', reverse=True)),
    ])
    table = table.compute([
        ('overall_rank_data', agate.Formula(number, overall_rank)),
        ])
    table = table.compute([
        ('overall_rank', agate.Rank('overall_rank_data')),
        ])
    return table
