# -*- coding: latin-1 -*-

import boto3
import agate
import agateremote
import decimal


number = agate.Number()
text = agate.Text()
OD_BUCKET = 'open-data-germany-orgs'
GROUPS = ("bevoelkerung",
    "bildung-und-wissenschaft",
    "geographie-geologie-und-geobasisdaten",
    "gesetze-und-justiz",
    "gesundheit",
    "infrastruktur-bauen-und-wohnen",
    "kultur-freizeit-sport-tourismus",
    "oeffentliche-verwaltung-haushalt-und-steuern",
    "politik-und-wahlen",
    "soziales",
    "transport-und-verkehr",
    "umwelt-und-klima",
    "verbraucherschutz",
    "wirtschaft-und-arbeit")
RAW_STATS_COLUMN_TYPES = {
        'id': text,
        'groups': text,
        'license': number,
        'format': number,
        'update_time': number,
        'overall': number,
        }
ORG_COLUMN_TYPE = {
        'id': text,
        'name': text,
        'portal': text,
        'contact_email': text,
        'contact_person': text,
        'latitude': number,
        'longitude': number,
        'city_type': text,
        'datasets': number,
        'format_count': number,
        'open_formats': number,
        'open_formats_datasets': number,
        'open_datasets': number,
        'days_since_last_update': number,
        'days_since_start': number,
        'days_between_start_and_last_update': number,
        'category_count': number,
        'category_score': number,
        'category_variance': number,
        'open_license_and_format_count': number,
        'dataset_score': number,
        }

RAW_DATA_COLUMN_TYPES = {
        'update_time': number,
        'license': number,
        'overall': number,
        'id': number,
        }

OPEN_FORMATS = ['csv', 'xml', 'json', 'geojson', 'gml', 'rss','txt', 'tsv', 'tiff']
MACHINE_READABLE_FORMATS = ['csv', 'xml', 'json', 'geojson', 'gml', 'rss','txt', 'tsv', 'tiff', 'xlsx', 'xls']
OPEN_LICENSES = ("cc-by", "odc-by", "cc-by 3.0", "dl-de-by-2.0", "dl-de/by-2-0", "cc-by-sa 3.0", "other-open", "cc0-1.0", "cc-zero", "dl-de-zero-2.0", "andere offene lizenzen", "cc by 3.0 de", "dl-de-by-1.0", "dl-de-by 1.0", "gfdl", "odbl", "cc-by-sa", "https://creativecommons.org/licenses/by/3.0/de/", "https://www.govdata.de/dl-de/by-2-0", "cc-by-3.0", "odc-odbl", "cc-by-4.0", "https://www.govdata.de/dl-de/zero-2-0")



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
        print('city:', s3_file['Key'])
        return agate.Table.from_url(url,column_types=ORG_COLUMN_TYPE)
    return None

def join_groups(item):
    item["groups"] = ",".join(item["groups"])
    return item

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
    return(
        decimal.Decimal(0.4)*row['dataset_score_rank_std']+
        decimal.Decimal(0.2)*row['category_score_rank_std']+
        decimal.Decimal(0.15)*row['category_variance_rank_std']+
        decimal.Decimal(0.1)*row['dataset_rank_std']+
        decimal.Decimal(0.05)*row['formats_rank_std']+
        decimal.Decimal(0.05)*row['last_update_rank_std']+
        decimal.Decimal(0.05)*row['category_rank_std']
    )

def openness_score(row):
    if row["open_license_and_format_count"]>0 and row["datasets"] > 0:
        return row["open_license_and_format_count"]*100/row["datasets"]
    return None

def compute_ranks(table):
    table = table.compute([
        ('dataset_rank', agate.Rank('datasets', reverse=True)),
        ('formats_rank', agate.Rank('format_count', reverse=True)),
        #('open_formats_rank', agate.Rank('open_formats', reverse=True)),
        ('last_update_rank', agate.Rank('days_since_last_update')),
        #('open_datasets_rank', agate.Rank('open_datasets', reverse=True)),
        ('category_rank', agate.Rank('category_count', reverse=True)),
        ('category_variance_rank', agate.Rank('category_variance')),
        #('update_start_rank', agate.Rank('days_between_start_and_last_update')),
        #('start_rank', agate.Rank('days_since_start', reverse=True)),
        #('openess_score', agate.Formula(number, openness_score)),
        ('dataset_score_rank', agate.Rank('dataset_score', reverse=True)),
        ('category_score_rank', agate.Rank('category_score', reverse=True)),
    ])
    table = table.compute([
        ('dataset_rank_std', StandadizeScore('dataset_rank')),
        ('formats_rank_std', StandadizeScore('formats_rank')),
        ('last_update_rank_std', StandadizeScore('last_update_rank')),
        ('category_rank_std', StandadizeScore('category_rank')),
        ('category_variance_rank_std', StandadizeScore('category_variance_rank')),
        ('dataset_score_rank_std', StandadizeScore('dataset_score_rank')),
        ('category_score_rank_std', StandadizeScore('category_score_rank')),
        ])
    table = table.compute([
        ('overall_rank_data', agate.Formula(agate.Number(), overall_rank))
    ])
    table = table.compute([
        ('overall_rank', agate.Rank('overall_rank_data')),
        ])
    return table

def filter_ranks(table):
    include_columns = ['id','overall_rank']
    return table.select(include_columns)

def has_open_format_in_resources(data):
    resources = [resource["format"] for resource in data["resources"] ]
    return any(f.lower() in OPEN_FORMATS for f in resources)

class StandadizeScore(agate.Computation):
    def __init__(self, column_name):
        self._column_name = column_name

        super(StandadizeScore, self).__init__()

    def get_computed_data_type(self, table):
        return agate.Number()

    def run(self, table):
        new_column = []

        rank_values = [r[self._column_name] for r in table.rows]
        max_rank = max(rank_values)
        for i, row in enumerate(table.rows):
            value = row[self._column_name]
            new_column.append(value/max_rank*100)

        return new_column
