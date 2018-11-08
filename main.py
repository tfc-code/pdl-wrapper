"""
Log calls to PDL API into DynamoDB.
"""

__author__ = "Tech For Campaigns"
__version__ = "0.1.0"
__license__ = "MIT"

import boto3
import requests
import time

PDL_URL = 'https://api.peopledatalabs.com/v4/person'


class PDLWrapper:
    def __init__(self, campaign_id, env, aws_access_key_id, aws_secret_access_key, region_name):
        dynamodb = boto3.resource(
            'dynamodb',
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        self.dynamodb_table = dynamodb.Table('pdl_log')
        self.campaign_id = campaign_id
        self.env = env

    def get_person(self, params):
        print("hello world")

        resp = requests.get(PDL_URL, params=params)

        print(self.dynamodb_table.item_count)

        item = {
            'partition_key': f'{self.env}_{self.campaign_id}',
            'timestamp': int(round(time.time() * 1000)),
            'env': self.env,
            'status_code': resp.status_code,
            'response': resp.json()
        }

        self.dynamodb_table.put_item(Item=item)
