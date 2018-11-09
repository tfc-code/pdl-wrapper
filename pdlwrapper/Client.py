"""
Log calls to PDL API into DynamoDB.
"""

import boto3
import requests
import time

PDL_URL = 'https://api.peopledatalabs.com/v4/person'
TABLE_NAME = 'pdl_log'


class Client:
    def __init__(self, campaign_id: str, env: str, aws_access_key_id: str, aws_secret_access_key: str,
                 aws_region_name: str):

        if env not in ["test", "prod"]:
            raise ValueError("env must be either test or prod")

        self.dynamodb = boto3.resource(
            'dynamodb',
            region_name=aws_region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        self.campaign_id = campaign_id
        self.env = env
        self.log_items = []

    def get_person(self, params):
        resp = requests.get(PDL_URL, params=params)

        log_item = {
            'PutRequest': {
                'Item': {
                    'partition_key': f'{self.env}_{self.campaign_id}',
                    'timestamp': int(round(time.time() * 1000)),
                    'env': self.env,
                    'status_code': resp.status_code,
                    'response': resp.json()
                }
            }
        }

        self.log_items.append(log_item)

        if len(self.log_items) == 2:
            self.dynamodb.batch_write_item(
                RequestItems={
                    TABLE_NAME: self.log_items
                }
            )
            self.log_items = []
