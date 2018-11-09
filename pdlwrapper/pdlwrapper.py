"""
Log calls to PDL API into DynamoDB.
"""

import boto3
import requests
import time


class Client:
    PDL_URL = 'https://api.peopledatalabs.com/v4/person'
    TABLE_NAME = 'pdl_log'

    def __init__(self, account_id: str, env: str, aws_access_key_id: str, aws_secret_access_key: str,
                 aws_region_name: str):

        if env not in ["test", "prod"]:
            raise ValueError("env must be either test or prod")

        dynamodb_resource = boto3.resource(
            'dynamodb',
            region_name=aws_region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        self.dynamodb_table = dynamodb_resource.Table(self.TABLE_NAME)
        self.account_id = account_id
        self.env = env

    def get_person(self, params):
        resp = requests.get(self.PDL_URL, params=params)

        log_item = {
            'partition_key': f'{self.env}_{self.account_id}',
            'timestamp': int(round(time.time() * 1000000)),
            'env': self.env,
            'status_code': resp.status_code,
            'response': resp.json()
        }

        self.dynamodb_table.put_item(Item=log_item)

        return resp
