"""
Log calls to PDL API into DynamoDB.
"""

import boto3
import decimal
import requests
import time
import json


class Client:
    PDL_URL = 'https://api.peopledatalabs.com/v4/person'
    TABLE_NAME = 'pdl_log'

    def __init__(self, account_id: str, env: str, pdl_api_key: str, aws_access_key_id: str, aws_secret_access_key: str,
                 aws_region_name: str):

        if env not in ["dev", "test", "prod"]:
            raise ValueError("env must be either dev, test or prod")

        dynamodb_resource = boto3.resource(
            'dynamodb',
            region_name=aws_region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        self.dynamodb_table = dynamodb_resource.Table(self.TABLE_NAME)
        self.account_id = account_id
        self.env = env
        self.pdl_api_key = pdl_api_key

    def get_person(self, params):
        params['api_key'] = self.pdl_api_key
        resp = requests.get(self.PDL_URL, params=params)

        resp_json = None
        if resp.status_code == 200:
            try:
                resp_json = resp.json(parse_float=decimal.Decimal)
            except json.JSONDecodeError:
                pass

        log_item = {
            'partition_key': f'{self.env}_{self.account_id}',
            'timestamp': int(round(time.time() * 1000000)),
            'env': self.env,
            'status_code': resp.status_code,
            'response': resp_json
        }

        self.dynamodb_table.put_item(Item=log_item)

        return resp
