import os
from os import environ
import json
import yaml
import redis
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

path = os.path.dirname(__file__)


def set_env_var(self):
    with open(f'{path}/../../config/environments/local.yml', 'r') as file:
        env_vars = yaml.load(file)
        for env_var in env_vars:
            environ[env_var] = env_vars[env_var]
        environ['http_proxy'] = ''


def seed_ec_subscriptions(self):
    if environ['REDIS_ENDPOINT_URL']:
        self.elasticache = redis.StrictRedis(
            host=environ['REDIS_ENDPOINT_URL'], port=6379)

        self.elasticache.flushall()
        with open(f'{path}/../fixtures/device_subscriptions.json') as data_file:
            device_subscriptions = json.load(data_file)

        for device in device_subscriptions:
            self.elasticache.hmset(
                f'device_subscriptions:{device["id"]}:{device["oid"]}',
                {"status": device["status"],
                 "message": device["message"],
                 "created_at": device["created_at"],
                 "updated_at": device["updated_at"]
                 }
            )


def create_table(self):
    self.dynamodb = boto3.resource(
        'dynamodb', endpoint_url=environ['DYNAMO_ENDPOINT_URL'])

    with open(
            f'{path}/../../db/migrations/device_subscriptions.json'
            ) as json_file:
        schema = json.load(json_file)['Table']
    try:
        self.dynamodb.create_table(
            TableName=schema['TableName'],
            KeySchema=schema['KeySchema'],
            AttributeDefinitions=schema['AttributeDefinitions'],
            ProvisionedThroughput=schema['ProvisionedThroughput']
            )
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            self.dynamodb.Table('device_subscriptions').delete()
            self.dynamodb.create_table(
                TableName=schema['TableName'],
                KeySchema=schema['KeySchema'],
                AttributeDefinitions=schema['AttributeDefinitions'],
                ProvisionedThroughput=schema['ProvisionedThroughput']
                )
    with open(
            f'{path}/../../db/migrations/service_oids.json'
            ) as json_file:
        schema = json.load(json_file)['Table']
    try:
        self.dynamodb.create_table(
            TableName=schema['TableName'],
            KeySchema=schema['KeySchema'],
            AttributeDefinitions=schema['AttributeDefinitions'],
            ProvisionedThroughput=schema['ProvisionedThroughput']
            )
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            self.dynamodb.Table('service_oids').delete()
            self.dynamodb.create_table(
                TableName=schema['TableName'],
                KeySchema=schema['KeySchema'],
                AttributeDefinitions=schema['AttributeDefinitions'],
                ProvisionedThroughput=schema['ProvisionedThroughput']
                )


def seed_ddb_subscriptions(self):
    create_table(self)
    table = self.dynamodb.Table('device_subscriptions')
    with open(
            f'{path}/../fixtures/device_subscriptions.json'
            ) as json_file:
        device_subscriptions = json.load(json_file)
    with table.batch_writer() as batch:
        for subscription in device_subscriptions:
            id = subscription["id"]
            oid = subscription["oid"]
            status = int(subscription["status"])
            message = subscription["message"]
            created_at = subscription["created_at"]
            updated_at = subscription["updated_at"]
            batch.put_item(
                    Item={
                        'id': id,
                        'oid': oid,
                        'status': status,
                        'message': message,
                        'created_at': created_at,
                        'updated_at': updated_at
                    }
            )

    table = self.dynamodb.Table('service_oids')
    with open(
            f'{path}/../fixtures/service_oids.json'
            ) as json_file:
        service_oids = json.load(json_file)
    with table.batch_writer() as batch:
        for service_oid in service_oids:
            id = service_oid["id"]
            oids = service_oid["oids"]
            boc_service_id = service_oid["boc_service_id"]
            batch.put_item(
                    Item={
                        'id': id,
                        'oids': oids,
                        'boc_service_id': boc_service_id
                    }
            )


def clear_db(self):
    self.dynamodb.Table('device_subscriptions').delete()
    if environ['REDIS_ENDPOINT_URL']:
        self.elasticache.flushall()


def get_device(self, device_key):
    table = self.dynamodb.Table('device_subscriptions')
    return table.scan(
        FilterExpression=Key('id').begins_with(device_key)
    )


def convert(data):
    if isinstance(data, bytes):
        return data.decode('ascii')
    if isinstance(data, dict):
        return dict(map(convert, data.items()))
    if isinstance(data, tuple):
        return map(convert, data)
    return data  # pragma: no cover
