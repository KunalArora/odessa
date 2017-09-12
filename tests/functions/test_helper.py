import os
from os import environ
import json
import yaml
import redis
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from botocore.client import Config

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
        with open(f'{path}/../fixtures/subscriptions/device_subscriptions.json') as data_file:
            device_subscriptions = json.load(data_file)

        for device in device_subscriptions:
            if 'oids' in device:
                oids = device["oids"]
            self.elasticache.hmset(
                f'device_subscriptions:{device["id"]}',
                {"oids": oids,
                 "status": device["status"],
                 "message": device["message"],
                 "created_at": device["created_at"],
                 "updated_at": device["updated_at"]
                 }
            )


def seed_ec_device_logs(self):
    if environ['REDIS_ENDPOINT_URL']:
        self.elasticache = redis.StrictRedis(
            host=environ['REDIS_ENDPOINT_URL'], port=6379)

        self.elasticache.flushall()
        with open(f'{path}/../fixtures/logs_and_notifications/device_logs.json') as data_file:
            device_logs = json.load(data_file)

        for log in device_logs:
            self.elasticache.hmset(
                f'device_logs:{log["id"]}',
                {"timestamp": log["timestamp"],
                 "value": log["value"]
                }
            )


def seed_ec_device_notifications(self):
    if environ['REDIS_ENDPOINT_URL']:
        self.elasticache = redis.StrictRedis(
            host=environ['REDIS_ENDPOINT_URL'], port=6379)
        self.elasticache.flushall()
        self.elasticache.hmset("device_network_status:ffffffff-ffff-ffff-ffff-ffffffff0001", { "id": "ffffffff-ffff-ffff-ffff-ffffffff0001",
                                                                       "timestamp": "2017-01-12T12:45:06",
                                                                       "status": "online"
    })


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
    with open(
            f'{path}/../../db/migrations/device_network_statuses.json'
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
            self.dynamodb.Table('device_network_statuses').delete()
            self.dynamodb.create_table(
                TableName=schema['TableName'],
                KeySchema=schema['KeySchema'],
                AttributeDefinitions=schema['AttributeDefinitions'],
                ProvisionedThroughput=schema['ProvisionedThroughput']
                )
    with open(
            f'{path}/../../db/migrations/device_logs.json'
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
            self.dynamodb.Table('device_logs').delete()
            self.dynamodb.create_table(
                TableName=schema['TableName'],
                KeySchema=schema['KeySchema'],
                AttributeDefinitions=schema['AttributeDefinitions'],
                ProvisionedThroughput=schema['ProvisionedThroughput']
                )
    with open(
            f'{path}/../../db/migrations/reporting_registrations.json'
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
            self.dynamodb.Table('reporting_registrations').delete()
            self.dynamodb.create_table(
                TableName=schema['TableName'],
                KeySchema=schema['KeySchema'],
                AttributeDefinitions=schema['AttributeDefinitions'],
                ProvisionedThroughput=schema['ProvisionedThroughput']
                )
    with open(
            f'{path}/../../db/migrations/device_email_logs.json'
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
            self.dynamodb.Table('device_email_logs').delete()
            self.dynamodb.create_table(
                TableName=schema['TableName'],
                KeySchema=schema['KeySchema'],
                AttributeDefinitions=schema['AttributeDefinitions'],
                ProvisionedThroughput=schema['ProvisionedThroughput']
                )


def seed_ddb_device_settings(self):
    create_table(self)
    table = self.dynamodb.Table('service_oids')
    with open(
            f'{path}/../fixtures/subscriptions/service_oids.json'
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


def seed_ddb_subscriptions(self):
    create_table(self)
    table = self.dynamodb.Table('device_subscriptions')
    with open(
            f'{path}/../fixtures/subscriptions/device_subscriptions.json'
            ) as json_file:
        device_subscriptions = json.load(json_file)
    with table.batch_writer() as batch:
        for subscription in device_subscriptions:
            id = subscription["id"]
            if 'oids' in subscription:
                oids = subscription["oids"]
            status = int(subscription["status"])
            message = subscription["message"]
            created_at = subscription["created_at"]
            updated_at = subscription["updated_at"]
            batch.put_item(
                    Item={
                        'id': id,
                        'oids': oids,
                        'status': status,
                        'message': message,
                        'created_at': created_at,
                        'updated_at': updated_at
                    }
            )

    table = self.dynamodb.Table('service_oids')
    with open(
            f'{path}/../fixtures/subscriptions/service_oids.json'
            ) as json_file:
        service_oids = json.load(json_file)
    with table.batch_writer() as batch:
        for service_oid in service_oids:
            id = service_oid["id"]
            oids = service_oid["oids"]
            boc_service_id = service_oid["boc_service_id"]
            callback_url = service_oid["callback_url"]
            batch.put_item(
                    Item={
                        'id': id,
                        'oids': oids,
                        'boc_service_id': boc_service_id,
                        'callback_url': callback_url
                    }
            )


def seed_ddb_device_logs(self):
    create_table(self)
    table = self.dynamodb.Table('device_subscriptions')
    with open(
            f'{path}/../fixtures/logs_and_notifications/device_subscriptions.json'
            ) as json_file:
        device_subscriptions = json.load(json_file)
    with table.batch_writer() as batch:
        for subscription in device_subscriptions:
            id = subscription["id"]
            if 'oids' in subscription:
                oids = subscription["oids"]
            status = int(subscription["status"])
            message = subscription["message"]
            created_at = subscription["created_at"]
            updated_at = subscription["updated_at"]
            batch.put_item(
                    Item={
                        'id': id,
                        'oids': oids,
                        'status': status,
                        'message': message,
                        'created_at': created_at,
                        'updated_at': updated_at
                    }
            )

    table = self.dynamodb.Table('service_oids')
    with open(
            f'{path}/../fixtures/logs_and_notifications/service_oids.json'
            ) as json_file:
        service_oids = json.load(json_file)
    with table.batch_writer() as batch:
        for service_oid in service_oids:
            id = service_oid["id"]
            oids = service_oid["oids"]
            batch.put_item(
                    Item={
                        'id': id,
                        'oids': oids
                    }
            )
    table = self.dynamodb.Table('device_logs')
    with open(
            f'{path}/../fixtures/logs_and_notifications/device_logs.json'
            ) as json_file:
        device_logs = json.load(json_file)
    with table.batch_writer() as batch:
        for log in device_logs:
            id = log["id"]
            timestamp = log["timestamp"]
            value = log["value"]
            batch.put_item(
                    Item={
                        'id': id,
                        'timestamp': timestamp,
                        'value': value
                    }
            )

    table = self.dynamodb.Table('device_network_statuses')
    with open(
            f'{path}/../fixtures/logs_and_notifications/device_network_statuses.json'
            ) as json_file:
        device_network_statuses = json.load(json_file)
    with table.batch_writer() as batch:
        for device_status in device_network_statuses:
            id = device_status["id"]
            timestamp = device_status["timestamp"]
            status = device_status["status"]
            batch.put_item(
                    Item={
                        'id': id,
                        'timestamp': timestamp,
                        'status': status
                    }
            )


def seed_ddb_device_notifications(self):
    seed_ddb_device_logs(self)


def seed_ddb_history_logs(self):
    create_table(self)
    table = self.dynamodb.Table('service_oids')
    with open(
            f'{path}/../fixtures/history_logs/service_oids.json'
            ) as json_file:
        service_oids = json.load(json_file)
    with table.batch_writer() as batch:
        for service_oid in service_oids:
            id = service_oid["id"]
            oids = service_oid["oids"]
            batch.put_item(
                    Item={
                        'id': id,
                        'oids': oids
                    }
            )

    table = self.dynamodb.Table('device_subscriptions')
    with open(
            f'{path}/../fixtures/history_logs/device_subscriptions.json'
            ) as json_file:
        device_subscriptions = json.load(json_file)
    with table.batch_writer() as batch:
        for subscription in device_subscriptions:
            id = subscription["id"]
            if 'oids' in subscription:
                oids = subscription["oids"]
            else:
                oids = None
            status = int(subscription["status"])
            message = subscription["message"]
            created_at = subscription["created_at"]
            updated_at = subscription["updated_at"]
            if oids:
                batch.put_item(
                    Item={
                        'id': id,
                        'oids': oids,
                        'status': status,
                        'message': message,
                        'created_at': created_at,
                        'updated_at': updated_at
                    }
                )
            else:
                batch.put_item(
                    Item={
                        'id': id,
                        'status': status,
                        'message': message,
                        'created_at': created_at,
                        'updated_at': updated_at
                    }
                )

    table = self.dynamodb.Table('device_logs')
    with open(
            f'{path}/../fixtures/history_logs/device_logs.json'
            ) as json_file:
        device_logs = json.load(json_file)
    with table.batch_writer() as batch:
        for log in device_logs:
            id = log["id"]
            timestamp = log["timestamp"]
            value = log["value"]
            batch.put_item(
                    Item={
                        'id': id,
                        'timestamp': timestamp,
                        'value': value
                    }
            )


def seed_ddb_reporting_registrations(self):
    self.dynamodb = boto3.resource(
        'dynamodb', endpoint_url=environ['DYNAMO_ENDPOINT_URL'])
    table = self.dynamodb.Table('service_oids')
    with open(
            f'{path}/../fixtures/reporting_registrations/service_oids.json'
            ) as json_file:
        service_oids = json.load(json_file)
    with table.batch_writer() as batch:
        for service_oid in service_oids:
            id = service_oid["id"]
            oids = service_oid["oids"]
            batch.put_item(
                    Item={
                        'id': id,
                        'oids': oids
                    }
            )
    table = self.dynamodb.Table('device_subscriptions')
    with open(
            f'{path}/../fixtures/reporting_registrations/device_subscriptions.json'
            ) as json_file:
        device_subscriptions = json.load(json_file)
    with table.batch_writer() as batch:
        for subscription in device_subscriptions:
            id = subscription["id"]
            if 'oids' in subscription:
                oids = subscription["oids"]
            else:
                oids = None
            status = int(subscription["status"])
            message = subscription["message"]
            created_at = subscription["created_at"]
            updated_at = subscription["updated_at"]
            if oids:
                batch.put_item(
                    Item={
                        'id': id,
                        'oids': oids,
                        'status': status,
                        'message': message,
                        'created_at': created_at,
                        'updated_at': updated_at
                    }
                )
            else:
                batch.put_item(
                    Item={
                        'id': id,
                        'status': status,
                        'message': message,
                        'created_at': created_at,
                        'updated_at': updated_at
                    }
                )


def clear_db(self):
    self.dynamodb.Table('device_subscriptions').delete()
    self.dynamodb.Table('device_logs').delete()
    self.dynamodb.Table('device_network_statuses').delete()
    self.dynamodb.Table('service_oids').delete()
    self.dynamodb.Table('reporting_registrations').delete()
    self.dynamodb.Table('device_email_logs').delete()

def clear_cache(self):
    if environ['REDIS_ENDPOINT_URL']:
        self.elasticache.flushall()


def delete_db_data(self):
    table = self.dynamodb.Table('device_network_statuses')
    with open(
            f'{path}/../fixtures/logs_and_notifications/device_network_statuses.json'
            ) as json_file:
        device_network_statuses = json.load(json_file)
    with table.batch_writer() as batch:
        for device_status in device_network_statuses:
            id = device_status['id']
            timestamp = device_status['timestamp']
            if id == 'ffffffff-ffff-ffff-ffff-ffffffff0001':
                batch.delete_item(
                    Key={
                        'id': id,
                        'timestamp': timestamp
                    }
                )

    table = self.dynamodb.Table('device_logs')
    with open(
            f'{path}/../fixtures/logs_and_notifications/device_logs.json'
            ) as json_file:
        device_logs = json.load(json_file)
    with table.batch_writer() as batch:
        for log in device_logs:
            id = log["id"]
            timestamp = log["timestamp"]
            if (id.split('#')[0]) == 'ffffffff-ffff-ffff-ffff-ffffffff0001':
                batch.delete_item(
                        Key={
                            'id': id,
                            'timestamp': timestamp
                        }
                )

def seed_s3(self):
    self.s3client = boto3.resource('s3', endpoint_url=environ['S3_ENDPOINT_URL'],
                            aws_access_key_id=environ['S3_ACCESS_KEY'],
                            aws_secret_access_key=environ['S3_SECRET_KEY'],
                            config=Config(signature_version='s3v4'),
                            )
    try:
        bucket = self.s3client.create_bucket(Bucket='email-test')
        bucket.put_object(Key='DL-FB XML.eml', Body=open('tests/data/email/DL-FB XML.eml', 'rb'))
        bucket.put_object(Key='DL-FB CSV.eml', Body=open('tests/data/email/DL-FB CSV.eml', 'rb'))
    except ClientError as err:
        error_code = err.response['Error']['Code']
        if error_code == 'BucketAlreadyOwnedByYou':
            bucket = self.s3client.Bucket('email-test')
            bucket.put_object(Key='DL-FB XML.eml', Body=open('tests/data/email/DL-FB XML.eml', 'rb'))
            bucket.put_object(Key='DL-FB CSV.eml', Body=open('tests/data/email/DL-FB CSV.eml', 'rb'))


def delete_s3_bucket(self):
    self.s3client = boto3.resource('s3', endpoint_url=environ['S3_ENDPOINT_URL'],
                            aws_access_key_id=environ['S3_ACCESS_KEY'],
                            aws_secret_access_key=environ['S3_SECRET_KEY'],
                            config=Config(signature_version='s3v4'),
                            )
    try:
        bucket = self.s3client.Bucket('email-test')
        for key in bucket.objects.all():
            key.delete()
        bucket.delete()
    except ClientError as err:
        bucket = self.s3client.Bucket('email-test')
        for key in bucket.objects.all():
            key.delete()
        bucket.delete()

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
