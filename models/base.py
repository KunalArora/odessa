import boto3
import redis
from os import environ


class Base(object):
    def __init__(self):
        if environ['DYNAMO_ENDPOINT_URL']:
            environ['http_proxy'] = environ['PROXY']
            self.dynamodb = boto3.resource(
                'dynamodb', endpoint_url=environ['DYNAMO_ENDPOINT_URL'])
        else:  # pragma: no cover
            self.dynamodb = boto3.resource('dynamodb')

        if environ['REDIS_ENDPOINT_URL']:
            self.elasticache = redis.StrictRedis(
                host=environ['REDIS_ENDPOINT_URL'], port=6379)
        else:
            self.elasticache = None

    def convert(self, data):
        if isinstance(data, bytes):
            return data.decode('ascii')
        if isinstance(data, dict):
            return dict(map(self.convert, data.items()))
        if isinstance(data, tuple):
            return map(self.convert, data)
        return data  # pragma: no cover
