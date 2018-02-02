import json
import boto3
import logging
import sys
from botocore.exceptions import ClientError
from os import environ

logger = logging.getLogger('tokens')
logger.setLevel(logging.INFO)


def get_one_time_token(event, context):
    client = boto3.client('sts')
    try:
        response = client.assume_role(
            RoleArn=environ['LAMBDA_ROLE_ARN'],
            RoleSessionName='CMPS',
            Policy='{"Version":"2012-10-17","Statement":[{"Sid":"AssumedRole","Effect":"Allow","Action":"execute-api:Invoke","Resource":"*"}]}'
            )['Credentials']

        token = {
            'session_token': response['SessionToken'],
            'secret_access_key': response['SecretAccessKey'],
            'access_key_id': response['AccessKeyId'],
            'expiration': response['Expiration'].strftime('%Y-%m-%dT%H:%M:%S')
        }
        return {
            'statusCode': 200,
            'body': json.dumps(token)
            }
    except ClientError as e:
        logger.error(e)
        return {'statusCode': 403}
    except:
        logger.error(sys.exc_info())
        return {'statusCode': 500}
