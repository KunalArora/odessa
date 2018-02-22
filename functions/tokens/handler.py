import datetime
import json
import jwt
import logging
import os

logger = logging.getLogger('tokens')
logger.setLevel(logging.INFO)


def get_one_time_token(event, context):
    secret = os.environ['ONETIME_SECRET']
    exp = datetime.datetime.today() + datetime.timedelta(hours=1)
    encoded = jwt.encode({'exp': exp}, secret, algorithm='HS256')
    encoded = encoded.decode(encoding='utf-8')
    return {'statusCode': 200, 'body': json.dumps({'session_token': encoded})}


def auth(event, context):
    token = event['authorizationToken'].split()[1]
    secret = os.environ['ONETIME_SECRET']
    try:
        jwt.decode(token, secret, algorithms=['HS256'])
        return __generate_policy('user', 'Allow', event['methodArn'])
    except:
        logger.warning(f'Token is invalid: ({token})', exc_info=1)
        return __generate_policy('user', 'Deny', event['methodArn'])


def __generate_policy(principalId, effect, resource):
    policy = {}
    policy['principalId'] = principalId
    policy['policyDocument'] = {'Version': '2012-10-17',
                                'Statement': [{'Action': 'execute-api:Invoke',
                                               'Effect': effect,
                                               'Resource': resource}]}
    return policy
