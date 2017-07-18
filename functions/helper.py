import json
import logging
from os import environ
import boto3
from constants.odessa_response_codes import *
from constants.boc_response_codes import *
from constants.device_response_codes import *
from boc.subscription import Subscription

RUN_SUBSCRIBE_ASYNC = 'run_subscribe'
RUN_UNSUBSCRIBE_ASYNC = 'run_unsubscribe'

DEFAULT_TIME_PERIOD_MINS = 60
MINIMUM_TIME_PERIOD_MINS = 30
MAXIMUM_TIME_PERIOD_MINS = 300

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def create_response(device_id, boc_response):
    body = {
        'code': boc_response['code'],
        'device_id': device_id
    }

    if 'get' in boc_response:
        body['data'] = boc_response['get']
    elif 'set' in boc_response:
        body['data'] = boc_response['set']

    body['message'] = boc_response['message']
    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }
    return response


def create_odessa_response(error_code, device_list=None):
    if not device_list:
        return {
            'statusCode': 200,
            'body': json.dumps({
                'code': error_code,
                'message': odessa_response_message(error_code)
            })
        }
    else:
        return {
            'statusCode': 200,
            'body': json.dumps({
                'code': error_code,
                'message': odessa_response_message(error_code),
                'devices': device_list
            })
        }


def odessa_response_message(error_code, reason=None):
    error_map = {
        SUCCESS: 'Success',
        PARTIAL_SUCCESS: 'Partial Success',
        BAD_REQUEST: 'Bad Request',
        CONFLICT: 'Requests conflict',
        ERROR: 'Error',
        DB_CONNECTION_ERROR: 'Failed to connect with DB',
        BOC_API_CALL_ERROR: 'Failed to call BOC API',
        PARAMS_MISSING_ERROR: reason
    }
    return error_map[error_code]


def error_response(device_id, code, message):
    body = {
        'code': code,
        'device_id': device_id,
        'message': message
    }
    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }
    return response


def subscription_api_client(boc_service_id):
    return Subscription(boc_service_id,
                        environ['BOC_BASE_URL'])


def process_get_subscription_response(response, device_info):
    if int(response['success']):
        for notification in response['notifications']:
            if(int(notification['error_code']) == NOTIFICATION_NO_CACHE):
                return SUBSCRIBED_OFFLINE
        return SUBSCRIBED
    elif(int(response['code']) == DEVICE_NOT_RECOGNIZED or
         int(response['code']) == OBJECT_SUBSCRIPTION_NOT_FOUND):
        return NOT_SUBSCRIBED
    else:
        return {'code': int(response['code']), 'message': response['message']}


def invoke_run_subscribe(device_id, log_service_id, time_period):
    payload = {
        'device_id': device_id,
        'log_service_id': log_service_id,
        'time_period': time_period}
    invoke_async(RUN_SUBSCRIBE_ASYNC, json.dumps(payload))


def invoke_run_unsubscribe(device_id, log_service_id):
    payload = {
        'device_id': device_id,
        'log_service_id': log_service_id}
    invoke_async(RUN_UNSUBSCRIBE_ASYNC, json.dumps(payload))


def invoke_async(function_name, payload):
    logging.info(
        f'invoking lambda function:{function_name} with payload: {payload}')
    if 'IS_LOCAL' in environ and environ['IS_LOCAL'] == 'true':
        return
    else:  # pragma: no cover
        lambda_client = boto3.client('lambda')
        return lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='Event',
            Payload=payload
        )


def verify_time_period(time_period):
    if time_period < MINIMUM_TIME_PERIOD_MINS:
        return MINIMUM_TIME_PERIOD_MINS
    elif time_period > MAXIMUM_TIME_PERIOD_MINS:
        return MAXIMUM_TIME_PERIOD_MINS
    return time_period


def has_acceptable_sub_errors_only(response):
    for item in response['subscribe']:
        item['error_code'] = int(item['error_code'])
        if (item['error_code'] != NO_ERROR and
           item['error_code'] != ALREADY_SUBSCRIBED):
            return False
    return True


def has_acceptable_unsub_errors_only(response):
    for item in response['unsubscribe']:
        item['error_code'] = int(item['error_code'])
        if (item['error_code'] != NO_ERROR and
           item['error_code'] != NOT_SUBSCRIBED_FROM_SERVICE and
           item['error_code'] != SUCCESS_BUT_DEVICE_OFFLINE and
           item['error_code'] != NO_SUCH_OID):
            return False
    return True
