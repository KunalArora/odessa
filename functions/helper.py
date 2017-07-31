import json
import logging
from os import environ
import boto3
from datetime import datetime
from datetime import timezone
from constants.odessa_response_codes import *
from constants.boc_response_codes import *
from constants.device_response_codes import *
from constants.oids import *
from boc.subscription import Subscription
from pymib.oid import OID

RUN_SUBSCRIBE_ASYNC = 'run_subscribe'
RUN_UNSUBSCRIBE_ASYNC = 'run_unsubscribe'

DEFAULT_TIME_PERIOD_MINS = 60
MINIMUM_TIME_PERIOD_MINS = 1
MAXIMUM_TIME_PERIOD_MINS = 300

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class ServiceIdError(Exception):
    def __init__(self, errArgu):
        Exception.__init__(self)
        self.errArgu = errArgu


class DeviceIdParameterError(Exception):
    def __init__(self, errArgu):
        Exception.__init__(self)
        self.errArgu = errArgu


class NotificationError(Exception):
    def __init__(self, errArgu):
        Exception.__init__(self)
        self.errArgu = errArgu


class EventParameterError(Exception):
    def __init__(self, errArgu):
        Exception.__init__(self)
        self.errArgu = errArgu

def latest_logs_response(error_code, device_list=[]):
    return create_odessa_response(error_code, {'devices': device_list})

def subscriptions_response(error_code, device_list=[]):
    return create_odessa_response(error_code, {'devices': device_list})


def device_settings_response(error_code, device_id='', message=None, data=[]):
    if data:
        for item in data:
            item['error_code'] = int(item['error_code'])

    return create_odessa_response(
        error_code, {'device_id': device_id, 'data': data}, message)


def create_odessa_response(error_code, result, message=None):
    body = {
        'code': error_code,
        'message': message if message else odessa_response_message(error_code)
    }

    body.update(result)

    return {
        'statusCode': 200,
        'body': json.dumps(body)
    }


def odessa_response_message(error_code):
    error_map = {
        SUCCESS: 'Success',
        PARTIAL_SUCCESS: 'Partial Success',
        BAD_REQUEST: 'Bad Request',
        DEVICE_NOT_FOUND: 'Device Not Found',
        CONFLICT: 'Requests conflict',
        INTERNAL_SERVER_ERROR: 'Internal Server Error',
        ERROR: 'Error',
        DB_CONNECTION_ERROR: 'Failed to connect with DB',
        BOC_DB_CONNECTION_ERROR: 'BOC DB Connection Error',
        BOC_API_CALL_ERROR: 'Failed to call BOC API'
    }
    return error_map[error_code]


def subscription_api_client(boc_service_id):
    return Subscription(boc_service_id,
                        environ['BOC_BASE_URL'])


def process_get_subscription_response(response, device_info):
    if int(response['success']):
        for notification in response['notifications']:
            if(int(notification['error_code']) == NO_ERROR):
                return SUBSCRIBED
        return SUBSCRIBED_OFFLINE
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
        return MINIMUM_TIME_PERIOD_MINS * 60
    elif time_period > MAXIMUM_TIME_PERIOD_MINS:
        return MAXIMUM_TIME_PERIOD_MINS * 60
    return time_period * 60


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


def filter_res(feature, value):
    filter_list = (["TonerInk_LifeBlack",
                    "TonerInk_LifeCyan",
                    "TonerInk_LifeMagenta",
                    "TonerInk_LifeYellow"])
    if feature in filter_list:
        value = str(int(value)//100)
    return value


def create_feature_format(code, feature, value, timestamp, **options):
    feature_format = {}
    feature_format['error_code'] = code
    feature_format['feature'] = feature
    feature_format['status'] = filter_res(feature, value) if value!= " " else ''
    feature_format['timestamp'] = convert_iso(timestamp) if timestamp != '' else ''
    if options.get('message'):
        feature_format['message'] = options.get('message')
    else:
        feature_format['message'] = odessa_response_message(code)
    return(feature_format)



def create_features_layer(parse_data):
#    Create features layer data to create response.
    data = []
    for d in parse_data:
        feature = {}
        feature['error_code'] = d['error_code']
        feature['feature'] = d['feature']
        if d['feature'] == 'Online_Offline':
            feature['value'] = ['0', '1'][d['status'] == 'online']
        else:
            feature['value'] = d['status']
        feature['updated'] = d['timestamp']
        feature['message'] = d['message']
        data.append(feature)
    return(data)


def create_devices_layer(data, device_id, **options):
#    Create devices layer data to create response.
    device = {}
    if options:
        device['error_code'] = options.get('code')
    else:
        device['error_code'] = devices_error_code(data)
    device['device_id'] = device_id
    device['data'] = data if data else None
    device['message'] = odessa_response_message(device['error_code'])
    return device


def devices_error_code(data):
    hasSuccess = False
    hasInternal = False
    for d in data:
        if d['error_code'] == SUCCESS:
            hasSuccess = True
        else:
            hasInternal = True
    if hasSuccess and hasInternal:
        return PARTIAL_SUCCESS
    elif hasSuccess and not hasInternal:
        return SUCCESS
    else:
        return INTERNAL_SERVER_ERROR


def odessa_error_code(data):
    hasSuccess = False
    hasInternal = False
    hasFound = False
    other = False
    for d in data:
        if d['error_code'] == SUCCESS:
            hasSuccess = True
        elif d['error_code'] == DEVICE_NOT_FOUND:
            hasFound = True
        elif d['error_code'] == INTERNAL_SERVER_ERROR:
            hasInternal = True
        else:
            other = True
    if hasSuccess and not hasFound and not hasInternal and not other:
        return SUCCESS
    elif hasFound and not hasSuccess and not hasInternal and not other:
        return DEVICE_NOT_FOUND
    elif hasInternal and not hasSuccess and not hasFound and not other:
        return ERROR
    else:
        return PARTIAL_SUCCESS

def convert_iso(time):
    time_dt = datetime.strptime(time, '%Y-%m-%dT%H:%M:%S')
    return time_dt.replace(tzinfo=timezone.utc).isoformat()
