import sys
import json
import logging
from botocore.exceptions import ClientError
from botocore.exceptions import ConnectionError
from collections import OrderedDict
from models.device_status import DeviceStatus
from models.device_log import DeviceLog
from models.reporting_registration import ReportingRegistration
from functions import helper
from helpers import time_functions
from datetime import datetime, timedelta
from constants.odessa_response_codes import *
from constants.feature_response_codes import *
from pymib.mib import MIB
from pymib.oid import OID

logger = logging.getLogger('device_statuses')
logger.setLevel(logging.INFO)


def get_device_statuses(event, context):
    logger.info(event)
    data = json.loads(event['body'])

    if ('reporting_id' not in data
            or not (isinstance(data['reporting_id'], list)
                    or isinstance(data['reporting_id'], str))
            or not len(data['reporting_id']) > 0):
        logger.warning('BadRequest on get_device_statuses')
        return device_statuses_response(BAD_REQUEST, [], "Parameter 'reporting_id' has incorrect value")
    if isinstance(data['reporting_id'], str):
        data['reporting_id'] = [data['reporting_id']]
    data['reporting_id'] = list(OrderedDict.fromkeys(data['reporting_id']))

    if ('features' not in data
            or not (isinstance(data['features'], list)
                    or isinstance(data['features'], str))
            or not len(data['features']) > 0):
        logger.warning('BadRequest on get_device_statuses')
        return device_statuses_response(BAD_REQUEST, [], "Parameter 'features' has incorrect value")
    if isinstance(data['features'], str):
        data['features'] = [data['features']]

    if 'log_service_id' in data:
        log_service_id = str(data['log_service_id'])
    else:
        log_service_id = '0'

    if 'from' in data:
        try:
            status_from = time_functions.parse_time_with_tz(data['from'])
        except:
            logger.warning('BadRequest on get_device_statuses')
            return device_statuses_response(BAD_REQUEST, [], "Parameter 'from' has incorrect value")
    else:
        status_from = datetime.now() - timedelta(hours=1)

    data['features'] = list(OrderedDict.fromkeys(data['features']))
    oids, missings = MIB.search_oid(data['features'])
    if missings and not oids:
        return device_statuses_response(BAD_REQUEST, [], '"features" values are invalid')

    response_data = []
    for reporting_id in data['reporting_id']:
        try:
            reporting_registration = ReportingRegistration().read(reporting_id, log_service_id)
            if not reporting_registration:
                response_data.append(create_device_result(reporting_id, [], DEVICE_NOT_FOUND))
                continue

            feature_results = []
            processed_count_type_features = []
            for missing_feature in missings:
                feature_results.append(create_feature_result(missing_feature, None, FEATURE_NOT_FOUND))
            for object_id, feature_names in oids.items():
                oid = OID(object_id)
                if oid.type in ['count_type', 'counter']:
                    feature_names = list(set(feature_names) - set(processed_count_type_features))
                    if not feature_names:
                        continue

                device_status = DeviceStatus()
                device_status.read(reporting_id, object_id)
                if(device_status.is_existing() and
                   timestamp_newer_than(device_status.timestamp, status_from)):
                    for feature_name in feature_names:
                        device_id = reporting_registration[0]['device_id']
                        oid_data = device_status.data
                        if(oid.type == 'counter' and oid_feature_is_matching(device_id, device_status, feature_name, status_from)):
                            processed_count_type_features.append(feature_name)
                            feature_results.append(create_feature_result(feature_name, oid_data['counter_value'], SUCCESS))
                        elif(feature_name in oid_data and
                             timestamp_newer_than(oid_data[feature_name]['timestamp'], status_from)):
                            feature_results.append(create_feature_result(feature_name, oid_data[feature_name], SUCCESS))
            if feature_results:
                response_data.append(create_device_result(reporting_id, feature_results))
            else:
                response_data.append(create_device_result(reporting_id, [], LOGS_NOT_FOUND))
        except (ClientError, ConnectionError) as e:  # pragma: no cover
            logger.error(e)
            response_data.append(create_device_result(reporting_id, [], DB_CONNECTION_ERROR))
        except:  # pragma: no cover
            logger.error(sys.exc_info())
            response_data.append(create_device_result(reporting_id, [], INTERNAL_SERVER_ERROR))

    if all(device['error_code'] == SUCCESS for device in response_data):
        return device_statuses_response(SUCCESS, response_data)
    elif all(device['error_code'] == LOGS_NOT_FOUND for device in response_data):
        return device_statuses_response(LOGS_NOT_FOUND, response_data)
    elif any((device['error_code'] == SUCCESS or
              device['error_code'] == PARTIAL_SUCCESS or
              device['error_code'] == LOGS_NOT_FOUND) for device in response_data):
        return device_statuses_response(PARTIAL_SUCCESS, response_data)
    elif any(device['error_code'] == DEVICE_NOT_FOUND for device in response_data):
        return device_statuses_response(DEVICE_NOT_FOUND, response_data)
    elif any(device['error_code'] == DB_CONNECTION_ERROR for device in response_data):
        return device_statuses_response(DB_CONNECTION_ERROR, response_data)
    else:
        return device_statuses_response(ERROR, response_data)


def timestamp_newer_than(timestamp, status_from):
    return time_functions.parse_time(timestamp) >= status_from


def create_feature_result(feature_name, feature_data, error_code):
    feature_result = {
        'error_code': error_code,
        'feature': feature_name,
        'message': helper.feature_response_message(error_code)
    }
    if feature_data:
        feature_result['value'] = feature_data['value']
        feature_result['updated'] = feature_data['timestamp']
    return feature_result


def create_device_result(reporting_id, device_data, error_code=None):
    device_result = {'reporting_id': reporting_id, 'data': device_data}
    if error_code == LOGS_NOT_FOUND or error_code == DEVICE_NOT_FOUND:
        device_result['error_code'] = error_code
    elif all(feature['error_code'] == FEATURE_NOT_FOUND for feature in device_data):
        # In this case one or more features exist but their logs are not found
        device_result['error_code'] = PARTIAL_SUCCESS
    elif all(feature['error_code'] == SUCCESS for feature in device_data):
        device_result['error_code'] = SUCCESS
    elif any(feature['error_code'] == SUCCESS for feature in device_data):
        device_result['error_code'] = PARTIAL_SUCCESS
    else:
        device_result['error_code'] = ERROR

    device_result['message'] = helper.odessa_response_message(device_result['error_code'])
    return device_result


def device_statuses_response(error_code, data, message=None):
    return helper.create_odessa_response(error_code, {'devices': data}, message)


def oid_feature_is_matching(device_id, device_status, feature_name, status_from):
    pair_oid = OID(device_status.object_id).pair_oid
    pair_oid_status = DeviceStatus()
    pair_oid_status.read(device_status.reporting_id, pair_oid)
    if (pair_oid_status.is_existing() and
       OID(pair_oid).parse(pair_oid_status.data['count_type_id']['value']) == feature_name and
       timestamp_newer_than(device_status.data['counter_value']['timestamp'], status_from)):
        return True
    else:
        log_record = DeviceLog().get_log({'oid': pair_oid}, device_id)
        if (log_record and log_record['Items'] and
           OID(pair_oid).parse(log_record['Items'][0]['value']) == feature_name and
           timestamp_newer_than(device_status.data['counter_value']['timestamp'], status_from)):
            return True
    return False
