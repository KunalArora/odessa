from botocore.exceptions import ConnectionError
from botocore.exceptions import ClientError
from collections import OrderedDict
import concurrent.futures
from constants import feature_response_codes
from constants import odessa_response_codes
from constants.device_response_codes import *
from constants import oids
from functions import helper
import json
import logging
from models.device_log import DeviceLog
from models.device_subscription import DeviceSubscription
from models.service_oid import ServiceOid
from pymib.mib import MIB
from pymib.parse import parse
import re
import sys


PARAMS_LIST = ['device_id', 'features', 'from', 'to', 'time_unit']

logger = logging.getLogger('get_history_logs')
logger.setLevel(logging.INFO)

# Retrieve history log data for a device in a specific time interval
# on an Hourly, Daily, Monthly basis


def get_history_logs(event, context):
    logger.info(f'handler:get_history_logs, request: {event}')
    device_log = DeviceLog()
    service_oid = ServiceOid()
    device_subscription = DeviceSubscription()

    missing_params_list = []

    try:
        request_body = json.loads(event['body'])
    except (TypeError, ValueError) as e:
        logger.warning(
            f'BadRequest on handler:get_history_logs, error occurred: {e} '
            'Reason: Parameter Request Body has incorrect format on event :'
            f'{event}')
        return helper.history_logs_response(
            odessa_response_codes.BAD_REQUEST, message="Request Body has "
            "incorrect format"
        )

    for param in PARAMS_LIST:
        if param not in request_body:
            missing_params_list.append(param)

    if missing_params_list:
        logger.warning(
            f'BadRequest on handler:get_history_logs, '
            f'Reason: Parameters missing from Request Body: '
            f'{missing_params_list}')
        return helper.history_logs_response(
            odessa_response_codes.BAD_REQUEST, message=f'Parameters Missing: '
            f'{missing_params_list}')

    device_id = request_body['device_id']

    # If device_id doesn't match the specified regular expression format,
    # response is sent back as error
    if (not isinstance(device_id, str)
            or re.match(helper.DEVICE_ID_REGEX, device_id) is None):
        logger.warning(
            f"BadRequest on handler:get_history_logs, "
            f"Reason: Parameter 'device_id' = {device_id} has incorrect format")
        return helper.history_logs_response(
            odessa_response_codes.BAD_REQUEST, device_id, message=f"Parameter "
            f"'device_id' = '{device_id}' has incorrect format")

    from_time = request_body['from']
    to_time = request_body['to']
    time_unit = request_body['time_unit']

    try:
        # Test for incorrect format of parameters 'for' and 'to' or if value
        # of 'from' is greater than value of 'to'
        parsed_from_time = None
        parsed_from_time = device_log.parse_time(from_time)
        parsed_to_time = device_log.parse_time(to_time)
        if parsed_from_time >= parsed_to_time:
            logger.warning(
                f"BadRequest on handler:get_history_logs, "
                f"Reason: Parameter 'from' = {from_time} should be less than "
                f"parameter 'to' = {to_time}")
            return helper.history_logs_response(
                odessa_response_codes.BAD_REQUEST, device_id, message="Parameter "
                "'from' should be less than parameter 'to'")
    except (TypeError, ValueError) as e:
        if parsed_from_time is None:
            logger.warning(
                f"BadRequest on handler:get_history_logs, error occurred = {e}"
                "Reason: Parameter 'from' has incorrect format")
            return helper.history_logs_response(
                odessa_response_codes.BAD_REQUEST, device_id, message=f"Parameter "
                f"'from' has incorrect value: {from_time}")
        else:
            logger.warning(
                f"BadRequest on handler:get_history_logs, error occured = {e}"
                "Reason: Parameter 'to' has incorrect format")
            return helper.history_logs_response(
                odessa_response_codes.BAD_REQUEST, device_id, message=f"Parameter "
                f"'to' has incorrect value: {to_time}")

    # Test for incorrect value of parameter 'time_unit'
    if time_unit not in helper.TIME_UNIT_VALUES:
        logger.warning(
            f"BadRequest on handler:get_history_logs, "
            f"Reason: Parameter 'time_unit' has incorrect value: {time_unit}")
        return helper.history_logs_response(
            odessa_response_codes.BAD_REQUEST, device_id, message=f"Parameter "
            f"'time_unit' has incorrect value: {time_unit}")

    original_feature_list = request_body['features']

    # Test if parameter 'features' is not a list or a string
    if not isinstance(original_feature_list, list):
        if isinstance(original_feature_list, str):
            original_feature_list = original_feature_list.split()
        else:
            logger.warning(
                f"BadRequest on handler:get_history_logs, "
                f"Reason: Parameter 'features' = {request_body['features']} "
                f"is not a list or string")
            return helper.history_logs_response(
                odessa_response_codes.BAD_REQUEST, device_id, message="Parameter "
                "'features' should be a list or string")

    # Remove redundancy from features list
    original_feature_list = list(OrderedDict.fromkeys(original_feature_list))

    try:
        parsed_data = []
        unsubscribed_features = []

        if 'log_service_id' in request_body:
            log_service_id = str(request_body['log_service_id'])
        else:
            log_service_id = '0'

        # Test if log_service_id doesn't exist in database
        oid = service_oid.read(log_service_id)
        if not oid:
            raise helper.ServiceIdError(event)

        # Find out the corresponding object ids from features
        # Features which do not exist (if any) are also returned
        object_id_list, unidentified_features = MIB.search_oid(
            original_feature_list)

        device_subscription.read(device_id, log_service_id)

        # Filter the object ids which are (or were) subscribed
        if device_subscription.is_existing():
            # Check if device subscription record has the field 'oids' which
            # means it is subscribed or was subscribed in the past
            if device_subscription.has_oids_field():
                subscribed_oids = device_subscription.get_subscribed_oids()
                # Check for unsubscribed object ids
                for key, val in object_id_list.items():
                    if key not in subscribed_oids:
                        unsubscribed_features.extend(val)
                object_id_list = {
                    key: val for key, val in object_id_list.items() if key in subscribed_oids}
        else:
            return helper.history_logs_response(
                odessa_response_codes.DEVICE_NOT_FOUND, device_id)

        # Erasing duplicates in case of features' type being count_type etc.
        unsubscribed_features = list(
            OrderedDict.fromkeys(unsubscribed_features))

        # Get the charset record of the device from the Database
        charset = device_log.get_charset(device_id)

        # Break the time period into smaller periods
        time_periods = device_log.break_time_period(
            from_time, to_time, time_unit)

        for period in time_periods:
            db_res = {}
            db_query_params = {
                'device_id': device_id,
                'from_time': period['start_time'],
                'to_time': period['end_time'],
                'time_unit': time_unit
            }
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = {
                    executor.submit(
                        get_oid_value, device_log, key, db_query_params): key for key in object_id_list.keys()}
                for future in concurrent.futures.as_completed(futures):
                    if future.result():
                        record = future.result()
                        object_id = record['id'].split('#')[1]
                        item = {object_id: record}
                        db_res.update(item)
            # Parse the retrieved data
            if db_res:
                if charset:
                    db_res.update({oids.CHARSET_OID: charset})
                parsed_data.extend(
                    parse_oid_value(
                        object_id_list, original_feature_list, db_res))

        # Create the data part (feature wise data) of the response body
        response_data = create_feature_data(
            original_feature_list, parsed_data, unidentified_features, unsubscribed_features)

        # Create the history logs API response body
        odessa_response = create_response_body(device_id, response_data)

        logger.info(
            f'handler:get_history_logs, response: {json.dumps(odessa_response)}')

        return odessa_response

    except helper.ServiceIdError as e:
        logger.warning(
            f"BadRequest on handler:get_history_logs, Reason: Parameter "
            f"'log_service_id' has invalid value: {log_service_id} "
            f"on event {event}")
        return helper.history_logs_response(
            odessa_response_codes.BAD_REQUEST, device_id,
            message=f"Parameter 'log_service_id' has invalid value: {log_service_id}")
    except (ConnectionError, ClientError) as e:
        logger.error(e)
        logger.warning(
            f'Database Error on handler:get_history_logs '
            f'on event {event}')
        return helper.history_logs_response(
            odessa_response_codes.DB_CONNECTION_ERROR, device_id)
    except: # pragma: no cover
        # Unkown error
        logger.error(sys.exc_info())
        logger.warning(
            f'Unknown Error occurred on handler:get_history_logs on event {event}')
        return helper.history_logs_response(
            odessa_response_codes.INTERNAL_SERVER_ERROR, device_id)


def get_oid_value(device_log, object_id, db_query_params):
    db_query_params['object_id'] = object_id
    response_from_db = device_log.get_history_logs(db_query_params)
    if response_from_db:
        return response_from_db


# Parse oid values and send back required features' values
def parse_oid_value(object_id_list, original_feature_list, log_data):
    response = []
    parse_res = parse(log_data)

    for key, val in parse_res.items():
        if 'error' in val:
            logger.warning(
                "Exception generated from parser in handler:get_history_logs for "
                f"id {val['id']} having value {val['value']} and error {val['error']}"
            )
            object_id = val['id'].split('#')[1]
            val['features'] = {}
            for feature in object_id_list[object_id]:
                val['features'].update({feature: None})
            response.append(val)
        else:
            # Rename key for naming convenience
            val['features'] = val.pop('value')
            response.append(val)

    # Filter the features which are in the original list
    for item in response:
        item['features'] = {
            key: val for key, val in item['features'].items() if key in original_feature_list}

    return response


def create_feature_data(
        feature_list, raw_data, unidentified_features, unsubscribed_features):
    data = []
    for feature in feature_list:
        value = []
        updated = []

        for data_item in raw_data:
            if feature in data_item['features']:
                value_item = data_item['features'][feature]
                # Check if value needs to be adjusted
                value.append(
                    value_item if feature not in helper.FEATURE_ADJUSTING_LIST else helper.adjust_feature_value(
                        value_item))

                updated.append(helper.convert_iso(data_item['timestamp']))

        # Invalid Feature name
        if feature in unidentified_features:
            feature_response = create_feature_format(
                feature, feature_response_codes.FEATURE_NOT_FOUND)
        # Unsubscribed Feature
        elif feature in unsubscribed_features:
            feature_response = create_feature_format(
                feature, feature_response_codes.FEATURE_NOT_SUBSCRIBED)
        # No values could be retrieved
        elif not value:
            feature_response = create_feature_format(
                feature, feature_response_codes.LOGS_NOT_FOUND)
        # All the values failed to be parsed
        elif all(v is None for v in value):
            value = ['' if v is None else v for v in value]
            feature_response = create_feature_format(
                feature, feature_response_codes.INTERNAL_SERVER_ERROR, value, updated)
        # Some of the values failed to be parsed
        elif any(v is None for v in value):
            value = ['' if v is None else v for v in value]
            feature_response = create_feature_format(
                feature, feature_response_codes.PARTIAL_SUCCESS, value, updated)
        # Values retrieved successfully
        else:
            feature_response = create_feature_format(
                feature, feature_response_codes.SUCCESS, value, updated)

        data.append(feature_response)

    return data


def create_feature_format(feature, error_code, value=None, updated=None):
    response = {
        "feature": feature,
        "error_code": error_code,
        "message": helper.feature_response_message(error_code)
    }
    if value and updated:
        response.update({"value": value, "updated": updated})

    return response


def create_response_body(device_id, data):
    # Data for none of the features could be parsed
    if all(feature['error_code'] == feature_response_codes.INTERNAL_SERVER_ERROR for feature in data):
        return helper.history_logs_response(
            odessa_response_codes.ERROR, device_id, data)
    # None of the features were found to be valid
    elif all(feature['error_code'] == feature_response_codes.FEATURE_NOT_FOUND for feature in data):
        return helper.history_logs_response(
            odessa_response_codes.BAD_REQUEST, device_id, data, message="Features Not Found")
    # None of the features are/were subscribed
    elif all(feature['error_code'] == feature_response_codes.FEATURE_NOT_SUBSCRIBED for feature in data):
        return helper.history_logs_response(
            odessa_response_codes.FEATURES_NOT_SUBSCRIBED, device_id, data)
    # Log data for none of the features could be found for the specified time period
    elif all(feature['error_code'] == feature_response_codes.LOGS_NOT_FOUND for feature in data):
        return helper.history_logs_response(
            odessa_response_codes.LOGS_NOT_FOUND, device_id, data)
    # Data retrieved successfully for all features
    elif all(feature['error_code'] == feature_response_codes.SUCCESS for feature in data):
        return helper.history_logs_response(
            odessa_response_codes.SUCCESS, device_id, data)
    # Rest of the cases fall into the Partial Success category
    else:
        return helper.history_logs_response(
            odessa_response_codes.PARTIAL_SUCCESS, device_id, data)
