from botocore.exceptions import ClientError
from botocore.exceptions import ConnectionError
from collections import OrderedDict
from constants import feature_response_codes
from constants import odessa_response_codes
from constants.device_response_codes import *
from functions import helper
from helpers import time_functions
import json
import logging
from models.device_email_log import DeviceEmailLog
from models.device_log import DeviceLog
from models.device_subscription import DeviceSubscription
from models.reporting_registration import ReportingRegistration
from models.service_oid import ServiceOid
from os import environ
from pymib.mib import MIB
import re
import sys


QUERY_PARAMS_LIST = ['features', 'from', 'to', 'time_unit']

logger = logging.getLogger('get_history_logs')
logger.setLevel(logging.INFO)

# Retrieve history log data for a BOC/Email device in a specific time interval
# on an Hourly, Daily, Monthly basis


def get_history_logs(event, context):
    logger.info(f'handler:get_history_logs, request: {event}')
    device_log = DeviceLog()
    service_oid = ServiceOid()
    device_subscription = DeviceSubscription()
    device_email_log = DeviceEmailLog()
    reporting_registration = ReportingRegistration()

    missing_params_list = []

    device_id = None
    reporting_id = None

    try:
        request_body = json.loads(event['body'])
    except (TypeError, ValueError) as e:
        logger.warning(
            f'BadRequest on handler:get_history_logs, error occurred: {e} '
            'Reason: Parameter Request Body has incorrect format on event :'
            f'{event}')
        return history_logs_response(
            odessa_response_codes.BAD_REQUEST, message="Request Body has "
            "incorrect format"
        )

    if 'reporting_id' in request_body:
        reporting_id = request_body['reporting_id']
    elif 'device_id' in request_body:
        device_id = request_body['device_id']
    else:
        logger.warning(
            f'BadRequest on handler:get_history_logs, '
            f'Reason: Parameters missing from Request Body: '
            f'reporting_id/device_id')
        return history_logs_response(
            odessa_response_codes.BAD_REQUEST, message=f'Parameters Missing: '
            f'reporting_id/device_id')

    for param in QUERY_PARAMS_LIST:
        if param not in request_body:
            missing_params_list.append(param)

    if missing_params_list:
        logger.warning(
            f'BadRequest on handler:get_history_logs, '
            f'Reason: Parameters missing from Request Body: '
            f'{missing_params_list}')
        return history_logs_response(
            odessa_response_codes.BAD_REQUEST, reporting_id, device_id, message=f'Parameters Missing: '
            f"{', '.join(missing_params_list)}")

    if device_id is not None:
        if (not isinstance(device_id, str)
                or re.match(helper.GUID_REGEX, device_id) is None):
            logger.warning(
                f"BadRequest on handler:get_history_logs, "
                f"Reason: Parameter 'device_id' has incorrect value: {device_id}")
            return history_logs_response(
                odessa_response_codes.BAD_REQUEST, device_id=device_id, message=f"Parameter "
                f"'device_id' has incorrect value: '{device_id}'")

    if reporting_id is not None:
        if (not isinstance(reporting_id, str) or reporting_id == ""):
            logger.warning(
                f"BadRequest on handler:get_history_logs, "
                f"Reason: Parameter 'reporting_id' has incorrect value")
            return history_logs_response(
                odessa_response_codes.BAD_REQUEST, reporting_id, message=f"Parameter "
                f"'reporting_id' has incorrect value: '{reporting_id}'")

    from_time = request_body['from']
    to_time = request_body['to']
    time_unit = request_body['time_unit']

    try:
        # Test for incorrect format of parameters 'from' and 'to' or if value
        # of 'from' is greater than value of 'to'
        parsed_from_time = None
        parsed_from_time = time_functions.parse_time_with_tz(from_time)
        parsed_to_time = time_functions.parse_time_with_tz(to_time)
        if parsed_from_time >= parsed_to_time:
            logger.warning(
                f"BadRequest on handler:get_history_logs, "
                f"Reason: Parameter 'from' = {from_time} should be less than "
                f"parameter 'to' = {to_time}")
            return history_logs_response(
                odessa_response_codes.BAD_REQUEST, reporting_id, device_id, message="Parameter "
                f"'from' = {from_time} should be less than parameter 'to' = {to_time}")
    except (TypeError, ValueError) as e:
        if parsed_from_time is None:
            logger.warning(
                f"BadRequest on handler:get_history_logs, error occurred = {e}"
                "Reason: Parameter 'from' has incorrect format")
            return history_logs_response(
                odessa_response_codes.BAD_REQUEST, reporting_id, device_id, message=f"Parameter "
                f"'from' has incorrect value: {from_time}")
        else:
            logger.warning(
                f"BadRequest on handler:get_history_logs, error occured = {e}"
                "Reason: Parameter 'to' has incorrect format")
            return history_logs_response(
                odessa_response_codes.BAD_REQUEST, reporting_id, device_id, message=f"Parameter "
                f"'to' has incorrect value: {to_time}")

    # Test for incorrect value of parameter 'time_unit'
    if (not isinstance(time_unit, str)
        or time_unit.lower() not in time_functions.TIME_UNIT_VALUES):
        logger.warning(
            f"BadRequest on handler:get_history_logs, "
            f"Reason: Parameter 'time_unit' has incorrect value: {time_unit}")
        return history_logs_response(
            odessa_response_codes.BAD_REQUEST, reporting_id, device_id, message=f"Parameter "
            f"'time_unit' has incorrect value: {request_body['time_unit']}")

    # Convert time_unit to lower case
    time_unit = time_unit.lower()

    original_feature_list = request_body['features']

    # Test if parameter 'features' is not a list or a string
    if (not original_feature_list
        or not isinstance(original_feature_list, list)):
        if (isinstance(original_feature_list, str)
            and original_feature_list):
            original_feature_list = [
                f.strip() for f in original_feature_list.split(',')]
        else:
            logger.warning(
                f"BadRequest on handler:get_history_logs, "
                f"Reason: Parameter 'features' = {request_body['features']} "
                f"is not a list or string")
            return history_logs_response(
                odessa_response_codes.BAD_REQUEST, reporting_id, device_id, message="Parameter "
                f"'features' has incorrect value: {request_body['features']}")

    # Remove redundancy from features list
    original_feature_list = list(OrderedDict.fromkeys(original_feature_list))

    try:
        feature_response = []
        unsubscribed_features_list = []

        if 'log_service_id' in request_body:
            log_service_id = str(request_body['log_service_id'])
            if log_service_id == "": # empty
                logger.warning(
                    f"BadRequest on handler:get_history_logs, "
                    "Reason: Parameter 'log_service_id' has empty value: "
                    f"on event {event}")
                return history_logs_response(
                    odessa_response_codes.BAD_REQUEST, reporting_id, device_id,
                    message=f"Parameter 'log_service_id' has incorrect "
                    f"value: {log_service_id}")
        else:
            log_service_id = '0'

        # Test if log_service_id doesn't exist in database
        oid = service_oid.read(log_service_id)
        if not oid:
            logger.warning(
                f"BadRequest on handler:get_history_logs, "
                "Reason: Parameter 'log_service_id' has incorrect value: "
                f"on event {event}")
            return history_logs_response(
                odessa_response_codes.BAD_REQUEST, reporting_id, device_id,
                message=f"Parameter 'log_service_id' has incorrect "
                f"value: {log_service_id}")

        # Find out the corresponding object ids from features
        # Features which do not exist (if any) are also returned
        object_id_list, unidentified_features = MIB.search_oid(
            original_feature_list)

        # Remove Timezone +00:00 value
        from_time = time_functions.remove_tz(from_time)
        to_time = time_functions.remove_tz(to_time)

        # Cases divided into 2 parts:
        # 1. Request is using device_id (Only BOC devices)
        # 2. Request is using reporting_id (Both BOC and Email devices possible)
        if device_id:
            unsubscribed_features = []
            device_subscription.read_for_history_logs(
                device_id, log_service_id)

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
            else:  # Device not found
                return history_logs_response(
                    odessa_response_codes.DEVICE_NOT_FOUND, device_id=device_id)

            # Erasing duplicates in case of features' type being count_type etc.
            unsubscribed_features_list.extend(
                list(
                    OrderedDict.fromkeys(unsubscribed_features)))

            params = {
                'device_id': device_id,
                'from_time_unit': from_time,
                'to_time_unit': to_time,
                'time_unit': time_unit
            }
            result = device_log.get_log_history(
                params, object_id_list, original_feature_list)
            if result:
                feature_response.extend(result)

        elif reporting_id:
            reporting_records = reporting_registration.get_reporting_records(
                reporting_id, from_time, to_time)
            if not reporting_records:
                # Reporting Id not found
                return history_logs_response(
                    odessa_response_codes.DEVICE_NOT_FOUND, reporting_id,
                        message="Reporting ID Not Found")

            for record in reporting_records:
                if (
                    'from_time_unit' in record and 'to_time_unit' in record
                        and record['communication_type'] == 'cloud'
                            and 'device_id' in record):
                    unsubscribed_features = []
                    device_id = record['device_id']
                    device_subscription.read_for_history_logs(
                        device_id, log_service_id)

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
                        # Case of contradiction: Reporting activated but device
                        # not subscribed/ not been subscribed in the past
                        logger.error("DB Contradiction Error")
                        logger.warning(
                            f"DB Contradiction Error on request = {request_body} "
                            "Reason: Reporting activated but device not "
                            "subscribed (currently or in the past)")
                        return history_logs_response(
                            odessa_response_codes.DB_CONTRADICTION_ERROR,
                            reporting_id)

                    # Erasing duplicates in case of features' type being count_type etc.
                    unsubscribed_features_list.extend(
                        list(
                            OrderedDict.fromkeys(unsubscribed_features)))

                    record['time_unit'] = time_unit
                    result = device_log.get_log_history(
                        record, object_id_list, original_feature_list)
                    if result:
                        feature_response.extend(result)
                elif (
                    'from_time_unit' in record and 'to_time_unit' in record
                        and record['communication_type'] == 'email'
                            and 'serial_number' in record):
                    record['time_unit'] = time_unit
                    result = device_email_log.get_log_history(
                        record, original_feature_list)
                    if result:
                        feature_response.extend(result)

        # Create the data part (feature wise data) of the response body
        response_data = create_feature_data(
            original_feature_list, feature_response, unidentified_features, unsubscribed_features_list)

        # Create the history logs API response body
        odessa_response = create_response_body(
            response_data, reporting_id, device_id)

        logger.info(
            f'handler:get_history_logs, response: {json.dumps(odessa_response)}')

        return odessa_response

    except (ConnectionError, ClientError) as e:
        logger.error(e)
        logger.warning(
            f'Database Error on handler:get_history_logs '
            f'on event {event}')
        return history_logs_response(
            odessa_response_codes.DB_CONNECTION_ERROR, reporting_id, device_id)
    except:  # pragma: no cover
        # Unkown error
        logger.error(sys.exc_info())
        logger.warning(
            f'Unknown Error occurred on handler:get_history_logs on event {event}')
        return history_logs_response(
            odessa_response_codes.INTERNAL_SERVER_ERROR, device_id)


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

                updated.append(time_functions.convert_iso(data_item['timestamp']))

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


def create_response_body(data, reporting_id=None, device_id=None):
    # Data for none of the features could be parsed
    if all(feature['error_code'] == feature_response_codes.INTERNAL_SERVER_ERROR for feature in data):
        return history_logs_response(
            odessa_response_codes.ERROR, reporting_id, device_id, data)
    # None of the features were found to be valid
    elif all(feature['error_code'] == feature_response_codes.FEATURE_NOT_FOUND for feature in data):
        return history_logs_response(
            odessa_response_codes.BAD_REQUEST, reporting_id, device_id, data, message="Features Not Found")
    # None of the features are/were subscribed
    elif all(feature['error_code'] == feature_response_codes.FEATURE_NOT_SUBSCRIBED for feature in data):
        return history_logs_response(
            odessa_response_codes.FEATURES_NOT_SUBSCRIBED, reporting_id, device_id, data)
    # Log data for none of the features could be found for the specified time period
    elif all(feature['error_code'] == feature_response_codes.LOGS_NOT_FOUND for feature in data):
        return history_logs_response(
            odessa_response_codes.LOGS_NOT_FOUND, reporting_id, device_id, data)
    # Data retrieved successfully for all features
    elif all(feature['error_code'] == feature_response_codes.SUCCESS for feature in data):
        return history_logs_response(
            odessa_response_codes.SUCCESS, reporting_id, device_id, data)
    # Rest of the cases fall into the Partial Success category
    else:
        return history_logs_response(
            odessa_response_codes.PARTIAL_SUCCESS, reporting_id, device_id, data)


def history_logs_response(error_code, reporting_id='', device_id='', data=[], message=None):
    if reporting_id:
        return helper.create_odessa_response(
            error_code, {'reporting_id': reporting_id, 'data': data},
            message, cors=True)

    elif device_id:
        return helper.create_odessa_response(
            error_code, {'device_id': device_id, 'data': data},
            message, cors=True)

    else:
        return helper.create_odessa_response(
            error_code, {'data': data}, message, cors=True)
