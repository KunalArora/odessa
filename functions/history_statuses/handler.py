from botocore.exceptions import ClientError
from botocore.exceptions import ConnectionError
from constants import odessa_response_codes
from functions import helper
from helpers import time_functions
import json
import logging
from models.device_network_status import DeviceNetworkStatus
from models.device_subscription import DeviceSubscription
from models.reporting_registration import ReportingRegistration
from models.service_oid import ServiceOid
import re
import sys


QUERY_PARAMS_LIST = ['from', 'to']

logger = logging.getLogger('get_history_statuses')
logger.setLevel(logging.INFO)

# Retrieve the network status history for a BOC device (only)
# in a specific time interval


def get_history_statuses(event, context):
    logger.info(f'handler:get_history_statuses, request: {event}')
    service_oid = ServiceOid()
    device_subscription = DeviceSubscription()
    device_network_status = DeviceNetworkStatus()
    reporting_registration = ReportingRegistration()

    missing_params_list = []

    device_id = None
    reporting_id = None

    try:
        request_body = json.loads(event['body'])
    except (TypeError, ValueError) as e:
        logger.warning(
            f'BadRequest on handler:get_history_statuses, error occurred: {e} '
            'Reason: Parameter Request Body has incorrect format on event :'
            f'{event}')
        return history_statuses_response(
            odessa_response_codes.BAD_REQUEST, message="Request Body has "
            "incorrect format"
        )

    if 'reporting_id' in request_body:
        reporting_id = request_body['reporting_id']
    elif 'device_id' in request_body:
        device_id = request_body['device_id']
    else:
        logger.warning(
            f'BadRequest on handler:get_history_statuses, '
            f'Reason: Parameters missing from Request Body: '
            f'reporting_id/device_id')
        return history_statuses_response(
            odessa_response_codes.BAD_REQUEST, message=f'Parameters Missing: '
            f'reporting_id/device_id')

    for param in QUERY_PARAMS_LIST:
        if param not in request_body:
            missing_params_list.append(param)

    if missing_params_list:
        logger.warning(
            f'BadRequest on handler:get_history_statuses, '
            f'Reason: Parameters missing from Request Body: '
            f'{missing_params_list}')
        return history_statuses_response(
            odessa_response_codes.BAD_REQUEST,
            reporting_id, device_id, message=f'Parameters Missing: '
            f"{', '.join(missing_params_list)}")

    if device_id is not None:
        if (not isinstance(device_id, str)
                or re.match(helper.GUID_REGEX, device_id) is None):
            logger.warning(
                f"BadRequest on handler:get_history_statuses, "
                f"Reason: Parameter 'device_id' = {device_id} "
                "has incorrect format")
            return history_statuses_response(
                odessa_response_codes.BAD_REQUEST,
                device_id=device_id, message=f"Parameter "
                f"'device_id' = '{device_id}' has incorrect value")

    if reporting_id == "":
        logger.warning(
            f"BadRequest on handler:get_history_statuses, "
            f"Reason: Parameter 'reporting_id' has empty value")
        return history_statuses_response(
            odessa_response_codes.BAD_REQUEST,
            reporting_id, message=f"Parameter "
            f"'reporting_id' = '{reporting_id}' has incorrect value")

    from_time = request_body['from']
    to_time = request_body['to']

    try:
        # Test for incorrect format of parameters 'from' and 'to' or if value
        # of 'from' is greater than value of 'to'
        parsed_from_time = None
        parsed_from_time = time_functions.parse_time_with_tz(from_time)
        parsed_to_time = time_functions.parse_time_with_tz(to_time)
        if parsed_from_time >= parsed_to_time:
            logger.warning(
                f"BadRequest on handler:get_history_statuses, "
                f"Reason: Parameter 'from' = {from_time} should be less than "
                f"parameter 'to' = {to_time}")
            return history_statuses_response(
                odessa_response_codes.BAD_REQUEST,
                reporting_id, device_id,
                message=f"Parameter 'from' = {from_time}"
                f" should be less than parameter 'to' = {to_time}")
    except (TypeError, ValueError) as e:
        if parsed_from_time is None:
            logger.warning(
                f"BadRequest on handler:get_history_statuses, error occurred= "
                f"{e}, Reason: Parameter 'from' has incorrect format")
            return history_statuses_response(
                odessa_response_codes.BAD_REQUEST,
                reporting_id, device_id, message=f"Parameter "
                f"'from' has incorrect value: {from_time}")
        else:
            logger.warning(
                f"BadRequest on handler:get_history_statuses, error "
                f"occured = {e},  Reason: Parameter 'to' has incorrect format")
            return history_statuses_response(
                odessa_response_codes.BAD_REQUEST,
                reporting_id, device_id, message=f"Parameter "
                f"'to' has incorrect value: {to_time}")

    try:
        db_res = []
        if 'log_service_id' in request_body:
            log_service_id = str(request_body['log_service_id'])
            if log_service_id == "": # empty
                logger.warning(
                    f"BadRequest on handler:get_history_statuses, "
                    "Reason: Parameter 'log_service_id' has empty value: "
                    f"on event {event}")
                return history_statuses_response(
                    odessa_response_codes.BAD_REQUEST, reporting_id, device_id,
                    message=f"Parameter 'log_service_id' has incorrect "
                    f"value: {log_service_id}")
        else:
            log_service_id = '0'

        # Test if log_service_id doesn't exist in database
        oid = service_oid.read(log_service_id)
        if not oid:
            logger.warning(
                f"BadRequest on handler:get_history_statuses, "
                "Reason: Parameter 'log_service_id' has incorrect value: "
                f"on event {event}")
            return history_statuses_response(
                odessa_response_codes.BAD_REQUEST, reporting_id, device_id,
                message=f"Parameter 'log_service_id' has incorrect "
                f"value: {log_service_id}")

        # Remove Timezone +00:00 value
        from_time = time_functions.remove_tz(from_time)
        to_time = time_functions.remove_tz(to_time)

        # Cases divided into 2 parts:
        # 1. Request is using device_id (Only BOC devices)
        # 2. Request is using reporting_id (Only BOC devices)
        if device_id:
            device_subscription.read_for_history_logs(
                device_id, log_service_id)

            if not device_subscription.is_existing():
                return history_statuses_response(
                    odessa_response_codes.DEVICE_NOT_FOUND,
                    device_id=device_id)

            params = {
                'device_id': device_id,
                'from_time': from_time,
                'to_time': to_time
            }
            result = device_network_status.get_status_history(
                params)
            if result:
                db_res.extend(result)

        elif reporting_id:
            reporting_records = reporting_registration.get_reporting_records(
                reporting_id, from_time, to_time)
            if not reporting_records:
                # Reporting Id not found
                return history_statuses_response(
                    odessa_response_codes.DEVICE_NOT_FOUND, reporting_id,
                    message="Reporting ID Not Found")

            for record in reporting_records:
                if (  # Filter the BOC devices records
                    'from_time_unit' in record and 'to_time_unit' in record
                        and record['communication_type'] == 'cloud'
                        and 'device_id' in record):
                    device_id = record['device_id']
                    device_subscription.read_for_history_logs(
                        device_id, log_service_id)

                    if not device_subscription.is_existing():
                        # Case of contradiction: Reporting activated but device
                        # not subscribed/ not been subscribed in the past
                        logger.error("DB Contradiction Error")
                        logger.warning(
                            "DB Contradiction Error on handler:get_history_statuses "
                            f"for request = {request_body} Reason: Reporting "
                            "activated but device not subscribed (currently or in the past)")
                        return history_statuses_response(
                            odessa_response_codes.DB_CONTRADICTION_ERROR,
                            reporting_id)

                    params = {
                        'device_id': device_id,
                        'from_time': record['from_time_unit'],
                        'to_time': record['to_time_unit']
                    }
                    result = device_network_status.get_status_history(
                        params)
                    if result:
                        db_res.extend(result)

        # Create the history statuses API response body
        odessa_response = create_response_body(
            db_res, reporting_id, device_id)

        logger.info(
            f'handler:get_history_statuses, '
            f'response: {json.dumps(odessa_response)}')

        return odessa_response

    except (ConnectionError, ClientError) as e:
        logger.error(e)
        logger.warning(
            f'Database Error on handler:get_history_statuses '
            f'on event {event}')
        return history_statuses_response(
            odessa_response_codes.DB_CONNECTION_ERROR, reporting_id, device_id)
    except Exception:  # pragma: no cover
        # Unkown error
        logger.error(sys.exc_info())
        logger.warning(
            f'Unknown Error occurred on handler:get_history_statuses '
            f'on event {event}')
        return history_statuses_response(
            odessa_response_codes.INTERNAL_SERVER_ERROR, device_id)


def create_response_body(db_res, reporting_id=None, device_id=None):
    value = []
    updated = []

    if db_res:
        for response in db_res:
            value.append(response['status'])
            updated.append(time_functions.convert_iso(response['timestamp']))
        data = {"value": value, "updated": updated}
        return history_statuses_response(
            odessa_response_codes.SUCCESS, reporting_id, device_id, data)

    else:  # Logs Not Found
        return history_statuses_response(
            odessa_response_codes.LOGS_NOT_FOUND, reporting_id, device_id
        )


def history_statuses_response(
        error_code, reporting_id='', device_id='', data=[], message=None):
    if reporting_id:
        return helper.create_odessa_response(
            error_code, {'reporting_id': reporting_id, 'data': data}, message)

    elif device_id:
        return helper.create_odessa_response(
            error_code, {'device_id': device_id, 'data': data}, message)

    else:
        return helper.create_odessa_response(
            error_code, {'data': data}, message)
