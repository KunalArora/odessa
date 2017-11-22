import json
import logging
from botocore.exceptions import ConnectionError
from botocore.exceptions import ClientError
from constants.odessa_response_codes import *
from functions import helper
from functions.reporting_registrations.bad_request_messages import *
from models.reporting_registration import ReportingRegistration
from models.device_subscription import DeviceSubscription
from models.service_oid import ServiceOid

logger = logging.getLogger('reporting_registrations')
logger.setLevel(logging.INFO)


def save_reporting_registration(event, context):
    reporting_registration = ReportingRegistration()
    device_subscription = DeviceSubscription()
    service_oid = ServiceOid()

    try:
        if isinstance(event['body'], (str, bytes)):
            request = json.loads(event['body'])
        res_basic = basic_validation(request)
        if res_basic:
            return res_basic
        subscribe_res = True

        if 'log_service_id' in request and request['log_service_id']:
            request['log_service_id'] = str(request['log_service_id'])
        else:
            request['log_service_id'] = '0'
        comm_type = request['communication_type'].lower()

        oid = service_oid.read(request['log_service_id'])
        if not oid:
            logger.warning(
                "handler:reporting_registration Log service Id doesn't exist "
                "in the database for request {}".format(request))
            return helper.reporting_registration_response(BAD_REQUEST, SERVICE_ID_NOT_EXIST)

        if comm_type == 'email':
            if not 'serial_number' in request:
                logger.warning(
                    "handler:reporting_registration Serial number key is "
                    "missing for request {}".format(request)
                )
                return helper.reporting_registration_response(BAD_REQUEST, SERIAL_NO_MISSING)
            if not request['serial_number']:
                logger.warning(
                    "handler:reporting_registration Serial number key is "
                    "empty for request {}".format(request)
                )
                return helper.reporting_registration_response(BAD_REQUEST, SERIAL_NO_EMPTY)
            if not isinstance(request['serial_number'], str):
                logger.warning(
                    "handler:reporting_registration Serial number key is "
                    "not in string format for request {}".format(request)
                )
                return helper.reporting_registration_response(BAD_REQUEST, SERIAL_NO_NOT_STRING)


        if comm_type == 'cloud':
            if not 'device_id' in request:
                logger.warning(
                    "handler:reporting_registration Device Id key is "
                    "missing for request {}".format(request)
                )
                return helper.reporting_registration_response(BAD_REQUEST, DEVICE_ID_MISSING)
            if not request['device_id']:
                logger.warning(
                    "handler:reporting_registration Device Id key is "
                    "empty for request {}".format(request)
                )
                return helper.reporting_registration_response(BAD_REQUEST, DEVICE_ID_EMPTY)
            if not isinstance(request['device_id'], str):
                logger.warning(
                    "handler:reporting_registration Device Id key is "
                    "not in string format for request {}".format(request)
                )
                return helper.reporting_registration_response(BAD_REQUEST, DEVICE_ID_NOT_STRING)
            if request['device_id']:
                subscribe_res = device_subscription.verify_subscribe(
                    request['device_id'].lower(), request['log_service_id'])

        if subscribe_res:
            request['communication_type'] = comm_type
            reporting_registration.create(request)
            return helper.reporting_registration_response(SUCCESS)
        else:
            logger.warning("handler:reporting_registration, {} device_id is not"
                           " subscribed.".format(request['device_id']))
            return helper.reporting_registration_response(DEVICE_NOT_FOUND)
    except ValueError as e:
        logger.warning(
            "handler:reporting_registration JSON format Value error in the "
            "request for event {}".format(event))
        return helper.reporting_registration_response(BAD_REQUEST, JSON_FORMAT)
    except TypeError as e:
        logger.warning(
            "handler:reporting_registration Format Type error in the "
            "request for event {}".format(event))
        return helper.reporting_registration_response(BAD_REQUEST, JSON_FORMAT)
    except ConnectionError as e:
        logger.error(e)
        logger.warning(
            "handler:reporting_registration Dynamodb Connection Error "
            "for request {}".format(request))
        return helper.reporting_registration_response(DB_CONNECTION_ERROR)
    except ClientError as e:  # pragma: no cover
        logger.error(e)
        logger.warning(
            "handler:reporting_registration Dynamodb Client Error "
            "for request {}".format(request))
        return helper.reporting_registration_response(DB_CONNECTION_ERROR)
    except:
        logger.warning(
            "handler:reporting_registration Unknown Error "
            "for request {}".format(request))
        return helper.reporting_registration_response(INTERNAL_SERVER_ERROR)


def basic_validation(request):
    comm_type = ['cloud', 'email']
    if not 'reporting_id' in request:
        logger.warning(
            "handler:reporting_registration Reporting Id key is missing "
            "for request {}".format(request)
        )
        return helper.reporting_registration_response(BAD_REQUEST, REPORTING_ID_MISSING)
    if not request['reporting_id']:
        logger.warning(
            "handler:reporting_registration Reporting Id key is empty "
            "for request {}".format(request)
        )
        return helper.reporting_registration_response(BAD_REQUEST, REPORTING_ID_EMPTY)
    if not (isinstance(request['reporting_id'], str)):
        logger.warning(
            "handler:reporting_registration Reporting Id key is not in "
            "string format for request {}".format(request)
        )
        return helper.reporting_registration_response(BAD_REQUEST, REPORTING_ID_NOT_STRING)
    if not 'communication_type' in request:
        logger.warning(
            "handler:reporting_registration Communication type key is missing "
            "for request {}".format(request)
        )
        return helper.reporting_registration_response(BAD_REQUEST, COMM_TYPE_MISSING)
    if not request['communication_type']:
        logger.warning(
            "handler:reporting_registration Communication type key is empty "
            "for request {}".format(request)
        )
        return helper.reporting_registration_response(BAD_REQUEST, COMM_TYPE_EMPTY)
    if request['communication_type']:
        if not isinstance(request['communication_type'], str):
            logger.warning(
                "handler:reporting_registration Communication type key is "
                "not in string format for request {}".format(request)
            )
            return helper.reporting_registration_response(BAD_REQUEST, COMM_TYPE_NOT_STRING)
        if not (request['communication_type'].lower() in comm_type):
            logger.warning(
                "handler:reporting_registration Communication type key is "
                "neither cloud nor email for request {}".format(request)
            )
            return helper.reporting_registration_response(BAD_REQUEST, COMM_TYPE_INVALID)
    if 'log_service_id' in request and request['log_service_id']:
        if not isinstance(request['log_service_id'], (str, int)):
            logger.warning(
                "handler:reporting_registration Log service Id key is neither "
                "string nor integer format for request {}".format(request)
            )
            return helper.reporting_registration_response(BAD_REQUEST, SERVICE_ID_NOT_STRING)
    return False
