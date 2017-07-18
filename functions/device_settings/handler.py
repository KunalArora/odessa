from boc.device_info import DeviceInfo
from boc.exceptions import ParamsMissingError
from constants.odessa_response_codes import BOC_API_CALL_ERROR
from constants.odessa_response_codes import ERROR
from constants.odessa_response_codes import PARAMS_MISSING_ERROR
from functions.helper import odessa_response_message
from functions.helper import create_response
from functions.helper import error_response
import json
import logging
from os import environ
import socket
from urllib import error


logger = logging.getLogger('device_settings')
logger.setLevel(logging.INFO)


def get(event, context):
    logger.info(event)
    data = json.loads(event['body'])
    device_id = data['device_id']
    object_id_list = data['setting']

    try:
        device_info = DeviceInfo(
            environ['DEFAULT_BOC_SERVICE_ID'], environ['BOC_BASE_URL'])
        boc_response = device_info.get(
            device_id, object_id_list, int(environ['BOC_API_CALL_TIMEOUT']))
    except error.HTTPError as e:
        logger.warning(
            'BOC Connection Error on GetDeviceSetting '
            'for event: {}'.format(event))
        logger.error('Error code: {}, Reason: {}'.format(e.code, e.reason))
        return error_response(
            device_id, BOC_API_CALL_ERROR, odessa_response_message(BOC_API_CALL_ERROR))
    except error.URLError as e:
        logger.warning('BOC Connection Error on GetDeviceSetting '
                       'for event: {}'.format(event))
        logger.error('Reason: {}'.format(e.reason))
        return error_response(
            device_id, BOC_API_CALL_ERROR, odessa_response_message(BOC_API_CALL_ERROR))
    except ValueError:
        logger.warning(
            'BOC response decode error on GetDeviceSetting '
            'for event: {}'.format(event))
        return error_response(device_id, ERROR, odessa_response_message(ERROR))
    except ParamsMissingError as e:
        logger.warning(
            'BOC request parameters missing error on GetDeviceSetting '
            'for event: {}'.format(event))
        logger.error('Error code: {}, Reason: {}'.format(e.code, e.reason))
        return error_response(
            device_id, PARAMS_MISSING_ERROR, odessa_response_message(
                PARAMS_MISSING_ERROR, e.reason))
    except socket.timeout as e:
        logger.warning(
            'BOC API call socket timeout error on GetDeviceSetting '
            'for event: {}'.format(event))
        return error_response(
            device_id, BOC_API_CALL_ERROR, odessa_response_message(BOC_API_CALL_ERROR))

    return create_response(device_id, boc_response)


def set(event, context):
    logger.info(event)
    data = json.loads(event['body'])
    device_id = data['device_id']
    object_id_value_list = data['setting']

    try:
        device_info = DeviceInfo(
            environ['DEFAULT_BOC_SERVICE_ID'], environ['BOC_BASE_URL'])
        boc_response = device_info.set(
            device_id, object_id_value_list, int(
                environ['BOC_API_CALL_TIMEOUT']))
    except error.HTTPError as e:
        logger.warning(
            'BOC Connection Error on SetDeviceSetting for '
            'event: {}'.format(event))
        logger.error('Error code: {}, Reason: {}'.format(e.code, e.reason))
        return error_response(
            device_id, BOC_API_CALL_ERROR, odessa_response_message(BOC_API_CALL_ERROR))
    except error.URLError as e:
        logger.warning('BOC Connection Error on GetDeviceSetting '
                       'for event: {}'.format(event))
        logger.error('Reason: {}'.format(e.reason))
        return error_response(
            device_id, BOC_API_CALL_ERROR, odessa_response_message(BOC_API_CALL_ERROR))
    except ValueError:
        logger.warning(
            'BOC response decode error on SetDeviceSetting '
            'for event: {}'.format(event))
        return error_response(device_id, ERROR, odessa_response_message(ERROR))
    except ParamsMissingError as e:
        logger.warning(
            'BOC request parameters missing error on SetDeviceSetting '
            'for event: {}'.format(event))
        logger.error('Error code: {}, Reason: {}'.format(e.code, e.reason))
        return error_response(
            device_id, PARAMS_MISSING_ERROR, odessa_response_message(
                PARAMS_MISSING_ERROR, e.reason))
    except socket.timeout as e:
        logger.warning(
            'BOC API call socket timeout error on GetDeviceSetting '
            'for event: {}'.format(event))
        return error_response(
            device_id, BOC_API_CALL_ERROR, odessa_response_message(BOC_API_CALL_ERROR))

    return create_response(device_id, boc_response)
