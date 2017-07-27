from boc.device_info import DeviceInfo
from boc.exceptions import ParamsMissingError
from constants.odessa_response_codes import *
from functions import helper
import json
import logging
from models.service_oid import ServiceOid
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

    if 'log_service_id' in data:
        log_service_id = data['log_service_id']
    else:
        log_service_id = '0'

    try:
        oid_info = ServiceOid().read(log_service_id)
        if not oid_info:
            logger.warning(
                f'BadRequest on handler:get_device_settings (log_service_id "{log_service_id}" does not exist.)')
            return helper.device_settings_response(BAD_REQUEST)

        boc_service_id = oid_info['boc_service_id']

        device_info = DeviceInfo(
            boc_service_id, environ['BOC_BASE_URL'])
        boc_response = device_info.get(
            device_id, object_id_list, int(environ['BOC_API_CALL_TIMEOUT']))
    except error.HTTPError as e:
        logger.warning(
            'BOC Connection Error on GetDeviceSetting '
            'for event: {}'.format(event))
        logger.error('Error code: {}, Reason: {}'.format(e.code, e.reason))
        return helper.error_response(
            device_id, BOC_API_CALL_ERROR, helper.odessa_response_message(BOC_API_CALL_ERROR))
    except error.URLError as e:
        logger.warning('BOC Connection Error on GetDeviceSetting '
                       'for event: {}'.format(event))
        logger.error('Reason: {}'.format(e.reason))
        return helper.error_response(
            device_id, BOC_API_CALL_ERROR, helper.odessa_response_message(BOC_API_CALL_ERROR))
    except ValueError:
        logger.warning(
            'BOC response decode error on GetDeviceSetting '
            'for event: {}'.format(event))
        return helper.error_response(device_id, ERROR, helper.odessa_response_message(ERROR))
    except ParamsMissingError as e:
        logger.warning(
            'BOC request parameters missing error on GetDeviceSetting '
            'for event: {}'.format(event))
        logger.error('Error code: {}, Reason: {}'.format(e.code, e.reason))
        return helper.error_response(
            device_id, PARAMS_MISSING_ERROR, helper.odessa_response_message(
                PARAMS_MISSING_ERROR, e.reason))
    except socket.timeout as e:
        logger.warning(
            'BOC API call socket timeout error on GetDeviceSetting '
            'for event: {}'.format(event))
        return helper.error_response(
            device_id, BOC_API_CALL_ERROR, helper.odessa_response_message(BOC_API_CALL_ERROR))

    return helper.create_response(device_id, boc_response)


def set(event, context):
    logger.info(event)
    data = json.loads(event['body'])
    device_id = data['device_id']
    object_id_value_list = data['setting']

    if 'log_service_id' in data:
        log_service_id = data['log_service_id']
    else:
        log_service_id = '0'

    try:
        oid_info = ServiceOid().read(log_service_id)
        if not oid_info:
            logger.warning(
                f'BadRequest on handler:set_device_settings (log_service_id "{log_service_id}" does not exist.)')
            return helper.device_settings_response(BAD_REQUEST)

        boc_service_id = oid_info['boc_service_id']

        device_info = DeviceInfo(
            boc_service_id, environ['BOC_BASE_URL'])
        boc_response = device_info.set(
            device_id, object_id_value_list, int(
                environ['BOC_API_CALL_TIMEOUT']))
    except error.HTTPError as e:
        logger.warning(
            'BOC Connection Error on SetDeviceSetting for '
            'event: {}'.format(event))
        logger.error('Error code: {}, Reason: {}'.format(e.code, e.reason))
        return helper.error_response(
            device_id, BOC_API_CALL_ERROR, helper.odessa_response_message(BOC_API_CALL_ERROR))
    except error.URLError as e:
        logger.warning('BOC Connection Error on GetDeviceSetting '
                       'for event: {}'.format(event))
        logger.error('Reason: {}'.format(e.reason))
        return helper.error_response(
            device_id, BOC_API_CALL_ERROR, helper.odessa_response_message(BOC_API_CALL_ERROR))
    except ValueError:
        logger.warning(
            'BOC response decode error on SetDeviceSetting '
            'for event: {}'.format(event))
        return helper.error_response(device_id, ERROR, helper.odessa_response_message(ERROR))
    except ParamsMissingError as e:
        logger.warning(
            'BOC request parameters missing error on SetDeviceSetting '
            'for event: {}'.format(event))
        logger.error('Error code: {}, Reason: {}'.format(e.code, e.reason))
        return helper.error_response(
            device_id, PARAMS_MISSING_ERROR, helper.odessa_response_message(
                PARAMS_MISSING_ERROR, e.reason))
    except socket.timeout as e:
        logger.warning(
            'BOC API call socket timeout error on GetDeviceSetting '
            'for event: {}'.format(event))
        return helper.error_response(
            device_id, BOC_API_CALL_ERROR, helper.odessa_response_message(BOC_API_CALL_ERROR))

    return helper.create_response(device_id, boc_response)
