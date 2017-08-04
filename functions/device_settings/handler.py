from boc.device_info import DeviceInfo
from boc.exceptions import ParamsMissingError
from constants.odessa_response_codes import *
from constants.boc_response_codes import SERVER_ERROR
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

    if (not isinstance(event['body'], (str, bytes)) or not event['body']):
        logger.warning('BadRequest on handler:get_device_settings')
        return helper.device_settings_response(BAD_REQUEST)

    data = json.loads(event['body'])

    if ('device_id' not in data or 'setting' not in data):
        logger.warning('BadRequest on handler:get_device_settings')
        return helper.device_settings_response(BAD_REQUEST)

    device_id = data['device_id']
    object_id_list = data['setting']

    if ((not isinstance(device_id, str) and not isinstance(device_id, list)) or
            not device_id or not object_id_list):
        logger.warning('BadRequest on handler:get_device_settings')
        return helper.device_settings_response(BAD_REQUEST)

    # In case of device_id being a list, choose the first element of the list
    # to be the only device_id to be processed
    if isinstance(device_id, list):
        device_id = device_id[0]

    if 'log_service_id' in data:
        log_service_id = str(data['log_service_id'])
    else:
        log_service_id = '0'

    try:
        oid_info = ServiceOid().read(log_service_id)
        if not oid_info:
            logger.warning(
                f'BadRequest on handler:get_device_settings '
                f'(log_service_id "{log_service_id}" does not exist.)')
            return helper.device_settings_response(
                BAD_REQUEST)

        boc_service_id = oid_info['boc_service_id']

        device_info = DeviceInfo(
            boc_service_id, environ['BOC_BASE_URL'])
        boc_response = device_info.get(
            device_id, object_id_list, int(environ['BOC_API_CALL_TIMEOUT']))
    except error.HTTPError as e:
        logger.warning(
            f'BOC Connection Error on GetDeviceSetting '
            f'for event: {event}')
        logger.error(f'Error code: {e.code}, Reason: {e.reason}')
        return helper.device_settings_response(
            BOC_API_CALL_ERROR, device_id)
    except error.URLError as e:
        logger.warning(
            f'BOC Connection Error on GetDeviceSetting '
            f'for event: {event}')
        logger.error(f'Reason: {e.reason}')
        return helper.device_settings_response(
            BOC_API_CALL_ERROR, device_id)
    except ValueError:
        logger.warning(
            f'BOC response decode error on GetDeviceSetting '
            f'for event: {event}')
        return helper.device_settings_response(ERROR, device_id)
    except ParamsMissingError as e:
        logger.warning(
            f'BOC request parameters missing error on GetDeviceSetting '
            f'for event: {event}')
        logger.error(f'Error code: {e.code}, Reason: {e.reason}')
        return helper.device_settings_response(BAD_REQUEST)
    except socket.timeout as e:
        logger.warning(
            f'BOC API call socket timeout error on GetDeviceSetting '
            f'for event: {event}')
        return helper.device_settings_response(
            BOC_API_CALL_ERROR, device_id)

    data_get = boc_response['get'] if 'get' in boc_response else []

    return helper.device_settings_response(
        int(boc_response['code']), device_id, boc_response['message'], data_get)


def set(event, context):
    logger.info(event)

    if (not isinstance(event['body'], (str, bytes)) or not event['body']):
        logger.warning('BadRequest on handler:set_device_settings')
        return helper.device_settings_response(BAD_REQUEST)

    data = json.loads(event['body'])

    if ('device_id' not in data or 'setting' not in data):
        logger.warning('BadRequest on handler:set_device_settings')
        return helper.device_settings_response(BAD_REQUEST)

    device_id = data['device_id']
    object_id_value_list = data['setting']

    if (not isinstance(device_id, str) and not isinstance(device_id, list) or
            not device_id or not object_id_value_list):
        logger.warning('BadRequest on handler:set_device_settings')
        return helper.device_settings_response(BAD_REQUEST)

    # In case of device_id being a list, choose the first element of the list
    # to be the only device_id to be processed
    if isinstance(device_id, list):
        device_id = device_id[0]

    for item in object_id_value_list:
        if 'object_id' not in item:
            logger.warning('Missing field object_id on '
            'handler:set_device_settings')
            return helper.device_settings_response(
                MISSING_FIELD_OBJECT_ID, device_id)

    # Remove redundancy of different values for same object_id
    # Choose the last value in the list
    object_id_value_list = {data['object_id']: data for data in object_id_value_list}.values()

    if 'log_service_id' in data:
        log_service_id = str(data['log_service_id'])
    else:
        log_service_id = '0'

    try:
        oid_info = ServiceOid().read(log_service_id)
        if not oid_info:
            logger.warning(
                f'BadRequest on handler:set_device_settings '
                f'(log_service_id "{log_service_id}" does not exist.)')
            return helper.device_settings_response(
                BAD_REQUEST)

        boc_service_id = oid_info['boc_service_id']

        device_info = DeviceInfo(
            boc_service_id, environ['BOC_BASE_URL'])
        boc_response = device_info.set(
            device_id, object_id_value_list, int(
                environ['BOC_API_CALL_TIMEOUT']))
    except error.HTTPError as e:
        logger.warning(
            f'BOC Connection Error on SetDeviceSetting for '
            f'event: {event}')
        logger.error(f'Error code: {e.code}, Reason: {e.reason}')
        return helper.device_settings_response(
            BOC_API_CALL_ERROR, device_id)
    except error.URLError as e:
        logger.warning(
            f'BOC Connection Error on GetDeviceSetting '
            f'for event: {event}')
        logger.error('Reason: {e.reason}')
        return helper.device_settings_response(
            BOC_API_CALL_ERROR, device_id)
    except ValueError:
        logger.warning(
            f'BOC response decode error on SetDeviceSetting '
            f'for event: {event}')
        return helper.device_settings_response(
            ERROR, device_id)
    except ParamsMissingError as e:
        logger.warning(
            f'BOC request parameters missing error on SetDeviceSetting '
            f'for event: {event}')
        logger.error(f'Error code: {e.code}, Reason: {e.reason}')
        return helper.device_settings_response(BAD_REQUEST)
    except socket.timeout as e:
        logger.warning(
            f'BOC API call socket timeout error on GetDeviceSetting '
            f'for event: {event}')
        return helper.device_settings_response(
            BOC_API_CALL_ERROR, device_id)

    if int(boc_response['code']) == SERVER_ERROR:
        logger.warning(
            f'Server Error due to incorrect parameters for '
            f'event: {event}')
        return helper.device_settings_response(
            BOC_API_CALL_ERROR, device_id)

    data_set = boc_response['set'] if 'set' in boc_response else []

    return helper.device_settings_response(
        int(boc_response['code']), device_id, boc_response['message'], data_set)
