import sys
import json
import logging
from botocore.exceptions import ClientError
from botocore.exceptions import ConnectionError
from collections import OrderedDict
from redis import RedisError
from functions import helper
from models.device_subscription import DeviceSubscription
from models.device_subscription import device_error_message
from models.service_oid import ServiceOid
from constants.device_response_codes import *
from constants.odessa_response_codes import *
from constants.boc_response_codes import *

logger = logging.getLogger('subscriptions')
logger.setLevel(logging.INFO)


def subscribe(event, context):
    logger.info(f'handler:subscribe, request: {event}')
    data = json.loads(event['body'])
    device_list = []

    accept_exists = False
    conflict_exists = False
    unsubscribe_error_exists = False
    db_error_exists = False

    if ('device_id' not in data
            or not (isinstance(data['device_id'], list)
                    or isinstance(data['device_id'], str))
            or not len(data['device_id']) > 0):
        logger.warning('BadRequest on handler:subscribe')
        return helper.subscriptions_response(BAD_REQUEST)

    if 'log_service_id' in data:
        log_service_id = str(data['log_service_id'])
    else:
        log_service_id = '0'

    if 'time_period' in data:
        try:
            data['time_period'] = int(data['time_period'])
        except:  # Non-integer strings return bad request
            logger.warning('BadRequest on handler:subscribe')
            return helper.subscriptions_response(BAD_REQUEST)
    else:
        data['time_period'] = helper.DEFAULT_TIME_PERIOD_MINS

    if isinstance(data['device_id'], str):
        data['device_id'] = [data['device_id']]

    # Remove duplicates while keeping order
    data['device_id'] = list(OrderedDict.fromkeys(data['device_id']))

    try:
        if not ServiceOid().read(log_service_id):
            logger.warning(
                f'BadRequest on handler:subscribe (log_service_id "{log_service_id}" does not exist.)')
            return helper.subscriptions_response(BAD_REQUEST)
    except (ClientError, ConnectionError,
            RedisError) as e:  # pragma: no cover
        logger.error(e)
        return helper.subscriptions_response(DB_CONNECTION_ERROR)

    for device_id in data['device_id']:
        try:
            device_id = device_id.lower()
            device_info = DeviceSubscription()
            device_info.read(device_id, log_service_id)
            if not device_info.is_existing():
                error_code = SUBSCRIBE_ACCEPTED
                device_info.insert(device_id, log_service_id, error_code)
                message = device_info.get_message()
                helper.invoke_run_subscribe(
                    device_id, log_service_id, data['time_period'])
                accept_exists = True
            elif (device_info.is_subscribed() or
                  device_info.is_offline() or
                  device_info.is_subscribe_error() or
                  device_info.is_not_found()):
                error_code = SUBSCRIBE_ACCEPTED
                device_info.update(error_code)
                message = device_info.get_message()
                helper.invoke_run_subscribe(
                    device_id, log_service_id, data['time_period'])
                accept_exists = True
            elif device_info.is_subscribing():
                error_code = SUBSCRIBE_EXCLUSIVE_CONTROL_ERROR_WITH_OTHER_SUBS
                message = device_error_message(error_code)
                conflict_exists = True
            elif device_info.is_unsubscribing():
                error_code = SUBSCRIBE_EXCLUSIVE_CONTROL_ERROR_WITH_OTHER_UNSUBS
                message = device_error_message(error_code)
                conflict_exists = True
            elif device_info.is_unsubscribe_error():
                error_code = device_info.get_status()
                message = device_info.get_message()
            else:  # re-subscribe unexpected error_codes
                error_code = SUBSCRIBE_ACCEPTED
                device_info.update(error_code)
                message = device_info.get_message()
                helper.invoke_run_subscribe(
                    device_id, log_service_id, data['time_period'])
                accept_exists = True
            device_list.append({
                'error_code': error_code, 'device_id': device_id,
                'message': message})

        except (ClientError, ConnectionError,
                RedisError) as e:  # pragma: no cover
            logger.error(e)
            device_info.update(SUBSCRIBE_COMMUNICATION_ERROR)
            db_error_exists = True
            device_list.append({
                'error_code': device_info.get_status(),
                'device_id': device_id,
                'message': device_info.get_message()})
        except:  # pragma: no cover
            logger.error(sys.exc_info())
            device_info.update(UNKNOWN)
            device_list.append({
                'error_code': device_info.get_status(),
                'device_id': device_id,
                'message': device_info.get_message()})

    if accept_exists:
        if (conflict_exists or unsubscribe_error_exists):
            response = helper.subscriptions_response(PARTIAL_SUCCESS,
                                                     device_list)
        else:
            response = helper.subscriptions_response(SUCCESS, device_list)
    elif conflict_exists:
        response = helper.subscriptions_response(CONFLICT, device_list)
    elif db_error_exists:  # pragma: no cover
        response = helper.subscriptions_response(DB_CONNECTION_ERROR, device_list)
    else:
        response = helper.subscriptions_response(ERROR, device_list)

    logger.info(f'handler:subscribe, response: {json.dumps(response)}')
    return response


def unsubscribe(event, context):
    logger.info(f'handler:unsubscribe, request: {event}')
    data = json.loads(event['body'])
    device_list = []

    complete_exists = False
    accept_exists = False
    error_exists = False
    db_error_exists = False

    if ('device_id' not in data
            or not (isinstance(data['device_id'], list)
                    or isinstance(data['device_id'], str))
            or not len(data['device_id']) > 0):
        logger.warning('BadRequest on handler:unsubscribe')
        return helper.subscriptions_response(BAD_REQUEST)

    if 'log_service_id' in data:
        log_service_id = str(data['log_service_id'])
    else:
        log_service_id = '0'

    if isinstance(data['device_id'], str):
        data['device_id'] = [data['device_id']]

    # Remove duplicates while keeping order
    data['device_id'] = list(OrderedDict.fromkeys(data['device_id']))

    try:
        if not ServiceOid().read(log_service_id):
            logger.warning(
                f'BadRequest on handler:unsubscribe (log_service_id "{log_service_id}" does not exist.)')
            return helper.subscriptions_response(BAD_REQUEST)
    except (ClientError, ConnectionError,
            RedisError) as e:  # pragma: no cover
        logger.error(e)
        return helper.subscriptions_response(DB_CONNECTION_ERROR)

    for device_id in data['device_id']:
        try:
            device_id = device_id.lower()
            device_info = DeviceSubscription()
            device_info.read(device_id, log_service_id)
            if not device_info.is_existing():
                error_code = NOT_SUBSCRIBED
                message = device_error_message(error_code)
                complete_exists = True
            elif(device_info.is_subscribe_error() or
                 device_info.is_not_found()):
                error_code = NOT_SUBSCRIBED
                message = device_error_message(error_code)
                device_info.delete()
                complete_exists = True
            elif (device_info.is_subscribed() or
                  device_info.is_offline() or
                  device_info.is_unsubscribe_error()):
                error_code = UNSUBSCRIBE_ACCEPTED
                device_info.update(error_code)
                message = device_info.get_message()
                helper.invoke_run_unsubscribe(device_id, log_service_id)
                accept_exists = True
            elif device_info.is_subscribing():
                error_code = UNSUBSCRIBE_EXCLUSIVE_CONTROL_ERROR_WITH_OTHER_SUBS
                message = device_error_message(error_code)
                error_exists = True
            elif device_info.is_unsubscribing():
                error_code = UNSUBSCRIBE_EXCLUSIVE_CONTROL_ERROR_WITH_OTHER_UNSUBS
                message = device_error_message(error_code)
                error_exists = True
            else:  # Unsubscribe unexpected error_codes
                error_code = UNSUBSCRIBE_ACCEPTED
                device_info.update(error_code)
                message = device_info.get_message()
                helper.invoke_run_unsubscribe(device_id, log_service_id)
                accept_exists = True
            device_list.append({
                'error_code': error_code, 'device_id': device_id,
                'message': message})

        except (ClientError, ConnectionError,
                RedisError) as e:  # pragma: no cover
            logger.error(e)
            device_info = DeviceSubscription()
            device_info.read(device_id, log_service_id)
            device_info.update(UNSUBSCRIBE_COMMUNICATION_ERROR)
            db_error_exists = True
            device_list.append({
                'error_code': device_info.get_status(),
                'device_id': device_id,
                'message': device_info.get_message()})
        except:  # pragma: no cover
            logger.error(sys.exc_info())
            device_info = DeviceSubscription()
            device_info.read(device_id, log_service_id)
            device_info.update(UNKNOWN)
            error_exists = True
            device_list.append({
                'error_code': device_info.get_status(),
                'device_id': device_id,
                'message': device_info.get_message()})

    if complete_exists or accept_exists:
        if error_exists or db_error_exists:
            response = helper.subscriptions_response(PARTIAL_SUCCESS,
                                                     device_list)
        else:
            response = helper.subscriptions_response(SUCCESS, device_list)
    elif db_error_exists:  # pragma: no cover
        response = helper.subscriptions_response(
            DB_CONNECTION_ERROR, device_list)
    else:
        response = helper.subscriptions_response(CONFLICT, device_list)

    logger.info(f'handler:unsubscribe, response: {json.dumps(response)}')
    return response


def subscription_info(event, context):
    logger.info(f'handler:subscription_info, request: {event}')
    data = json.loads(event['body'])
    device_list = []

    if ('device_id' not in data
            or not (isinstance(data['device_id'], list)
                    or isinstance(data['device_id'], str))
            or not len(data['device_id']) > 0):
        logger.warning('BadRequest on handler:subscription_info')
        return helper.subscriptions_response(BAD_REQUEST)

    if 'log_service_id' in data:
        log_service_id = str(data['log_service_id'])
    else:
        log_service_id = '0'

    if isinstance(data['device_id'], str):
        data['device_id'] = [data['device_id']]

    # Remove duplicates while keeping order
    data['device_id'] = list(OrderedDict.fromkeys(data['device_id']))

    try:
        if not ServiceOid().read(log_service_id):
            logger.warning(
                f'BadRequest on handler:subscription_info (log_service_id "{log_service_id}" does not exist.)')
            return helper.subscriptions_response(BAD_REQUEST)
    except (ClientError, ConnectionError,
            RedisError) as e:  # pragma: no cover
        logger.error(e)
        return helper.subscriptions_response(DB_CONNECTION_ERROR)

    for device_id in data['device_id']:
        try:
            device_id = device_id.lower()
            device_info = DeviceSubscription()
            device_info.read(device_id, log_service_id)
        except (ClientError, ConnectionError,
                RedisError) as e:  # pragma: no cover
            logger.error(e)
            return helper.subscriptions_response(DB_CONNECTION_ERROR)
        except:  # pragma: no cover
            logger.error(sys.exc_info())
            return helper.subscriptions_response(ERROR)

        try:
            if not device_info.is_existing():
                error_code = NOT_SUBSCRIBED
                message = device_error_message(error_code)
            else:
                error_code = device_info.get_status()
                message = device_info.get_message()
                if device_info.is_offline():
                    helper.invoke_run_get_notify_result(
                        device_id,
                        device_info.get_log_service_id())
            device_list.append({
                'error_code': error_code, 'device_id': device_id,
                'message': message})

        except (ClientError, ConnectionError,
                RedisError) as e:  # pragma: no cover
            logger.error(e)
            device_list.append({
                'error_code': device_info.get_status(),
                'device_id': device_id,
                'message': device_info.get_message()})
        except:  # pragma: no cover
            logger.error(sys.exc_info())
            device_list.append({
                'error_code': device_info.get_status(),
                'device_id': device_id,
                'message': device_info.get_message()})

    response = helper.subscriptions_response(SUCCESS, device_list)
    logger.info(f'handler:subscription_info, response: {json.dumps(response)}')
    return response
