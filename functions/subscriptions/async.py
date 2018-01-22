import json
import logging
import sys
from functions import helper
from botocore.exceptions import ClientError
from botocore.exceptions import ConnectionError
from constants.device_response_codes import *
from constants.odessa_response_codes import *
from constants.boc_response_codes import *
from models.device_subscription import DeviceSubscription
from models.service_oid import ServiceOid

logger = logging.getLogger('subscriptions:async')
logger.setLevel(logging.INFO)


def run_subscribe(event, context):
    logger.info(f'async:run_subscribe, request: {json.dumps(event)}')
    if('device_id' not in event or
            not isinstance(event['device_id'], str) or
            'log_service_id' not in event):
        logger.warning('BadRequest on async:run_subscribe')
        return

    try:
        time_period = helper.verify_time_period(event['time_period'])
    except:
        time_period = helper.DEFAULT_TIME_PERIOD_MINS * 60
    try:
        oid_info = ServiceOid().read(event['log_service_id'])
        if not oid_info:
            logger.warning(
                f'BadRequest on async:run_subscribe (log_service_id "{event["log_service_id"]}" does not exist.)')
            return

        oid_list = oid_info['oids']
        oid_map = []
        for oid in oid_list:
            oid_map.append({'object_id': oid, 'time_period': time_period})

        device_info = DeviceSubscription()
        device_info.read(
            event['device_id'], event['log_service_id'])

        if not device_info.is_existing():
            logger.error(
                f'Error subscribing device {event["device_id"]}#event["log_service_id"]: device does not exist in Odessa')
            return

        subscription_api = helper.subscription_api_client(
            oid_info['boc_service_id'])
        boc_response = subscription_api.subscribe(
            event['device_id'], oid_map, oid_info['callback_url'], 'true')

        if(boc_response['code'] == NO_ERROR or
                boc_response['code'] == ALREADY_SUBSCRIBED_ON_SUBSCRIBE):
            boc_response = device_info.delete_unsupported_oids(boc_response)
        elif boc_response['code'] == SUCCESS_BUT_DEVICE_OFFLINE:
            device_info.update(SUBSCRIBED_OFFLINE)
        elif boc_response['code'] == DEVICE_NOT_RECOGNIZED:
            device_info.update(DEVICE_NOT_FOUND)
        elif(boc_response['code'] == PARTIAL_SUCCESS or
                boc_response['code'] == INTERNAL_ERROR):
            boc_response = device_info.delete_unsupported_oids(boc_response)
            if not ('subscribe' in boc_response and not
               len(boc_response['subscribe']) == 0 and
               helper.has_acceptable_sub_errors_only(boc_response)):
                    device_info.update_as_subscribe_error(
                        boc_response['code'], boc_response['message'])
        else:
            device_info.update_as_subscribe_error(
                boc_response['code'], boc_response['message'])
        logger.info(f'async:run_subscribe, response: nil')

    except (ClientError, ConnectionError) as e:  # pragma: no cover
        logger.error(e)
        raise
    except:  # pragma: no cover
        logger.error(sys.exc_info())
        device_info.update(SUBSCRIBE_BOC_RESPONSE_ERROR)


def run_unsubscribe(event, context):
    logger.info(f'async:run_unsubscribe, request: {json.dumps(event)}')

    if('device_id' not in event or not isinstance(event['device_id'], str) or
            'log_service_id' not in event):
        logger.warning('BadRequest on async:run_unsubscribe')
        return

    try:
        oid_info = ServiceOid().read(event['log_service_id'])
        if not oid_info:
            logger.warning(
                f'BadRequest on async:run_unsubscribe (log_service_id "{event["log_service_id"]}" does not exist.)')
            return

        oid_list = oid_info['oids']
        oid_map = []
        for oid in oid_list:
            oid_map.append({'object_id': oid})

        device_info = DeviceSubscription()
        device_info.read(
            event['device_id'], event['log_service_id'])

        if not device_info.is_existing():
            logger.error(
                f'Error unsubscribing device {event["device_id"]}#event["log_service_id"]: device does not exist in Odessa')
            return

        subscription_api = helper.subscription_api_client(
            oid_info['boc_service_id'])
        boc_response = subscription_api.unsubscribe(
            event['device_id'], oid_map)

        if(boc_response['code'] == NO_ERROR or
            boc_response['code'] == NOT_SUBSCRIBED_FROM_SERVICE_ON_UNSUBSCRIBE
                or boc_response['code'] == DEVICE_NOT_RECOGNIZED):
            device_info.delete()
        elif(boc_response['code'] == PARTIAL_SUCCESS or
                boc_response['code'] == INTERNAL_ERROR):
            if('unsubscribe' in boc_response and not
               len(boc_response['unsubscribe']) == 0 and
               helper.has_acceptable_unsub_errors_only(boc_response)):
                device_info.delete()
            else:
                device_info.update_as_unsubscribe_error(
                    boc_response['code'], boc_response['message'])
        else:
            device_info.update_as_unsubscribe_error(
                boc_response['code'], boc_response['message'])
        logger.info(f'async:run_unsubscribe, response: nil')

    except (ClientError, ConnectionError) as e:  # pragma: no cover
        logger.error(e)
        raise
    except:  # pragma: no cover
        logger.error(sys.exc_info())
        device_info.update(UNSUBSCRIBE_BOC_RESPONSE_ERROR)


def run_get_notify_result(event, context):
    logger.info(f'async:run_get_notify_result, request: {json.dumps(event)}')

    if('device_id' not in event or not isinstance(event['device_id'], str) or
            'log_service_id' not in event):
        logger.warning('BadRequest on async:run_get_notify_result')
        return

    try:
        oid_info = ServiceOid().read(event['log_service_id'])
        if not oid_info:
            logger.warning(
                f'BadRequest on async:run_get_notify_result (log_service_id "{event["log_service_id"]}" does not exist.)')
            return

        device_info = DeviceSubscription()
        device_info.read(
            event['device_id'], event['log_service_id'])

        if not device_info.is_existing():
            logger.error(
                f'Error getting notify results for device {event["device_id"]}#event["log_service_id"]: device does not exist in Odessa')
            return
        subscription_api = helper.subscription_api_client(
            oid_info['boc_service_id'])
        oids = device_info.get_subscribed_oids()
        oid_dict = []
        for oid in oids:
            oid_dict.append({'object_id': oid})

        boc_response = subscription_api.get_notify_result(event['device_id'], oid_dict)
        error_code = helper.process_get_subscription_response(
            boc_response, device_info)
        if error_code == SUBSCRIBED:
            device_info.delete_offline_unsupported_oids(boc_response)
            device_info.update(error_code)

    except (ClientError, ConnectionError) as e:  # pragma: no cover
        logger.error(e)
        raise
    except:  # pragma: no cover
        logger.error(sys.exc_info())
        device_info.update(UNSUBSCRIBE_BOC_RESPONSE_ERROR)
