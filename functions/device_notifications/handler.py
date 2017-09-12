import json
import logging
from functions import helper
from models.device_log import DeviceLog
from models.device_network_status import DeviceNetworkStatus
from botocore.exceptions import ClientError
from botocore.exceptions import ConnectionError
from redis import RedisError
from constants.odessa_response_codes import *

logger = logging.getLogger('device_notifications')
logger.setLevel(logging.INFO)


def save_notify_logs_db(event, context):
    device_log = DeviceLog()
    logger.info("Request parameter {}".format(event))
    try:
        request = json.loads(event['body'].replace("\\x22", "\""))
        device_id = request['device_id'] if 'device_id' in request else ''
        notification = request['notification'] if 'notification' in request else ''
        if not device_id:
            logger.warning(
                f'handler:device_notifications:notify_logs Device Id '
                f'request parameter is either null or not present '
                f'for event {event}'
                )
            raise helper.DeviceIdParameterError(event)
        if not notification:
            logger.warning(
                f'handler:device_notifications:notify_logs Notification '
                f'request parameter is either null or not present '
                f'for event {event}'
                )
            raise helper.NotificationError(event)

        #   Make sure that multiple instance of same notification
        #   is not present in the request
        redundant_data = [dict(t) for t in set(
            [tuple(d.items()) for d in request['notification']])]
        request['notification'] = redundant_data

        cache_res = device_log.is_exists_cache(request)
        db_res = device_log.is_exists_db(cache_res)

        if db_res['notification']:
            device_log.put_logs(db_res)
        return odessa_response(SUCCESS)
    except (helper.DeviceIdParameterError, helper.NotificationError) as e:
        logger.warning(
            f'handler:device_notifications:notify_logs Bad request '
            f'parameter for {event}'
            )
        return odessa_response(BAD_REQUEST)
    except ValueError as e:
        logger.warning(
            f'handler:device_notifications:notify_logs JSON format '
            f'error in the request for event {event}'
            )
        return odessa_response(BAD_REQUEST)
    except TypeError as e:
        logger.warning(
            f'handler:device_notifications:notify_logs Format '
            f'type error in the request for event {event}'
            )
        return odessa_response(BAD_REQUEST)
    except ConnectionError as e:
        logger.error(e)
        logger.warning(
            f'handler:device_notifications:notify_logs Dynamodb '
            f'connection error for event {event}'
            )
        return odessa_response(DB_CONNECTION_ERROR)
    except ClientError as e:
        logger.error(e)
        logger.warning(
            f'handler:device_notifications:notify_logs Dynamodb '
            f'client error for event {event}'
            )
        return odessa_response(DB_CONNECTION_ERROR)
    except RedisError as e:
        logger.error(e)
        logger.warning(
            f'handler:device_notifications:notify_logs Redis '
            f'error for event {event}'
            )
        return odessa_response(DB_CONNECTION_ERROR)


def save_notify_status_db(event, context):
    device_network_status = DeviceNetworkStatus()
    logger.info('Request parameter {}'.format(event))
    try:
        request = json.loads(event["Records"][0]['Sns']['Message'])
        if not request:
            logger.warning(
                f'handler:device_notifications:notify_status Message request '
                f'parameter is null for event {event}'
                )
            raise helper.NotificationError(event)
        for data in request:
            device_id = data['device_id'] if 'device_id' in data else ''
            event = data['event'] if 'event' in data else ''
            if not device_id:
                logger.warning(
                    f'handlerr:device_notifications:notify_status Device Id '
                    f'request parameter is either null or not '
                    f'present for event message data {data}'
                    )
                raise helper.DeviceIdParameterError(data)
            if not event:
                logger.warning(
                    f'handler:device_notifications:notify_status Event '
                    f'request parameter is either null or not present '
                    f'for event message data {data}'
                    )
                raise helper.EventParameterError(data)

        cache_res = device_network_status.is_exists_cache(request)
        db_res = device_network_status.is_exists_db(cache_res)
        if db_res:
            device_network_status.put_status(db_res)
    except (helper.DeviceIdParameterError, helper.EventParameterError,
            helper.NotificationError) as e:
        logger.warning(
            f'handler:device_notifications:notify_status Bad request '
            f'parameter for {event}'
            )
    except ValueError as e:
        logger.warning(
            f'handler:device_notifications:notify_status JSON format '
            f'value error in the request for event {event}'
            )
    except TypeError as e:
        logger.warning(
            f'handler:device_notifications:notify_status Format type '
            f'error in the request for event {event}'
            )
    except ConnectionError as e:
        logger.error(e)
        logger.warning(
            f'handler:device_notifications:notify_status Dynamodb '
            f'connection error for event {event}'
            )
    except ClientError as e:
        logger.error(e)
        logger.warning(
            f'handler:device_notifications:notify_status Dynamodb '
            f'client error for event {event}'
            )
    except RedisError as e:
        logger.error(e)
        logger.warning(
            f'handler:device_notifications:notify_status Redis error '
            f'for event {event}'
            )

def odessa_response(error_code, message=None):
    body = {
        'code': error_code,
        'message': message if message else helper.odessa_response_message(error_code)
    }

    return {
        'statusCode': status_code(error_code),
        'body': json.dumps(body)
    }

def status_code(code):
    success_cases = [SUCCESS, LOGS_NOT_FOUND, PARTIAL_SUCCESS, FEATURES_NOT_SUBSCRIBED]
    client_errors = [BAD_REQUEST, DEVICE_NOT_FOUND, CONFLICT]
    server_errors = [INTERNAL_SERVER_ERROR, MISSING_FIELD_OBJECT_ID, ERROR,
        DB_CONNECTION_ERROR, BOC_DB_CONNECTION_ERROR, BOC_API_CALL_ERROR]
    if code in success_cases:
        return 200
    elif code in client_errors:
        return 400
    elif code in server_errors:
        return 500
