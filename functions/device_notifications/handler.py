import json
import logging
from functions import helper
from models.device_log import DeviceLog
from models.device_network_status import DeviceNetworkStatus
from botocore.exceptions import ClientError
from botocore.exceptions import ConnectionError
from redis import RedisError

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
                "Device Id request parameter is either null or not present "
                "for event {}".format(event))
            raise helper.DeviceIdParameterError(event)
        if not notification:
            logger.warning(
                "Notification request parameter is either null or not present "
                "for event {}".format(event))
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
    except (helper.DeviceIdParameterError, helper.NotificationError) as e:
        logger.error(e)
    except ValueError as e:
        logger.error(e)
        logger.warning(
            "JSON format error in the request for event {}".format(event))
    except TypeError as e:
        logger.error(e)
        logger.warning(
            "Format Type error in the request for event {}".format(event))
    except ConnectionError as e:
        logger.error(e)
        logger.warning(
            "Dynamodb Connection Error on GetDeviceLog "
            "for event {}".format(event))
    except ClientError as e:
        logger.error(e)
        logger.warning(
            "Dynamodb Client Error on GetDeviceLog for event {}".format(event))
    except RedisError as e:
        logger.error(e)
        logger.warning(
            "Redis Error on GetDeviceLog for event {}".format(event))


def save_notify_status_db(event, context):
    device_network_status = DeviceNetworkStatus()
    logger.info('Request parameter {}'.format(event))
    try:
        request = json.loads(event["Records"][0]['Sns']['Message'])
        if not request:
            logger.warning(
                "Message request parameter is null for event {}".format(event))
            raise helper.NotificationError(event)
        for data in request:
            device_id = data['device_id'] if 'device_id' in data else ''
            event = data['event'] if 'event' in data else ''
            if not device_id:
                logger.warning(
                    "Device Id request parameter is either null or not "
                    "present for event message data {}".format(data))
                raise helper.DeviceIdParameterError(data)
            if not event:
                logger.warning(
                    "Event request parameter is either null or not present "
                    "for event message data {}".format(data))
                raise helper.EventParameterError(data)

        cache_res = device_network_status.is_exists_cache(request)
        db_res = device_network_status.is_exists_db(cache_res)
        if db_res:
            device_network_status.put_status(db_res)
    except (helper.DeviceIdParameterError, helper.EventParameterError,
            helper.NotificationError) as e:
        logger.error(e)
    except ValueError as e:
        logger.error(e)
        logger.warning(
            "JSON format Value error in the request for event {}".format(event))
    except TypeError as e:
        logger.error(e)
        logger.warning(
            "Format Type error in the request for event {}".format(event))
    except ConnectionError as e:
        logger.error(e)
        logger.warning(
            "Dynamodb Connection Error on GetDeviceLog "
            "for event {}".format(event))
    except ClientError as e:
        logger.error(e)
        logger.warning(
            "Dynamodb Client Error on GetDeviceLog for event {}".format(event))
    except RedisError as e:
        logger.error(e)
        logger.warning(
            "Redis Error on GetDeviceLog for event {}".format(event))
