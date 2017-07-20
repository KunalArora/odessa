from models.device_log import DeviceLog
from models.device_network_status import DeviceNetworkStatus
from functions.helper import *
from redis import RedisError

logger = logging.getLogger('device_notifications_stream')
logger.setLevel(logging.INFO)


def save_notify_logs_cache(event, context):
    device_log = DeviceLog()
    try:
        device_log.update_logs(event)
    except TypeError as e:
        logger.error(e)
        logger.warning(
            "JSON format error in the request for event {}".format(event))
    except RedisError as e:
        logger.error(e)
        logger.warning(
            "Redis Error on GetDeviceLog for event {}".format(event))


def save_notify_status_cache(event, context):
    device_network_status = DeviceNetworkStatus()
    try:
        device_network_status.update_status(event)
    except TypeError as e:
        logger.error(e)
        logger.warning(
            "JSON format error in the request for event {}".format(event))
    except RedisError as e:
        logger.error(e)
        logger.warning(
            "Redis Error on GetDeviceLog for event {}".format(event))
