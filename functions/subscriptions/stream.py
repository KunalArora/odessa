import logging
import sys
from models.device_subscription import DeviceSubscription

logger = logging.getLogger('subscriptions:stream')
logger.setLevel(logging.INFO)


def subscriptions(event, context):
    logger.info(f'stream: device_subscriptions, {event}')
    device_info = DeviceSubscription()
    for record in event['Records']:
        try:
            ddb_action = record['eventName']
            if ddb_action == 'INSERT':
                device_info.write_to_ec(
                    record['dynamodb']['Keys'], record['dynamodb']['NewImage'])
            elif ddb_action == 'MODIFY':
                device_info.update_ec(
                    record['dynamodb']['Keys'], record['dynamodb']['NewImage'])
            elif ddb_action == 'REMOVE':
                device_info.delete_from_ec(record['dynamodb']['Keys'])
        except:  # pragma: no cover
            logger.error(sys.exc_info())
