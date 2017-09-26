import json
import traceback
import logging
from models.service_oid import ServiceOid
from models.device_subscription import DeviceSubscription

logger = logging.getLogger('device_events')
logger.setLevel(logging.INFO)


def handle_device_events(event, context):
    logger.info(f'request: {event}')

    try:
        message = json.loads(event['Records'][0]['Sns']['Message'])
        if not message:
            return True

        log_service_ids = ServiceOid.ids()
        if not log_service_ids:
            return True

        if 'delete' == message['event']:
            for i in log_service_ids:
                ds = DeviceSubscription().read(message['device_id'], i)
                if ds:
                    ds.delete()
        else:
            logger.warn(f"Unkonw device envent type: {message['event']}")

        return True
    except (ValueError, TypeError):
        logger.warn(traceback.format_exc())
        return True

