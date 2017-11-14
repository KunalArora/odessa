import logging
import sys
import json
from models.device_status import DeviceStatus
from models.cloud_device import CloudDevice
from models.push_notification_subscription import PushNotificationSubscription
from functions import helper
from helpers.time_functions import parse_time
from pymib.oid import OID

logger = logging.getLogger('device_logs_stream')
logger.setLevel(logging.INFO)


def save_cloud_device_status(event, context):
    logger.info(f'stream: device_logs, {event}')
    device_status = DeviceStatus()
    cloud_device = CloudDevice()

    for record in event['Records']:
        try:
            ddb_action = record['eventName']
            if ddb_action == 'INSERT':
                device_log_id = record['dynamodb']['Keys']['id']['S'].split('#')
                timestamp = record['dynamodb']['NewImage']['timestamp']['S']
                device_id = device_log_id[0]
                object_id = device_log_id[1]
                data = OID(object_id).parse(record['dynamodb']['NewImage']['value']['S'])
                cloud_device.read(device_id)
                if not cloud_device.is_existing():
                    logger.warning(f'device_id {device_id} not subscribed to reporting')
                    return

                device_status.read(cloud_device.reporting_id, object_id)
                notify_data = []
                if not device_status.is_existing():
                    insert_data = {}
                    for key, value in data.items():
                        insert_data[key] = {'value': value, 'timestamp': timestamp}
                        notify_data.append({'feature_name': key, 'value': value})
                    device_status.insert(cloud_device.reporting_id, object_id, timestamp, insert_data)
                elif parse_time(device_status.timestamp) < parse_time(timestamp):
                    update_data = device_status.data
                    updated = False
                    for key, value in data.items():
                        if update_data[key]['value'] != value:
                            updated = True
                            update_data[key]['value'] = value
                            update_data[key]['timestamp'] = timestamp
                            notify_data.append({'feature_name': key, 'value': value})
                    if updated:
                        device_status.update(timestamp, update_data)

                subscription = PushNotificationSubscription().read(cloud_device.log_service_id, object_id)
                if subscription.is_subscribed() and notify_data:
                    payload = {
                        'reporting_id': cloud_device.reporting_id,
                        'object_id': subscription.object_id,
                        'timestamp': timestamp,
                        'data': notify_data,
                        'notify_url': subscription.notify_url
                    }
                    helper.invoke_async('send_push_notification', json.dumps(payload))
            else:
                logger.warning(f'Unexpected DB action {ddb_action} on table device_logs caught on stream')
        except:  # pragma: no cover
            logger.error(sys.exc_info())
