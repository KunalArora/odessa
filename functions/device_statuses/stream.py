import logging
import sys
import json
from models.device_status import DeviceStatus
from models.cloud_device import CloudDevice
from models.email_device import EmailDevice
from models.service_oid import ServiceOid
from models.push_notification_subscription import PushNotificationSubscription
from models.accumulated_device_log import AccumulatedDeviceLog
from functions import helper
from helpers.time_functions import parse_time
from pymib.oid import OID
from pymib.mib import MIB

logger = logging.getLogger('device_logs_stream')
logger.setLevel(logging.INFO)


def save_cloud_device_status(event, context):
    logger.info(f'stream: device_logs, {event}')
    device_status = DeviceStatus()
    cloud_device = CloudDevice()
    accumulated_device_log = AccumulatedDeviceLog()

    for record in event['Records']:
        try:
            ddb_action = record['eventName']
            if ddb_action == 'INSERT':
                device_log_id = record['dynamodb']['Keys']['id']['S'].split('#')
                timestamp = record['dynamodb']['NewImage']['timestamp']['S']
                device_id = device_log_id[0]
                object_id = device_log_id[1]
                oid = OID(object_id)
                rawdata = record['dynamodb']['NewImage']['value']['S']
                data = oid.parse(rawdata)

                if oid.type == 'count_type':
                    data = {'count_type_id': record['dynamodb']['NewImage']['value']['S']}
                elif oid.type == 'counter':
                    data = {'counter_value': data}

                accumulated_device_log.read(device_id, object_id, timestamp)
                if not accumulated_device_log.is_existing():
                    accumulated_device_log.insert(device_id, object_id, timestamp, rawdata)
                else:
                    accumulated_log = accumulated_device_log.accumulated_log
                    latest_date = accumulated_log[-1]['timestamp'][0:10]
                    if latest_date == timestamp[0:10]:
                        accumulated_log[-1]['timestamp'] = timestamp
                        accumulated_log[-1]['value'] = rawdata
                    else:
                        accumulated_log.append({'value' : rawdata, 'timestamp' : timestamp})
                    accumulated_device_log.update(accumulated_log)

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


def save_email_device_status(event, context):
    logger.info(f'stream: email_logs, {event}')
    email_device = EmailDevice()

    for record in event['Records']:
        try:
            ddb_action = record['eventName']
            if ddb_action == 'INSERT':
                serial_number = record['dynamodb']['Keys']['serial_number']['S']
                email_device.read(serial_number)
                if not email_device.is_existing():
                    logger.warning(f'serial_number {serial_number} not subscribed to reporting')
                    return

                timestamp = record['dynamodb']['NewImage']['timestamp']['S']
                features = record['dynamodb']['NewImage']
                features.pop('timestamp')
                service_oids = ServiceOid().read(email_device.log_service_id)['oids']

                oids, missings = MIB.search_oid(list(features.keys()))
                if missings:
                    logger.warning(f'unknown features {missings} saved in email logs')

                processed_feature_names = []
                notify_data = []
                for object_id, feature_names in oids.items():
                    if(object_id not in service_oids or feature_names in processed_feature_names):
                        continue
                    else:
                        processed_feature_names.append(feature_names)

                    oid = OID(object_id)
                    if oid.type == 'count_type':
                        notify_data.append(
                            put_count_type_oid(email_device, oid, feature_names,
                                               timestamp, features)
                        )
                    else:
                        notify_data.append(
                            put_key_value_oid(email_device, object_id,
                                              feature_names, timestamp, features)
                        )

            else:
                logger.warning(f'Unexpected DB action {ddb_action} on table device_email_logs caught on stream')
        except:  # pragma: no cover
            logger.error(sys.exc_info())


def put_count_type_oid(email_device, oid, feature_names, timestamp, features):
    notify_data = {}
    for count_type_id, feature_name in oid.id_map.items():
        device_status = DeviceStatus()
        if feature_name not in feature_names:
            continue

        map_id = list(oid.id_map.keys()).index(count_type_id) + 1
        count_type_oid = '.'.join([oid.oid, str(map_id)])
        try:
            pair_oid = OID(count_type_oid).pair_oid
        except:
            short_oid = oid.oid[:oid.oid.rindex('.')]
            count_type_oid = '.'.join([short_oid, str(map_id)])
            pair_oid = OID(count_type_oid).pair_oid
        device_status.read(email_device.reporting_id, count_type_oid)
        if not device_status.is_existing():
            device_status.insert(
                email_device.reporting_id, count_type_oid,
                timestamp, {'count_type_id': {'value': count_type_id, 'timestamp': timestamp}}
            )
            notify_data[count_type_oid] = {'feature_name': 'count_type_id', 'value': count_type_id}
        elif(parse_time(device_status.timestamp) < parse_time(timestamp) and
             device_status.data['count_type_id']['value'] != count_type_id):
            device_status.update(timestamp, {'count_type_id': {'value': count_type_id, 'timestamp': timestamp}})
            notify_data[count_type_oid] = {'feature_name': 'count_type_id', 'value': count_type_id}

        pair_oid_status = DeviceStatus()
        pair_oid_status.read(email_device.reporting_id, pair_oid)
        if not pair_oid_status.is_existing():
            pair_oid_status.insert(
                email_device.reporting_id, pair_oid,
                timestamp, {'counter_value': {'value': features[feature_name]['S'], 'timestamp': timestamp}}
            )
            notify_data[pair_oid] = {'feature_name': 'counter_value', 'value': features[feature_name]['S']}
        elif(parse_time(pair_oid_status.timestamp) < parse_time(timestamp) and
             pair_oid_status.data['counter_value']['value'] != features[feature_name]['S']):
            pair_oid_status.update(timestamp, {'counter_value': {'value': features[feature_name]['S'], 'timestamp': timestamp}})
            notify_data[pair_oid] = {'feature_name': 'counter_value', 'value': features[feature_name]['S']}

    if notify_data:
        send_push_notification(email_device, notify_data, timestamp)


def put_key_value_oid(email_device, object_id, feature_names, timestamp, features):
    device_status = DeviceStatus()
    notify_data = []
    device_status.read(email_device.reporting_id, object_id)
    if not device_status.is_existing():
        insert_data = {}
        for feature_name in feature_names:
            insert_data[feature_name] = {'value': features[feature_name]['S'], 'timestamp': timestamp}
            notify_data.append({'feature_name': feature_name, 'value': features[feature_name]['S']})
        device_status.insert(email_device.reporting_id, object_id, timestamp, insert_data)
    elif parse_time(device_status.timestamp) < parse_time(timestamp):
        update_data = device_status.data
        updated = False
        for feature_name in feature_names:
            if(feature_name not in update_data or
               update_data[feature_name]['value'] != features[feature_name]['S']):
                updated = True
                update_data[feature_name] = {'value': features[feature_name]['S'], 'timestamp': timestamp}
                notify_data.append({'feature_name': feature_name, 'value': features[feature_name]['S']})
        if updated:
            device_status.update(timestamp, update_data)

    if notify_data:
        data = {}
        data[object_id] = notify_data
        send_push_notification(email_device, data, timestamp)


def send_push_notification(email_device, notify_data, timestamp):
    for object_id, data in notify_data.items():
        subscription = PushNotificationSubscription().read(email_device.log_service_id, object_id)
        if subscription.is_subscribed():
            payload = {
                'reporting_id': email_device.reporting_id,
                'object_id': subscription.object_id,
                'timestamp': timestamp,
                'data': data,
                'notify_url': subscription.notify_url
            }
            helper.invoke_async('send_push_notification', json.dumps(payload))
