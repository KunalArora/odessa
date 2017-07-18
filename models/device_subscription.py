from boto3.dynamodb.conditions import Key
import datetime
import re
from models.base import Base
from models.service_oid import ServiceOid
from constants.device_response_codes import *
from constants.odessa_response_codes import *
from constants.boc_response_codes import NO_SUCH_OID

SUBSCRIBE_CODE_OFFSET = 1000
UNSUBSCRIBE_CODE_OFFSET = 2000
ERROR_OFFSET = 400

TO_SUBSCRIBE = 'subscribe'
TO_UNSUBSCRIBE = 'unsubscribe'


class DeviceSubscription(Base):
    def __init__(self):
        super().__init__()

    def read_for_subscribe(self, device_id, log_service_id):
        self.read(device_id, log_service_id, TO_SUBSCRIBE)

    def read_for_unsubscribe(self, device_id, log_service_id):
        self.read(device_id, log_service_id, TO_UNSUBSCRIBE)

    def read(self, device_id, log_service_id='?', read_type=None):
        subscription_list = []
        if (self.elasticache):
            device_keys = self.elasticache.keys(
                f'device_subscriptions:{device_id}#{log_service_id}:*')
            for ec_id in device_keys:
                ec_id = self.convert(ec_id)
                sub = self.convert(self.elasticache.hgetall(ec_id))
                subscription_list.append({
                    'id': device_id,
                    'log_service_id': re.match(
                        r'device_subscriptions\:(\w*-?)+#(\d+)\:',
                        ec_id).group(2),
                    'status': sub['status'],
                    'message': sub['message'],
                    'created_at': sub['created_at'],
                    'updated_at': sub['updated_at']})

        if len(subscription_list) == 0:
            table = self.dynamodb.Table('device_subscriptions')
            if read_type:
                ddb_res = table.query(
                    KeyConditionExpression=Key('id').eq(
                        f'{device_id}#{log_service_id}')
                )['Items']
            else:
                ddb_res = table.scan(
                    FilterExpression=Key('id').begins_with(device_id)
                )['Items']

            for sub in ddb_res:
                subscription_list.append({
                    'id': device_id,
                    'log_service_id': re.match(r'(\w*-?)+#(\d+)',
                                               sub['id']).group(2),
                    'status': sub['status'],
                    'message': sub['message'],
                    'created_at': sub['created_at'],
                    'updated_at': sub['updated_at']})

            if len(subscription_list) == 0:
                return None

        self.device_id = device_id

        if read_type == TO_SUBSCRIBE:
            self.get_subscribe_record(subscription_list)
        elif read_type == TO_UNSUBSCRIBE:
            self.get_unsubscribe_record(subscription_list)
        else:
            self.get_subscription_info(subscription_list)

    def get_subscribe_record(self, subscription_list):
        subscribed_list = []
        offline_list = []
        subscribing_list = []
        unsubscribing_list = []
        subscribe_error_list = []
        unsubscribe_error_list = []
        other_error_list = []

        for sub in subscription_list:
            self.status = int(sub['status'])
            if self.is_subscribed():
                subscribed_list.append(sub)
            elif self.is_offline():
                offline_list.append(sub)
            elif self.is_subscribing():
                subscribing_list.append(sub)
            elif self.is_unsubscribing():
                unsubscribing_list.append(sub)
            elif self.is_subscribe_error():
                subscribe_error_list.append(sub)
            elif self.is_unsubscribe_error():
                unsubscribe_error_list.append(sub)
            else:
                other_error_list.append(sub)

        if other_error_list:
            self.get_latest_record(other_error_list)
        elif subscribing_list or unsubscribing_list:
            self.get_latest_record(subscribing_list + unsubscribing_list)
        elif subscribe_error_list:
            self.get_latest_record(subscribe_error_list)
        elif unsubscribe_error_list:
            self.get_latest_record(unsubscribe_error_list)
        elif offline_list:
            self.get_latest_record(offline_list)
        else:
            self.get_latest_record(subscribed_list)

    def get_unsubscribe_record(self, subscription_list):
        subscribed_list = []
        offline_list = []
        subscribing_list = []
        unsubscribing_list = []
        subscribe_error_list = []
        unsubscribe_error_list = []
        other_error_list = []

        for sub in subscription_list:
            self.status = int(sub['status'])
            if self.is_subscribed():
                subscribed_list.append(sub)
            elif self.is_offline():
                offline_list.append(sub)
            elif self.is_subscribing():
                subscribing_list.append(sub)
            elif self.is_unsubscribing():
                unsubscribing_list.append(sub)
            elif self.is_subscribe_error():
                subscribe_error_list.append(sub)
            elif self.is_unsubscribe_error():
                unsubscribe_error_list.append(sub)
            else:
                other_error_list.append(sub)

        if other_error_list:
            self.get_latest_record(other_error_list)
        elif subscribing_list or unsubscribing_list:
            self.get_latest_record(subscribing_list + unsubscribing_list)
        elif unsubscribe_error_list:
            self.get_latest_record(unsubscribe_error_list)
        elif subscribe_error_list:
            self.get_latest_record(subscribe_error_list)
        elif offline_list:
            self.get_latest_record(offline_list)
        else:
            self.get_latest_record(subscribed_list)

    def get_subscription_info(self, subscription_list):
        subscribed_list = []
        offline_list = []
        others_list = []

        for sub in subscription_list:
            self.status = int(sub['status'])
            if self.is_subscribed():
                subscribed_list.append(sub)
            elif self.is_offline():
                offline_list.append(sub)
            else:
                others_list.append(sub)

        if others_list:
            self.get_latest_record(others_list)
        elif offline_list:
            self.get_latest_record(offline_list)
        else:
            self.get_latest_record(subscribed_list)

    def insert(self, device_id, log_service_id, error_code):
        self.device_id = device_id
        self.log_service_id = log_service_id
        self.status = error_code
        self.message = device_error_message(error_code)
        self.created_at = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        self.updated_at = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        oid_list = ServiceOid().read(log_service_id)['oids']
        table = self.dynamodb.Table('device_subscriptions')
        with table.batch_writer() as batch:
            for oid in oid_list:
                batch.put_item(
                        Item={
                            'id': f'{self.device_id}#{self.log_service_id}',
                            'oid': oid,
                            'status': self.status,
                            'message': self.message,
                            'created_at': self.created_at,
                            'updated_at': self.updated_at
                            })

    def write_to_ec(self, keys, image):
        ec_id = self.format_key(keys)
        ec_value = self.format_value(image)
        self.elasticache.hmset(f'device_subscriptions:{ec_id}', ec_value)

    def update(self, error_code, message=None):
        oid_list = self.get_subscribed_oids()

        table = self.dynamodb.Table('device_subscriptions')
        self.status = error_code
        if message:
            self.message = message
        else:
            self.message = device_error_message(self.status)
        for oid in oid_list:
            table.update_item(
                Key={
                    'id': f'{self.device_id}#{self.log_service_id}',
                    'oid': oid['object_id']
                },
                ExpressionAttributeNames={
                    '#s': 'status'
                },
                UpdateExpression="set #s = :s, message = :m, updated_at = :u",
                ExpressionAttributeValues={
                    ':s': self.status,
                    ':m': self.message,
                    ':u': datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
                    }
            )

    def update_as_subscribe_error(self, error, message):
        self.update(error + SUBSCRIBE_CODE_OFFSET, message)

    def update_as_unsubscribe_error(self, error, message):
        self.update(error + UNSUBSCRIBE_CODE_OFFSET, message)

    def update_ec(self, keys, image):
        self.write_to_ec(keys, image)

    def delete(self):
        oid_list = ServiceOid().read(self.log_service_id)['oids']
        table = self.dynamodb.Table('device_subscriptions')
        with table.batch_writer() as batch:
            for oid in oid_list:
                batch.delete_item(
                        Key={
                            'id': f'{self.device_id}#{self.log_service_id}',
                            'oid': oid
                        }
                )

    def delete_unsupported_oids(self, boc_response):
        updated_res = []
        if('subscribe' in boc_response and not
           len(boc_response['subscribe']) == 0):
            table = self.dynamodb.Table('device_subscriptions')
            with table.batch_writer() as batch:
                for subscription in boc_response['subscribe']:
                    if int(subscription['error_code']) == NO_SUCH_OID:
                        batch.delete_item(Key={
                            'id': f'{self.device_id}#{self.log_service_id}',
                            'oid': subscription['object_id']})
                    else:
                        updated_res.append(subscription)
            boc_response['subscribe'] = updated_res
        return boc_response

    def delete_from_ec(self, keys):
        ec_id = self.format_key(keys)
        self.elasticache.delete(f'device_subscriptions:{ec_id}')

    def get_message(self):
        return self.message

    def get_status(self):
        return self.status

    def get_log_service_id(self):
        return self.log_service_id

    def get_subscribed_oids(self):
        table = self.dynamodb.Table('device_subscriptions')
        ddb_res = table.query(
            KeyConditionExpression=Key('id').eq(
                f'{self.device_id}#{self.log_service_id}')
        )['Items']

        oid_list = []
        for subscription in ddb_res:
            oid_list.append({'object_id': subscription['oid']})

        return oid_list

    def is_existing(self):
        return hasattr(self, 'device_id')

    def is_offline(self):
        return self.status == SUBSCRIBED_OFFLINE

    def is_not_found(self):
        return self.status == DEVICE_NOT_FOUND

    def is_subscribed(self):
        return self.status == SUBSCRIBED

    def is_subscribing(self):
        return self.status == SUBSCRIBE_ACCEPTED

    def is_unsubscribing(self):
        return self.status == UNSUBSCRIBE_ACCEPTED

    def is_subscribe_error(self):
        return ((self.status // SUBSCRIBE_CODE_OFFSET == 1) and
                (self.status - SUBSCRIBE_CODE_OFFSET) > ERROR_OFFSET)

    def is_unsubscribe_error(self):
        return ((self.status // UNSUBSCRIBE_CODE_OFFSET == 1) and
                (self.status - UNSUBSCRIBE_CODE_OFFSET) > ERROR_OFFSET)

    def format_key(self, key):
        return f'{key["id"]["S"]}:{key["oid"]["S"]}'

    def format_value(self, value):
        return {
            'status': int(value['status']['N']),
            'message': value['message']['S'],
            'created_at': value['created_at']['S'],
            'updated_at': value['updated_at']['S']
        }

    def get_latest_record(self, subscription_list):
        device = max(subscription_list, key=lambda x:
                     datetime.datetime.strptime(x['updated_at'],
                                                "%Y-%m-%dT%H:%M:%S"))
        self.log_service_id = device['log_service_id']
        self.status = int(device['status'])
        self.message = device['message']
        self.created_at = device['created_at']
        self.updated_at = device['updated_at']


def device_error_message(error_code):
    error_map = {
        NOT_SUBSCRIBED: 'Not Subscribed',
        DEVICE_NOT_FOUND: 'Device not found',
        SUBSCRIBED: 'Subscribed',
        UNSUBSCRIBED: 'Unsubscribed',
        SUBSCRIBED_OFFLINE: 'Subscribed (Device Offline)',
        SUBSCRIBE_ACCEPTED: 'Subscribe accepted',
        UNSUBSCRIBE_ACCEPTED: 'Unsubscribe accepted',
        SUBSCRIBE_COMMUNICATION_ERROR: 'Subscribe communication error',
        UNSUBSCRIBE_COMMUNICATION_ERROR: 'Unsubscribe communication error',
        SUBSCRIBE_BOC_RESPONSE_ERROR: 'BOC subscribe response is invalid',
        UNSUBSCRIBE_BOC_RESPONSE_ERROR: 'BOC unsubscribe response is invalid',
        SUBSCRIBE_EXCLUSIVE_CONTROL_ERROR_WITH_OTHER_SUBS:
            'Subscribe exclusive control error (with other subscribing)',
        SUBSCRIBE_EXCLUSIVE_CONTROL_ERROR_WITH_OTHER_UNSUBS:
            'Subscribe exclusive control error (with other unsubscribing)',
        UNSUBSCRIBE_EXCLUSIVE_CONTROL_ERROR_WITH_OTHER_SUBS:
            'Unsubscribe exclusive control error (with other subscribing)',
        UNSUBSCRIBE_EXCLUSIVE_CONTROL_ERROR_WITH_OTHER_UNSUBS:
            'Unsubscribe exclusive control error (with other unsubscribing)',
        UNKNOWN: 'Unknown error'
    }
    return error_map[error_code]
