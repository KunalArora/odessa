from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError
import datetime
import re
from models.base import Base
from models.service_oid import ServiceOid
from constants.device_response_codes import *
from constants.odessa_response_codes import *
from constants.boc_response_codes import NO_SUCH_OID
from constants.boc_response_codes import OBJECT_SUBSCRIPTION_NOT_FOUND

SUBSCRIBE_CODE_OFFSET = 1000
UNSUBSCRIBE_CODE_OFFSET = 2000
ERROR_OFFSET = 400

TO_SUBSCRIBE = 'subscribe'
TO_UNSUBSCRIBE = 'unsubscribe'


class DeviceSubscription(Base):
    def __init__(self):
        super().__init__()

    def read(self, device_id, log_service_id):
        subscription = self.get_record(device_id, log_service_id)

        # Ignore unsubscribed devices
        if (not subscription or int(subscription['status']) == UNSUBSCRIBED):
            return None

        self.device_id = device_id
        self.log_service_id = log_service_id
        self.status = int(subscription['status'])
        self.message = subscription['message']

        if 'latest_async_id' in subscription:
            self.latest_async_id = subscription['latest_async_id']

        return self

    def get_record(self, device_id, log_service_id):
        subscription = {}

        if (self.elasticache):
            ec_id = self.elasticache.keys(
                f'device_subscriptions:{device_id}#{log_service_id}')
            if len(ec_id) == 1:
                ec_id = self.convert(ec_id[0])
                sub = self.convert(self.elasticache.hgetall(ec_id))
                subscription = {
                    'id': device_id,
                    'log_service_id': log_service_id,
                    'status': sub['status'],
                    'message': sub['message'],
                    'created_at': sub['created_at'],
                    'updated_at': sub['updated_at']}
                if 'latest_async_id' in sub:
                    subscription['latest_async_id'] = sub['latest_async_id']

        if not subscription:
            table = self.dynamodb.Table('device_subscriptions')
            ddb_res = table.get_item(Key={
                    'id': f'{device_id}#{log_service_id}'
                })

            if 'Item' in ddb_res:
                    subscription = {
                        'id': device_id,
                        'log_service_id': log_service_id,
                        'status': ddb_res['Item']['status'],
                        'message': ddb_res['Item']['message'],
                        'created_at': ddb_res['Item']['created_at'],
                        'updated_at': ddb_res['Item']['updated_at']}
                    if 'latest_async_id' in ddb_res['Item']:
                        subscription['latest_async_id'] = ddb_res['Item']['latest_async_id']
            else:
                return None

        return subscription

    def read_for_history_logs(self, device_id, log_service_id):
        subscription = self.get_record(device_id, log_service_id)

        # No record = Device not found
        if not subscription:
            return None

        self.device_id = device_id
        self.log_service_id = log_service_id

    def insert(self, device_id, log_service_id, error_code):
        self.device_id = device_id
        self.log_service_id = log_service_id
        self.status = error_code
        self.message = device_error_message(error_code)
        self.created_at = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        self.updated_at = self.created_at

        oid_list = ServiceOid().read(log_service_id)['oids']
        if len(oid_list) <= 0:
            return None

        table = self.dynamodb.Table('device_subscriptions')
        try:
            table.put_item(
                ConditionExpression='attribute_not_exists(id)',
                Item={
                    'id': f'{self.device_id}#{self.log_service_id}',
                    'status': self.status,
                    'message': self.message,
                    'created_at': self.created_at,
                    'updated_at': self.updated_at
                })
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                self.update(error_code)

    def write_to_ec(self, keys, image):
        ec_id = self.format_key(keys)
        ec_value = self.format_value(image)
        self.elasticache.hmset(f'device_subscriptions:{ec_id}', ec_value)

    def update(self, error_code, message=None):
        table = self.dynamodb.Table('device_subscriptions')
        self.status = error_code
        if message:
            self.message = message
        else:
            self.message = device_error_message(self.status)

        table.update_item(
            Key={'id': f'{self.device_id}#{self.log_service_id}'},
            ExpressionAttributeNames={'#s': 'status'},
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
        self.update(UNSUBSCRIBED)

    # Processed when an online device is subscribed
    def delete_unsupported_oids(self, boc_response, async_id):
        updated_res = []
        oids = []

        if('subscribe' in boc_response and not
           len(boc_response['subscribe']) == 0):
            table = self.dynamodb.Table('device_subscriptions')
            for subscription in boc_response['subscribe']:
                if int(subscription['error_code']) != NO_SUCH_OID:
                    updated_res.append(subscription)
                    oids.append({
                        'oid': subscription['object_id'],
                        'error_code': subscription['error_code'],
                        'messsage': subscription['message']
                    })

            table.update_item(
                Key={'id': f'{self.device_id}#{self.log_service_id}'},
                ExpressionAttributeNames={'#s': 'status'},
                UpdateExpression="set oids = :o, #s = :s, message = :m, updated_at = :u, latest_async_id = :r",
                ExpressionAttributeValues={
                    ':o': oids,
                    ':s': SUBSCRIBED,
                    ':m': device_error_message(SUBSCRIBED),
                    ':u': datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
                    ':r': async_id
                    })

            boc_response['subscribe'] = updated_res
        return boc_response

    # Processed when an offline device is subscribed and becomes online
    def delete_offline_unsupported_oids(self, boc_response):
        updated_res = []
        oids = []

        if('notifications' in boc_response and not
           len(boc_response['notifications']) == 0):
            table = self.dynamodb.Table('device_subscriptions')
            for subscription in boc_response['notifications']:
                if int(subscription['error_code']) != OBJECT_SUBSCRIPTION_NOT_FOUND:
                    updated_res.append(subscription)
                    oids.append({
                        'oid': subscription['object_id'],
                        'error_code': subscription['error_code']
                    })
            table.update_item(
                Key={'id': f'{self.device_id}#{self.log_service_id}'},
                ExpressionAttributeNames={'#s': 'status'},
                UpdateExpression="set oids = :o, #s = :s, message = :m, updated_at = :u",
                ExpressionAttributeValues={
                    ':o': oids,
                    ':s': SUBSCRIBED,
                    ':m': device_error_message(SUBSCRIBED),
                    ':u': datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
                    })

            boc_response['notifications'] = updated_res
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
        ddb_res = table.get_item(Key={
            'id': f'{self.device_id}#{self.log_service_id}'
        })

        if 'Item' in ddb_res and 'oids' in ddb_res['Item']:
            oid_list = []
            for subscription in ddb_res['Item']['oids']:
                oid_list.append(subscription['oid'])

            return oid_list
        else:  # subscribed but device_offline
            return ServiceOid().read(self.log_service_id)['oids']

    def has_oids_field(self):
        table = self.dynamodb.Table('device_subscriptions')
        ddb_res = table.get_item(Key={
            'id': f'{self.device_id}#{self.log_service_id}'
        })
        if ('Item' in ddb_res and 'oids' in ddb_res['Item']
            and ddb_res['Item']['oids']):
            return True
        else:
            return False

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
        return key["id"]["S"]

    def format_value(self, value):
        oids = []
        for oid in value['oids']:
            oids.append({
                'oid': oid['oid']['S'],
                'status': int(oid['status']['N']),
                'message': oid['message']['S']
            })
        return {
            'oids': oids,
            'status': int(value['status']['N']),
            'message': value['message']['S'],
            'created_at': value['created_at']['S'],
            'updated_at': value['updated_at']['S']
        }

    def get_device_status(self, device_id, service_id):
        table = self.dynamodb.Table('device_subscriptions')
        status = table.query(
            KeyConditionExpression=Key('id').eq(device_id + '#' + service_id),
            FilterExpression=Attr('status').eq(1200) | Attr('status').eq(1201)
        )
        return status['Items']

    def verify_subscribe(self, device_id, service_id):
        table = self.dynamodb.Table('device_subscriptions')
        status = table.query(
            KeyConditionExpression=Key('id').eq(device_id + '#' + service_id)
        )
        if status['Items'] and status['Items'][0]['status'] == 1200:
            return True
        else:
            return False

def device_error_message(error_code):
    error_map = {
        NOT_SUBSCRIBED: 'Not subscribed',
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
