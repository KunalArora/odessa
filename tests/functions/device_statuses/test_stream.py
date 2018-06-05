import unittest
import json
import logging
from os import path
from unittest.mock import patch
from functions.device_statuses import stream
from tests.functions import test_helper
from boto3.dynamodb.conditions import Key


class StreamTestCase(unittest.TestCase):
    def setUp(self):
        test_helper.set_env_var(self)
        self.path = path.dirname(__file__)
        test_helper.seed_ddb_device_statuses(self)
        logging.getLogger('subscriptions:stream').setLevel(100)

    def tearDown(self):
        test_helper.clear_db(self)
        test_helper.create_table(self)

    def test_insert_accumulated_device_log(self):
        with open(
                f'{self.path}/../../data/device_statuses/stream/cloud/insert_accumulated_device_log.json'
                ) as data_file:
            input = json.load(data_file)
        id = input['Records'][0]['dynamodb']['Keys']['id']['S']
        timestamp = input['Records'][0]['dynamodb']['NewImage']['timestamp']['S']
        self.assertIsNone(get_accumulated_device_log(self, id, timestamp))
        stream.save_cloud_device_status(input, 'dummy')
        accumulated_device_log = get_accumulated_device_log(self, id, timestamp)
        self.assertTrue(accumulated_device_log)
        self.assertEqual(len(accumulated_device_log['accumulated_log']), 1)
        self.assertEqual(accumulated_device_log['id'], id)
        self.assertEqual(accumulated_device_log['year_month'], timestamp[0:7])
        self.assertTrue('value' in accumulated_device_log['accumulated_log'][0])
        self.assertTrue('timestamp' in accumulated_device_log['accumulated_log'][0])
        self.assertEqual(accumulated_device_log['accumulated_log'][0]['value'], input['Records'][0]['dynamodb']['NewImage']['value']['S'])
        self.assertEqual(accumulated_device_log['accumulated_log'][0]['timestamp'], timestamp)
    
    def test_update_accumulated_device_log(self):    
        with open(
                f'{self.path}/../../data/device_statuses/stream/cloud/update_accumulated_device_log.json'
                ) as data_file:
            input = json.load(data_file)

        # with the same date case
        id = input[0]['Records'][0]['dynamodb']['Keys']['id']['S']
        timestamp = input[0]['Records'][0]['dynamodb']['NewImage']['timestamp']['S']
        self.assertTrue(get_accumulated_device_log(self, id, timestamp))
        stream.save_cloud_device_status(input[0], 'dummy')
        accumulated_device_log = get_accumulated_device_log(self, id, timestamp)
        self.assertTrue(accumulated_device_log)
        self.assertEqual(len(accumulated_device_log['accumulated_log']), 1)
        self.assertEqual(accumulated_device_log['id'], id)
        self.assertEqual(accumulated_device_log['year_month'], timestamp[0:7])
        self.assertTrue('value' in accumulated_device_log['accumulated_log'][0])
        self.assertTrue('timestamp' in accumulated_device_log['accumulated_log'][0])
        self.assertEqual(accumulated_device_log['accumulated_log'][0]['value'], input[0]['Records'][0]['dynamodb']['NewImage']['value']['S'])
        self.assertEqual(accumulated_device_log['accumulated_log'][0]['timestamp'], timestamp)

        # with not the same date case
        id = input[1]['Records'][0]['dynamodb']['Keys']['id']['S']
        timestamp = input[1]['Records'][0]['dynamodb']['NewImage']['timestamp']['S']
        self.assertTrue(get_accumulated_device_log(self, id, timestamp))
        stream.save_cloud_device_status(input[1], 'dummy')
        accumulated_device_log = get_accumulated_device_log(self, id, timestamp)
        self.assertTrue(accumulated_device_log)
        self.assertEqual(len(accumulated_device_log['accumulated_log']), 2)
        self.assertEqual(accumulated_device_log['id'], id)
        self.assertEqual(accumulated_device_log['year_month'], timestamp[0:7])
        self.assertTrue('value' in accumulated_device_log['accumulated_log'][1])
        self.assertTrue('timestamp' in accumulated_device_log['accumulated_log'][1])
        self.assertEqual(accumulated_device_log['accumulated_log'][1]['value'], input[1]['Records'][0]['dynamodb']['NewImage']['value']['S'])
        self.assertEqual(accumulated_device_log['accumulated_log'][1]['timestamp'], timestamp)

    @patch('logging.info')
    def test_newly_subscribed_cloud_device(self, mock):
        with open(
                f'{self.path}/../../data/device_statuses/stream/cloud/new_status.json'
                ) as data_file:
            input = json.load(data_file)
        id = input['Records'][0]['dynamodb']['Keys']['id']['S'].split('#')
        self.assertIsNone(get_cloud_device_status(self, id[0], id[1]))
        stream.save_cloud_device_status(input, 'dummy')
        device_status = get_cloud_device_status(self, id[0], id[1])
        self.assertTrue(device_status)
        self.assertEqual(len(device_status['data']), 12)
        self.assertEqual(device_status['timestamp'], input['Records'][0]['dynamodb']['NewImage']['timestamp']['S'])
        for feature, data in device_status['data'].items():
            self.assertTrue('value' in data)
            self.assertTrue('timestamp' in data)
            self.assertEqual(data['timestamp'], input['Records'][0]['dynamodb']['NewImage']['timestamp']['S'])
        mock.assert_called_with('invoking lambda function:send_push_notification with payload: {"reporting_id": "eeeeeeee-eeee-eeee-eeee-eeeeeeee0000", "object_id": "1.3.6.1.4.1.2435.2.3.9.4.2.1.5.5.8.0", "timestamp": "2017-02-01T12:23:01", "data": [{"feature_name": "Drum_Unit", "value": "1"}, {"feature_name": "Drum_Count", "value": "11"}, {"feature_name": "TonerInk_Black", "value": "1"}, {"feature_name": "TonerInk_LifeBlack", "value": "9900"}, {"feature_name": "LaserUnit_Status", "value": "1"}, {"feature_name": "FuserUnit_Status", "value": "1"}, {"feature_name": "PFKitMP_Status", "value": "1"}, {"feature_name": "PFKit1_Status", "value": "1"}, {"feature_name": "PFKit2_Status", "value": "1"}, {"feature_name": "PFKit3_Status", "value": "1"}, {"feature_name": "PFKit4_Status", "value": "1"}, {"feature_name": "PFKit5_Status", "value": "1"}], "notify_url": "http://dummy.com"}')

    def test_updated_cloud_device_status(self):
        with open(
                f'{self.path}/../../data/device_statuses/stream/cloud/updated_status.json'
                ) as data_file:
            input = json.load(data_file)
        id = input['Records'][0]['dynamodb']['Keys']['id']['S'].split('#')
        before = get_cloud_device_status(self, id[0], id[1])
        self.assertTrue(before)
        stream.save_cloud_device_status(input, 'dummy')
        after = get_cloud_device_status(self, id[0], id[1])
        self.assertEqual(after['timestamp'], input['Records'][0]['dynamodb']['NewImage']['timestamp']['S'])
        self.assertNotEqual(before['timestamp'], after['timestamp'])
        for feature, data in after['data'].items():
            if feature == 'TonerInk_LifeBlack':
                self.assertNotEqual(data['timestamp'], before['data'][feature]['timestamp'])
                self.assertNotEqual(data['value'], before['data'][feature]['value'])
            else:
                self.assertEqual(data['timestamp'], before['data'][feature]['timestamp'])
                self.assertEqual(data['value'], before['data'][feature]['value'])

    def test_unchanged_cloud_device_status(self):
        with open(
                f'{self.path}/../../data/device_statuses/stream/cloud/unchanged_status.json'
                ) as data_file:
            input = json.load(data_file)
        id = input['Records'][0]['dynamodb']['Keys']['id']['S'].split('#')
        before = get_cloud_device_status(self, id[0], id[1])
        self.assertTrue(before)
        stream.save_cloud_device_status(input, 'dummy')
        after = get_cloud_device_status(self, id[0], id[1])
        self.assertEqual(before, after)

    def test_outdated_cloud_device_status(self):
        with open(
                f'{self.path}/../../data/device_statuses/stream/cloud/outdated_status.json'
                ) as data_file:
            input = json.load(data_file)
        id = input['Records'][0]['dynamodb']['Keys']['id']['S'].split('#')
        before = get_cloud_device_status(self, id[0], id[1])
        self.assertTrue(before)
        stream.save_cloud_device_status(input, 'dummy')
        after = get_cloud_device_status(self, id[0], id[1])
        self.assertEqual(before, after)

    @patch('logging.info')
    def test_unsubscribed_cloud_device_status(self, mock):
        with open(
                f'{self.path}/../../data/device_statuses/stream/cloud/unsubscribed_status.json'
                ) as data_file:
            input = json.load(data_file)
        id = input['Records'][0]['dynamodb']['Keys']['id']['S'].split('#')
        self.assertIsNone(get_cloud_device_status(self, id[0], id[1]))
        stream.save_cloud_device_status(input, 'dummy')
        device_status = get_cloud_device_status(self, id[0], id[1])
        self.assertTrue(device_status)
        mock.assert_not_called()

    @patch('logging.info')
    def test_count_type_cloud_device(self, mock):
        with open(
                f'{self.path}/../../data/device_statuses/stream/cloud/count_type_new_status.json'
                ) as data_file:
            input = json.load(data_file)
        id = input['Records'][0]['dynamodb']['Keys']['id']['S'].split('#')
        self.assertIsNone(get_cloud_device_status(self, id[0], id[1]))
        stream.save_cloud_device_status(input, 'dummy')
        device_status = get_cloud_device_status(self, id[0], id[1])
        self.assertTrue(device_status)
        self.assertEqual(len(device_status['data']), 1)
        self.assertEqual(device_status['timestamp'], input['Records'][0]['dynamodb']['NewImage']['timestamp']['S'])
        for feature, data in device_status['data'].items():
            self.assertEqual(feature, 'count_type_id')
            self.assertTrue('value' in data)
            self.assertTrue('timestamp' in data)
            self.assertEqual(data['timestamp'], input['Records'][0]['dynamodb']['NewImage']['timestamp']['S'])

        with open(
                f'{self.path}/../../data/device_statuses/stream/cloud/counter_new_status.json'
                ) as data_file:
            input = json.load(data_file)
        id = input['Records'][0]['dynamodb']['Keys']['id']['S'].split('#')
        self.assertIsNone(get_cloud_device_status(self, id[0], id[1]))
        stream.save_cloud_device_status(input, 'dummy')
        device_status = get_cloud_device_status(self, id[0], id[1])
        self.assertTrue(device_status)
        self.assertEqual(len(device_status['data']), 1)
        self.assertEqual(device_status['timestamp'], input['Records'][0]['dynamodb']['NewImage']['timestamp']['S'])
        for feature, data in device_status['data'].items():
            self.assertEqual(feature, 'counter_value')
            self.assertTrue('value' in data)
            self.assertTrue('timestamp' in data)
            self.assertEqual(data['timestamp'], input['Records'][0]['dynamodb']['NewImage']['timestamp']['S'])

        mock.assert_not_called()

    @patch('logging.info')
    def test_newly_subscribed_email_device(self, mock):
        with open(
                f'{self.path}/../../data/device_statuses/stream/email/new_status.json'
                ) as data_file:
                input = json.load(data_file)
        timestamp = (input['Records'][0]['dynamodb']['NewImage']['timestamp']['S'])
        serial_number = input['Records'][0]['dynamodb']['Keys']['serial_number']['S']
        self.assertIsNone(get_email_device_status(self, serial_number))
        stream.save_email_device_status(input, 'dummy')
        device_statuses = get_email_device_status(self, serial_number)
        self.assertEqual(len(device_statuses), 3)
        for device_status in device_statuses:
            self.assertEqual(device_status['timestamp'], timestamp)
            for feature, data in device_status['data'].items():
                self.assertTrue('value' in data)
                self.assertTrue('timestamp' in data)
                self.assertEqual(data['timestamp'], timestamp)
                self.assertEqual(data['value'], input['Records'][0]['dynamodb']['NewImage'][feature]['S'])

        mock.assert_called_with('invoking lambda function:send_push_notification with payload: {"reporting_id": "eeeeeeee-eeee-eeee-eeee-eeeeeeee0005", "object_id": "1.3.6.1.4.1.2435.2.3.9.4.2.1.5.5.8.0", "timestamp": "2017-02-01T12:23:01", "data": [{"feature_name": "Drum_Count", "value": "2"}, {"feature_name": "TonerInk_Black", "value": "40"}, {"feature_name": "TonerInk_Cyan", "value": "50"}], "notify_url": "http://dummy.com"}')

    @patch('logging.info')
    def test_updated_email_device_status(self, mock):
        with open(
                f'{self.path}/../../data/device_statuses/stream/email/updated_status.json'
                ) as data_file:
            input = json.load(data_file)
        timestamp = (input['Records'][0]['dynamodb']['NewImage']['timestamp']['S'])
        serial_number = input['Records'][0]['dynamodb']['Keys']['serial_number']['S']
        before = get_email_device_status(self, serial_number)
        self.assertTrue(before)
        stream.save_email_device_status(input, 'dummy')
        device_statuses = get_email_device_status(self, serial_number)
        for device_status in device_statuses:
            for feature, data in device_status['data'].items():
                if feature in input['Records'][0]['dynamodb']['NewImage']:
                    self.assertEqual(device_status['timestamp'], timestamp)
                    self.assertEqual(data['timestamp'], timestamp)
                    self.assertEqual(data['value'], input['Records'][0]['dynamodb']['NewImage'][feature]['S'])
        mock.assert_called_with('invoking lambda function:send_push_notification with payload: {"reporting_id": "eeeeeeee-eeee-eeee-eeee-eeeeeeee0006", "object_id": "1.3.6.1.4.1.2435.2.3.9.4.2.1.5.5.8.0", "timestamp": "2017-02-05T12:23:01", "data": [{"feature_name": "Drum_Count", "value": "2"}, {"feature_name": "TonerInk_Black", "value": "40"}, {"feature_name": "TonerInk_Cyan", "value": "50"}], "notify_url": "http://dummy.com"}')

    @patch('logging.info')
    def test_unchanged_email_device_status(self, mock):
        with open(
                f'{self.path}/../../data/device_statuses/stream/email/unchanged_status.json'
                ) as data_file:
            input = json.load(data_file)
        serial_number = input['Records'][0]['dynamodb']['Keys']['serial_number']['S']
        before = get_email_device_status(self, serial_number)
        self.assertTrue(before)
        stream.save_email_device_status(input, 'dummy')
        after = get_email_device_status(self, serial_number)
        self.assertEqual(before, after)
        mock.assert_not_called()

    @patch('logging.info')
    def test_outdated_email_device_status(self, mock):
        with open(
                f'{self.path}/../../data/device_statuses/stream/email/outdated_status.json'
                ) as data_file:
            input = json.load(data_file)
        serial_number = input['Records'][0]['dynamodb']['Keys']['serial_number']['S']
        before = get_email_device_status(self, serial_number)
        self.assertTrue(before)
        stream.save_email_device_status(input, 'dummy')
        after = get_email_device_status(self, serial_number)
        self.assertEqual(before, after)
        mock.assert_not_called()

    def test_email_unknown_features(self):
        with open(
                f'{self.path}/../../data/device_statuses/stream/email/unknown_features.json'
                ) as data_file:
            input = json.load(data_file)
        serial_number = input['Records'][0]['dynamodb']['Keys']['serial_number']['S']
        before = get_email_device_status(self, serial_number)
        self.assertTrue(before)
        stream.save_email_device_status(input, 'dummy')
        after = get_email_device_status(self, serial_number)
        self.assertEqual(before, after)

    def test_not_subscribed_feature(self):
        with open(
                f'{self.path}/../../data/device_statuses/stream/email/not_subscribed_feature.json'
                ) as data_file:
            input = json.load(data_file)
        serial_number = input['Records'][0]['dynamodb']['Keys']['serial_number']['S']
        before = get_email_device_status(self, serial_number)
        self.assertTrue(before)
        stream.save_email_device_status(input, 'dummy')
        after = get_email_device_status(self, serial_number)
        self.assertEqual(before, after)

    def test_full_email_device_log(self):
        with open(
                f'{self.path}/../../data/device_statuses/stream/email/full_new_status.json'
                ) as data_file:
                input = json.load(data_file)
        timestamp = (input['Records'][0]['dynamodb']['NewImage']['timestamp']['S'])
        serial_number = input['Records'][0]['dynamodb']['Keys']['serial_number']['S']
        self.assertIsNone(get_email_device_status(self, serial_number))
        stream.save_email_device_status(input, 'dummy')
        device_statuses = get_email_device_status(self, serial_number)
        self.assertEqual(len(device_statuses), 9)
        for device_status in device_statuses:
            self.assertEqual(device_status['timestamp'], timestamp)
            for feature, data in device_status['data'].items():
                self.assertTrue('value' in data)
                self.assertTrue('timestamp' in data)
                self.assertEqual(data['timestamp'], timestamp)
                self.assertEqual(data['value'], input['Records'][0]['dynamodb']['NewImage'][feature]['S'])

    @patch('logging.info')
    def test_new_email_count_type_status(self, mock):
        with open(
                f'{self.path}/../../data/device_statuses/stream/email/count_type_new_status.json'
                ) as data_file:
                input = json.load(data_file)
        timestamp = (input['Records'][0]['dynamodb']['NewImage']['timestamp']['S'])
        serial_number = input['Records'][0]['dynamodb']['Keys']['serial_number']['S']
        self.assertIsNone(get_email_device_status(self, serial_number))
        stream.save_email_device_status(input, 'dummy')
        device_statuses = get_email_device_status(self, serial_number)
        print(device_statuses)
        self.assertEqual(len(device_statuses), 8)
        for device_status in device_statuses:
            self.assertEqual(device_status['timestamp'], timestamp)
            for feature, data in device_status['data'].items():
                self.assertTrue('value' in data)
                self.assertTrue('timestamp' in data)
                self.assertEqual(data['timestamp'], timestamp)
        self.assertEqual(mock.call_count, 4)
        mock.assert_called_with('invoking lambda function:send_push_notification with payload: {"reporting_id": "eeeeeeee-eeee-eeee-eeee-eeeeeeee0005", "object_id": "1.3.6.1.4.1.2435.2.3.9.4.2.1.5.5.52.3.1.3.17", "timestamp": "2017-02-01T12:23:01", "data": {"feature_name": "counter_value", "value": "2"}, "notify_url": "http://dummy.com"}')

    @patch('logging.info')
    def test_updated_email_count_type_status(self, mock):
        with open(
                f'{self.path}/../../data/device_statuses/stream/email/count_type_updated_status.json'
                ) as data_file:
            input = json.load(data_file)
        timestamp = (input['Records'][0]['dynamodb']['NewImage']['timestamp']['S'])
        serial_number = input['Records'][0]['dynamodb']['Keys']['serial_number']['S']
        before = get_email_device_status(self, serial_number)
        self.assertEqual(len(before), 8)
        stream.save_email_device_status(input, 'dummy')
        device_statuses = get_email_device_status(self, serial_number)
        self.assertEqual(len(device_statuses), 8)
        updated_count = 0
        for device_status in device_statuses:
            if 'counter_value' in device_status['data'] and device_status['data']['counter_value']['value'] == 'updated':
                self.assertEqual(device_status['data']['counter_value']['timestamp'], timestamp)
                updated_count += 1
            else:
                self.assertTrue(device_status in before)
        self.assertEqual(updated_count, 2)
        self.assertEqual(mock.call_count, 1)
        mock.assert_called_with('invoking lambda function:send_push_notification with payload: {"reporting_id": "eeeeeeee-eeee-eeee-eeee-eeeeeeee0007", "object_id": "1.3.6.1.4.1.2435.2.3.9.4.2.1.5.5.52.3.1.3.1", "timestamp": "2017-02-02T12:23:01", "data": {"feature_name": "counter_value", "value": "updated"}, "notify_url": "http://dummy.com"}')


def get_cloud_device_status(self, device_id, object_id):
    reporting_id = self.dynamodb.Table('reporting_registrations').query(
        IndexName='cloud_devices',
        KeyConditionExpression=Key('device_id').eq(device_id)
    )['Items'][0]['reporting_id']
    result = self.dynamodb.Table('device_statuses').get_item(Key={
            'reporting_id': reporting_id,
            'object_id': object_id
        })
    if 'Item' in result:
        return result['Item']


def get_email_device_status(self, serial_number):
    reporting_id = self.dynamodb.Table('reporting_registrations').query(
        IndexName='email_devices',
        KeyConditionExpression=Key('serial_number').eq(serial_number)
    )['Items'][0]['reporting_id']
    result = self.dynamodb.Table('device_statuses').query(
        KeyConditionExpression=Key('reporting_id').eq(reporting_id)
    )
    if 'Items' in result and result['Items']:
        return result['Items']

def get_accumulated_device_log(self, id, timestamp):
    result = self.dynamodb.Table('accumulated_device_logs').get_item(
        Key={
                'id': id,
                'year_month': timestamp[0:7]
                }
    )
    if 'Item' in result and result['Item']:
        return result['Item']

def main():
    unittest.main()


if __name__ == '__main__':
    main()
