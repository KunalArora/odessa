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

    @patch('logging.info')
    def test_newly_subscribed_device(self, mock):
        with open(
                f'{self.path}/../../data/device_statuses/stream/new_status.json'
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

    def test_updated_device_status(self):
        with open(
                f'{self.path}/../../data/device_statuses/stream/updated_status.json'
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

    def test_unchanged_device_status(self):
        with open(
                f'{self.path}/../../data/device_statuses/stream/unchanged_status.json'
                ) as data_file:
            input = json.load(data_file)
        id = input['Records'][0]['dynamodb']['Keys']['id']['S'].split('#')
        before = get_cloud_device_status(self, id[0], id[1])
        self.assertTrue(before)
        stream.save_cloud_device_status(input, 'dummy')
        after = get_cloud_device_status(self, id[0], id[1])
        self.assertEqual(before, after)

    def test_outdated_device_status(self):
        with open(
                f'{self.path}/../../data/device_statuses/stream/outdated_status.json'
                ) as data_file:
            input = json.load(data_file)
        id = input['Records'][0]['dynamodb']['Keys']['id']['S'].split('#')
        before = get_cloud_device_status(self, id[0], id[1])
        self.assertTrue(before)
        stream.save_cloud_device_status(input, 'dummy')
        after = get_cloud_device_status(self, id[0], id[1])
        self.assertEqual(before, after)

    @patch('logging.info')
    def test_unsubscribed_status(self, mock):
        with open(
                f'{self.path}/../../data/device_statuses/stream/unsubscribed_status.json'
                ) as data_file:
            input = json.load(data_file)
        id = input['Records'][0]['dynamodb']['Keys']['id']['S'].split('#')
        self.assertIsNone(get_cloud_device_status(self, id[0], id[1]))
        stream.save_cloud_device_status(input, 'dummy')
        device_status = get_cloud_device_status(self, id[0], id[1])
        self.assertTrue(device_status)
        mock.assert_not_called()


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


def main():
    unittest.main()


if __name__ == '__main__':
    main()
