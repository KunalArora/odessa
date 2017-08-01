import unittest
import json
import logging
from os import path
from functions.subscriptions import stream
from tests.functions import test_helper

logging.disable(logging.CRITICAL)


class StreamTestCase(unittest.TestCase):
    def setUp(self):
        test_helper.set_env_var(self)
        self.path = path.dirname(__file__)
        test_helper.seed_ddb_subscriptions(self)
        test_helper.seed_ec_subscriptions(self)
        logging.getLogger('subscriptions:stream').setLevel(100)

    def tearDown(self):
        test_helper.clear_db(self)
        test_helper.clear_cache(self)
        test_helper.create_table(self)

    def test_insert_subscription(self):
        with open(
                f'{self.path}/../../data/stream/new_subscription.json'
                ) as data_file:
            input = json.load(data_file)
        before = self.elasticache.hgetall(
            'device_subscriptions:ffffffff-ffff-ffff-ffff-ffffff0wrong#0')
        self.assertEqual(len(before), 0)
        stream.subscriptions(input, 'dummy')
        after = self.elasticache.hgetall(
            'device_subscriptions:ffffffff-ffff-ffff-ffff-ffffff0wrong#0')
        self.assertEqual(len(after), 5)
        values = test_helper.convert(after)
        self.assertEqual(int(values['status']), 1202)
        self.assertEqual(values['message'], 'Subscribe accepted')
        self.assertEqual(values['created_at'], '2017-06-02T00:59:59')
        self.assertEqual(values['updated_at'], '2017-06-02T00:59:59')

    def test_update_subscription(self):
        with open(
                f'{self.path}/../../data/stream/update_subscription.json'
                ) as data_file:
            input = json.load(data_file)
        before = self.elasticache.hgetall(
            'device_subscriptions:ffffffff-ffff-ffff-ffff-ffffff000002#0')
        self.assertEqual(len(before), 5)
        values = test_helper.convert(before)
        self.assertEqual(int(values['status']), 1201)
        self.assertEqual(values['message'], 'Subscribed (Device Offline)')
        self.assertEqual(values['created_at'], '2017-06-01T00:00:06')
        self.assertEqual(values['updated_at'], '2017-06-01T00:00:07')
        stream.subscriptions(input, 'dummy')
        after = self.elasticache.hgetall(
            'device_subscriptions:ffffffff-ffff-ffff-ffff-ffffff000002#0')
        self.assertEqual(len(after), 5)
        values = test_helper.convert(after)
        self.assertEqual(int(values['status']), 1202)
        self.assertEqual(values['message'], 'Subscribe accepted')
        self.assertEqual(values['created_at'], '2017-06-01T00:00:06')
        self.assertEqual(values['updated_at'], '2017-06-03T00:59:59')

    def test_delete_subscription(self):
        with open(
                f'{self.path}/../../data/stream/delete_subscription.json'
                ) as data_file:
            input = json.load(data_file)
        before = self.elasticache.hgetall(
            'device_subscriptions:ffffffff-ffff-ffff-ffff-ffffff000003#0')
        self.assertEqual(len(before), 5)
        stream.subscriptions(input, 'dummy')
        after = self.elasticache.hgetall(
            'device_subscriptions:ffffffff-ffff-ffff-ffff-ffffff000003#0')
        self.assertEqual(len(after), 0)


def main():
    unittest.main()


if __name__ == '__main__':
    main()
