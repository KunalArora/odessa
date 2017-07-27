import unittest
from unittest.mock import patch
from os import path
import logging
import json
from functions.subscriptions import handler
from tests.functions import test_helper


class SubscribeTestCase(unittest.TestCase):
    def setUp(self):
        test_helper.set_env_var(self)
        self.path = path.dirname(__file__)
        test_helper.seed_ddb_subscriptions(self)
        test_helper.seed_ec_subscriptions(self)
        logging.getLogger('subscriptions').setLevel(100)

    def tearDown(self):
        test_helper.clear_db(self)
        test_helper.clear_cache(self)
        test_helper.create_table(self)

    def test_bad_request(self):
        output = handler.subscribe(
            {'body': json.dumps({})}, 'dummy')
        self.assertFalse('device_id' in output)
        self.assertEqual(json.loads(output['body'])["code"], 400)
        self.assertEqual(json.loads(output['body'])["devices"], [])
        output = handler.subscribe(
            {'body': json.dumps({"time_period": 30})}, 'dummy')
        self.assertEqual(json.loads(output['body'])["code"], 400)
        output = handler.subscribe(
            {'body': json.dumps({"time_period": 30, "device_id": []})},
            'dummy')
        self.assertEqual(json.loads(output['body'])["code"], 400)
        output = handler.subscribe({'body': json.dumps({"device_id": []})},
                                   'dummy')
        self.assertEqual(json.loads(output['body'])["code"], 400)
        output = handler.subscribe(
            {'body': json.dumps(
                {"time_period": 30,
                 "device_id": ["ffffffff-ffff-ffff-ffff-ffffff000000"],
                 "log_service_id": "2"})},
            'dummy')
        self.assertEqual(json.loads(output['body'])["code"], 400)

    def test_subscribe_new_device(self):
        with open(
                f'{self.path}/../../data/subscribe/subscribe_new_device.json'
                ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.subscribe({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(len(output["devices"]), 1)
        self.assertEqual(output["devices"][0]["error_code"], 1202)
        self.assertEqual(output["devices"][0]["device_id"],
                         "ffffffff-ffff-ffff-ffff-ffffff0wrong")
        self.assertEqual(output["devices"][0]["message"], "Subscribe accepted")
        self.assertEqual(output["code"], 200)
        self.assertEqual(output["message"], "Success")
        output = handler.subscribe(
            {'body': json.dumps(
                {"time_period": 30,
                 "device_id": 'ffffffff-ffff-ffff-ffff-fffff1string'})},
            'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output["code"], 200)
        self.assertEqual(len(output["devices"]), 1)
        self.assertEqual(output["devices"][0]["error_code"], 1202)
        self.assertEqual(output["devices"][0]["device_id"],
                         "ffffffff-ffff-ffff-ffff-fffff1string")

    def test_subscribed_and_offline_device(self):
        with open(
                f'{self.path}/../../data/subscribe/subscribe_subscribed_and_offline_device.json'
                ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.subscribe({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(len(output["devices"]), 2)
        self.assertEqual(output["devices"][0]["error_code"], 1202)
        self.assertEqual(output["devices"][0]["device_id"],
                         "ffffffff-ffff-ffff-ffff-ffffff000000")
        self.assertEqual(output["devices"][0]["message"], "Subscribe accepted")
        self.assertEqual(output["devices"][1]["error_code"], 1202)
        self.assertEqual(output["devices"][1]["device_id"],
                         "ffffffff-ffff-ffff-ffff-ffffff000002")
        self.assertEqual(output["devices"][1]["message"], "Subscribe accepted")
        self.assertEqual(output["code"], 200)
        self.assertEqual(output["message"], "Success")

    def test_subscription_error_device(self):
        with open(
                f'{self.path}/../../data/subscribe/subscribe_subscription_error_device.json'
                ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.subscribe({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(len(output["devices"]), 2)
        self.assertEqual(output["devices"][0]["error_code"], 1202)
        self.assertEqual(output["devices"][0]["device_id"],
                         "ffffffff-ffff-ffff-ffff-ffffff000003")
        self.assertEqual(output["devices"][0]["message"], "Subscribe accepted")
        self.assertEqual(output["devices"][1]["error_code"], 1202)
        self.assertEqual(output["devices"][1]["device_id"],
                         "ffffffff-ffff-ffff-ffff-ffffff000004")
        self.assertEqual(output["devices"][1]["message"], "Subscribe accepted")
        self.assertEqual(output["code"], 200)
        self.assertEqual(output["message"], "Success")

    def test_subscribe_and_unsubscribe_accepted_device(self):
        with open(
                f'{self.path}/../../data/subscribe/subscribe_subscribe_and_unsubscribe_accepted_device.json'
                ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.subscribe({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(len(output["devices"]), 2)
        self.assertEqual(output["devices"][0]["error_code"], 1602)
        self.assertEqual(output["devices"][0]["device_id"],
                         "ffffffff-ffff-ffff-ffff-ffffff000005")
        self.assertEqual(
            output["devices"][0]["message"],
            "Subscribe exclusive control error (with other subscribing)")
        self.assertEqual(output["devices"][1]["error_code"], 1602)
        self.assertEqual(output["devices"][1]["device_id"],
                         "ffffffff-ffff-ffff-ffff-ffffff000007")
        self.assertEqual(
            output["devices"][1]["message"],
            "Subscribe exclusive control error (with other subscribing)")
        self.assertEqual(output["code"], 409)
        self.assertEqual(output["message"], "Requests conflict")

    def test_subscribe_multi_success(self):
        with open(
                f'{self.path}/../../data/subscribe/subscribe_multi_success.json'
                ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.subscribe({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(len(output["devices"]), 3)
        self.assertEqual(output["code"], 200)
        self.assertEqual(output["message"], "Success")

    def test_subscribe_multi_partial_success(self):
        with open(
                f'{self.path}/../../data/subscribe/subscribe_multi_partial_success.json'
                ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.subscribe({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(len(output["devices"]), 5)
        self.assertEqual(output["devices"][0]["message"], "Subscribe accepted")
        self.assertEqual(output["devices"][1]["error_code"], 2603)
        self.assertEqual(
            output["devices"][1]["message"],
            "Unsubscribe exclusive control error (with other unsubscribing)")
        self.assertEqual(output["devices"][2]["error_code"], 1602)
        self.assertEqual(
            output["devices"][2]["message"],
            "Subscribe exclusive control error (with other subscribing)")
        self.assertEqual(output["devices"][3]["error_code"], 1202)
        self.assertEqual(output["devices"][3]["message"], "Subscribe accepted")
        self.assertEqual(output["devices"][4]["error_code"], 1602)
        self.assertEqual(
            output["devices"][4]["message"],
            "Subscribe exclusive control error (with other subscribing)")
        self.assertEqual(output["code"], 207)
        self.assertEqual(output["message"], "Partial Success")

    def test_subscribe_multi_error(self):
        with open(
                f'{self.path}/../../data/subscribe/subscribe_multi_error.json'
                ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.subscribe({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(len(output["devices"]), 2)
        self.assertEqual(output["code"], 560)
        self.assertEqual(output["message"], "Error")

    def test_subscribe_unknown_error(self):
        with open(
                f'{self.path}/../../data/subscribe/subscribe_unknown_error.json'
                ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.subscribe({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(len(output["devices"]), 1)
        self.assertEqual(output["code"], 560)
        self.assertEqual(output["message"], "Error")
        self.assertEqual(output["devices"][0]["error_code"], 9999)

    @patch('functions.helper.invoke_async')
    def test_subscribe_async_payload(self, mock):
        with open(
                f'{self.path}/../../data/subscribe/subscribe_new_device.json'
                ) as data_file:
            input = json.dumps(json.load(data_file))
        handler.subscribe({'body': input}, 'dummy')
        mock.assert_called()
        mock.assert_called_with(
            'run_subscribe',
            '{"device_id": "ffffffff-ffff-ffff-ffff-ffffff0wrong", "log_service_id": "0", "time_period": 30}')


class UnsubscribeTestCase(unittest.TestCase):
    def setUp(self):
        test_helper.set_env_var(self)
        self.path = path.dirname(__file__)
        test_helper.seed_ddb_subscriptions(self)
        test_helper.seed_ec_subscriptions(self)
        logging.getLogger('subscriptions').setLevel(100)

    def tearDown(self):
        test_helper.clear_db(self)
        test_helper.clear_cache(self)
        test_helper.create_table(self)

    def test_bad_request(self):
        output = handler.unsubscribe(
            {'body': json.dumps({})}, 'dummy')
        self.assertFalse('device_id' in output)
        self.assertEqual(json.loads(output['body'])["code"], 400)
        self.assertEqual(json.loads(output['body'])["devices"], [])
        output = handler.unsubscribe(
            {'body': json.dumps({"device_id": []})}, 'dummy')
        self.assertEqual(json.loads(output['body'])["code"], 400)
        output = handler.unsubscribe(
            {'body': json.dumps(
                {"device_id": ["ffffffff-ffff-ffff-ffff-ffffff000000"],
                 "log_service_id": "2"})},
            'dummy')
        self.assertEqual(json.loads(output['body'])["code"], 400)

    def test_unsubscribe_unknown_device(self):
        with open(
                f'{self.path}/../../data/unsubscribe/unsubscribe_unknown_device.json'
                ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.unsubscribe({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(len(output["devices"]), 1)
        self.assertEqual(output["devices"][0]["error_code"], 200)
        self.assertEqual(output["devices"][0]["device_id"],
                         "ffffffff-ffff-ffff-ffff-ffffff0wrong")
        self.assertEqual(output["devices"][0]["message"], "Not Subscribed")
        self.assertEqual(output["code"], 200)
        self.assertEqual(output["message"], "Success")
        output = handler.unsubscribe(
            {'body': json.dumps(
                {"device_id": 'ffffffff-ffff-ffff-ffff-fffff1string'})},
            'dummy')
        output = json.loads(output['body'])
        self.assertEqual(len(output["devices"]), 1)
        self.assertEqual(output["code"], 200)
        self.assertEqual(output["devices"][0]["device_id"],
                         "ffffffff-ffff-ffff-ffff-fffff1string")
        self.assertEqual(output["devices"][0]["message"], "Not Subscribed")
        self.assertEqual(output["code"], 200)

    def test_unsubscribe_subscription_error_device(self):
        with open(
                f'{self.path}/../../data/unsubscribe/unsubscribe_subscription_error_device.json'
                ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.unsubscribe({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(len(output["devices"]), 2)
        self.assertEqual(output["devices"][0]["error_code"], 200)
        self.assertEqual(output["devices"][0]["device_id"],
                         "ffffffff-ffff-ffff-ffff-ffffff000003")
        self.assertEqual(output["devices"][0]["message"], "Not Subscribed")
        self.assertEqual(output["devices"][1]["error_code"], 200)
        self.assertEqual(output["devices"][1]["device_id"],
                         "ffffffff-ffff-ffff-ffff-ffffff000004")
        self.assertEqual(output["devices"][1]["message"], "Not Subscribed")
        self.assertEqual(output["code"], 200)
        self.assertEqual(output["message"], "Success")

    def test_unsubscribe_subscribed_and_offline_device(self):
        with open(
                f'{self.path}/../../data/unsubscribe/unsubscribe_subscribed_and_offline_device.json'
                ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.unsubscribe({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(len(output["devices"]), 2)
        self.assertEqual(output["devices"][0]["error_code"], 2202)
        self.assertEqual(output["devices"][0]["device_id"],
                         "ffffffff-ffff-ffff-ffff-ffffff000000")
        self.assertEqual(output["devices"][0]["message"],
                         "Unsubscribe accepted")
        self.assertEqual(output["devices"][1]["error_code"], 2202)
        self.assertEqual(output["devices"][1]["device_id"],
                         "ffffffff-ffff-ffff-ffff-ffffff000002")
        self.assertEqual(output["devices"][1]["message"],
                         "Unsubscribe accepted")
        self.assertEqual(output["code"], 200)
        self.assertEqual(output["message"], "Success")

    def test_unsubscribe_subscribe_and_unsubscribe_accepted_device(self):
        with open(
                f'{self.path}/../../data/unsubscribe/unsubscribe_subscribe_and_unsubscribe_accepted_device.json'
                ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.unsubscribe({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(len(output["devices"]), 2)
        self.assertEqual(output["devices"][0]["error_code"], 2602)
        self.assertEqual(output["devices"][0]["device_id"],
                         "ffffffff-ffff-ffff-ffff-ffffff000005")
        self.assertEqual(
            output["devices"][0]["message"],
            "Unsubscribe exclusive control error (with other subscribing)")
        self.assertEqual(output["devices"][1]["error_code"], 2602)
        self.assertEqual(output["devices"][1]["device_id"],
                         "ffffffff-ffff-ffff-ffff-ffffff000007")
        self.assertEqual(
            output["devices"][1]["message"],
            "Unsubscribe exclusive control error (with other subscribing)")
        self.assertEqual(output["code"], 409)
        self.assertEqual(output["message"], "Requests conflict")

    def test_unsubscribe_unknown_error(self):
        with open(
            f'{self.path}/../../data/unsubscribe/unsubscribe_unknown_error.json'
                ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.unsubscribe({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(len(output["devices"]), 1)
        self.assertEqual(output["code"], 409)
        self.assertEqual(output["message"], "Requests conflict")
        self.assertEqual(output["devices"][0]["error_code"], 9999)

    def test_unsubscribe_multi_success(self):
        with open(
            f'{self.path}/../../data/unsubscribe/unsubscribe_multi_success.json'
                ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.unsubscribe({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(len(output["devices"]), 4)
        self.assertEqual(output["code"], 200)
        self.assertEqual(output["message"], "Success")

    def test_unsubscribe_multi_partial_success(self):
        with open(
                f'{self.path}/../../data/unsubscribe/unsubscribe_multi_partial_success.json'
                ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.unsubscribe({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(len(output["devices"]), 5)
        self.assertEqual(output["code"], 207)
        self.assertEqual(output["message"], "Partial Success")

    @patch('functions.helper.invoke_async')
    def test_unsubscribe_async_payload(self, mock):
        with open(
                f'{self.path}/../../data/unsubscribe/unsubscribe_subscribed_and_offline_device.json'
                ) as data_file:
            input = json.dumps(json.load(data_file))
        handler.unsubscribe({'body': input}, 'dummy')
        mock.assert_called_with(
            'run_unsubscribe',
            '{"device_id": "ffffffff-ffff-ffff-ffff-ffffff000002", "log_service_id": "0"}')


class SubscriptionInfoTestCase(unittest.TestCase):

    def setUp(self):
        test_helper.set_env_var(self)
        self.path = path.dirname(__file__)
        test_helper.seed_ddb_subscriptions(self)
        test_helper.seed_ec_subscriptions(self)
        logging.getLogger('subscriptions').setLevel(100)

    def tearDown(self):
        test_helper.clear_db(self)
        test_helper.clear_cache(self)
        test_helper.create_table(self)

    def test_bad_request(self):
        output = handler.subscription_info(
            {'body': json.dumps({})}, 'dummy')
        self.assertFalse('device_id' in output)
        self.assertEqual(json.loads(output['body'])["code"], 400)
        self.assertEqual(json.loads(output['body'])["devices"], [])
        output = handler.subscription_info(
            {'body': json.dumps({"device_id": []})}, 'dummy')
        self.assertEqual(json.loads(output['body'])["code"], 400)
        output = handler.subscription_info(
            {'body': json.dumps(
                {"device_id": ['ffffffff-ffff-ffff-ffff-ffffff000000'],
                 "log_service_id": "2"})}, 'dummy')
        self.assertEqual(json.loads(output['body'])["code"], 400)

    @patch('boc.base.Base.post_content')
    def test_get_subscription(self, mock):
        mock.return_value = {
            'success': True,
            'message': 'Success.',
            'code': 200,
            'notifications':
                [{'error_code': '200',
                  'object_id': '1.3.6.1.2.1.1.4.0',
                  'status': '70726F78792E62726F746865722E636F2E6A70',
                  'user_id': '184878',
                  'timestamp': '2017-06-30 07:09:00'},
                 {'error_code': '200',
                  'object_id': '1.3.6.1.2.1.1.6.0',
                  'status': '70726F78792E62726F746865722E636F2E6A70',
                  'user_id': '184878',
                  'timestamp': '2017-06-30 07:09:00'},
                 {'error_code': '200',
                  'object_id': '1.3.6.1.2.1.2.2.1.6.1',
                  'status': '70726F78792E62726F746865722E636F2E6A70',
                  'user_id': '184878',
                  'timestamp': '2017-06-30 07:09:00'},
                 {'error_code': '200',
                  'object_id': '1.3.6.1.2.1.25.3.2.1.3.1',
                  'status': '70726F78792E62726F746865722E636F2E6A70',
                  'user_id': '184878',
                  'timestamp': '2017-06-30 07:09:00'}]}

        with open(
                f'{self.path}/../../data/subscription_info/subscription_info.json'
                ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.subscription_info({'body': input}, 'dummy')
        output = json.loads(output['body'])
        mock.assert_called()
        self.assertEqual(len(output["devices"]), 11)
        self.assertEqual(output["devices"][0]["error_code"], 200)
        self.assertEqual(output["devices"][1]["error_code"], 1200)
        self.assertEqual(output["devices"][2]["error_code"], 2603)
        self.assertEqual(output["devices"][3]["error_code"], 1200)
        self.assertEqual(output["devices"][4]["error_code"], 1505)
        self.assertEqual(output["devices"][5]["error_code"], 1602)
        self.assertEqual(output["devices"][6]["error_code"], 2202)
        self.assertEqual(output["devices"][7]["error_code"], 1600)
        self.assertEqual(output["devices"][8]["error_code"], 1202)
        self.assertEqual(output["devices"][9]["error_code"], 2603)
        self.assertEqual(output["devices"][10]["error_code"], 2500)
        self.assertEqual(output["code"], 200)
        self.assertEqual(output["message"], "Success")

    @patch('boc.base.Base.post_content')
    def test_get_boc_unsubscribed_device(self, mock):
        mock.return_value = {
            'success': False,
            'message': 'Unrecognised device_id',
            'code': 505}
        with open(
                f'{self.path}/../../data/subscription_info/boc_unsubscribed_device.json'
                ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.subscription_info({'body': input}, 'dummy')
        output = json.loads(output['body'])
        mock.assert_called()
        self.assertEqual(len(output["devices"]), 1)
        self.assertEqual(output["devices"][0]["error_code"], 200)
        self.assertEqual(output["code"], 200)
        self.assertEqual(output["message"], "Success")

    @patch('boc.base.Base.post_content')
    def test_get_odessa_unsubscribed_device(self, mock):
        mock.return_value = {
            'success': False,
            'message': 'Object subscription not found',
            'code': 524}
        with open(
                f'{self.path}/../../data/subscription_info/boc_unsubscribed_device.json'
                ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.subscription_info({'body': input}, 'dummy')
        output = json.loads(output['body'])
        mock.assert_called()
        self.assertEqual(len(output["devices"]), 1)
        self.assertEqual(output["devices"][0]["error_code"], 200)
        self.assertEqual(output["code"], 200)
        self.assertEqual(output["message"], "Success")

    def test_non_existing_device(self):
        with open(
                f'{self.path}/../../data/subscription_info/non_existing_device.json'
                ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.subscription_info({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(len(output["devices"]), 1)
        self.assertEqual(output["devices"][0]["error_code"], 200)
        self.assertEqual(output["code"], 200)
        self.assertEqual(output["message"], "Success")

    @patch('boc.base.Base.post_content')
    def test_unknown_error(self, mock):
        mock.return_value = {
            'success': False,
            'message': 'Unknown Error',
            'code': 999}
        with open(
                f'{self.path}/../../data/subscription_info/boc_unsubscribed_device.json'
                ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.subscription_info({'body': input}, 'dummy')
        output = json.loads(output['body'])
        mock.assert_called()
        self.assertEqual(len(output["devices"]), 1)
        self.assertEqual(output["devices"][0]["error_code"], 999)
        self.assertEqual(output["code"], 200)
        self.assertEqual(output["message"], "Success")
        # params: single device id string
        output = handler.subscription_info({'body': json.dumps(
            {"device_id": "ffffffff-ffff-ffff-ffff-ffffff000000"})}, 'dummy')
        output = json.loads(output['body'])
        mock.assert_called()
        self.assertEqual(len(output["devices"]), 1)
        self.assertEqual(output["devices"][0]["error_code"], 999)
        self.assertEqual(output["code"], 200)
        self.assertEqual(output["message"], "Success")

    @patch('boc.base.Base.post_content')
    def test_get_offline_device(self, mock):
        mock.return_value = {
            'success': True,
            'message': 'Success.',
            'code': 200,
            'notifications':
                [{'error_code': '404',
                  'object_id': '1.3.6.1.2.1.1.6.0',
                  'status': '0',
                  'user_id': '184878',
                  'timestamp': '0'}]}
        with open(
                f'{self.path}/../../data/subscription_info/boc_unsubscribed_device.json'
                ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.subscription_info({'body': input}, 'dummy')
        output = json.loads(output['body'])
        mock.assert_called_with(
            'https://dev-connections.mysora.net/svc_api/devices/notify_result',
            {'service_id': '2',
             'device_id': 'ffffffff-ffff-ffff-ffff-ffffff000000',
             'object_id[0]': '1.3.6.1.2.1.1.6.0'},
            300)
        self.assertEqual(len(output["devices"]), 1)
        self.assertEqual(output["devices"][0]["error_code"], 1201)
        self.assertEqual(output["code"], 200)
        self.assertEqual(output["message"], "Success")

    @patch('boc.base.Base.post_content')
    def test_get_offline_turned_online_device(self, mock):
        mock.return_value = {
            'success': True,
            'message': 'Success.',
            'code': 200,
            'notifications':
                [{'error_code': '200',
                  'object_id': '1.3.6.1.2.1.1.6.0',
                  'status': '0',
                  'user_id': '184878',
                  'timestamp': '0'},
                 {'error_code': '404',
                  'object_id': '1.3.6.1.2.1.1.4.0',
                  'status': '0',
                  'user_id': '184878',
                  'timestamp': '0'},
                 {'error_code': '524',
                  'object_id': '1.3.6.1.2.1.2.2.1.6.1',
                  'status': '0',
                  'user_id': '184878',
                  'timestamp': '0'},
                 {'error_code': '200',
                  'object_id': '1.3.6.1.2.1.25.3.2.1.3.1',
                  'status': '0',
                  'user_id': '184878',
                  'timestamp': '0'}
                 ]}
        with open(
                f'{self.path}/../../data/subscription_info/offline_turned_online_device.json'
                ) as data_file:
            input = json.dumps(json.load(data_file))

        before = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000014#0')
        self.assertEqual(len(before['Items']), 4)
        for subscription in before['Items']:
            self.assertEqual(int(subscription['status']), 1201)

        output = handler.subscription_info({'body': input}, 'dummy')
        output = json.loads(output['body'])
        mock.assert_called_with(
            'https://dev-connections.mysora.net/svc_api/devices/notify_result',
            {'service_id': '2',
             'device_id': 'ffffffff-ffff-ffff-ffff-ffffff000014',
             'object_id[0]': '1.3.6.1.2.1.1.4.0',
             'object_id[1]': '1.3.6.1.2.1.1.6.0',
             'object_id[2]': '1.3.6.1.2.1.2.2.1.6.1',
             'object_id[3]': '1.3.6.1.2.1.25.3.2.1.3.1'},
            300)
        self.assertEqual(len(output["devices"]), 1)
        self.assertEqual(output["devices"][0]["error_code"], 1200)
        self.assertEqual(output["code"], 200)
        self.assertEqual(output["message"], "Success")
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000014#0')
        self.assertEqual(len(after['Items']), 3)
        for subscription in after['Items']:
            self.assertEqual(int(subscription['status']), 1200)


def main():
    unittest.main()


if __name__ == '__main__':
    main()
