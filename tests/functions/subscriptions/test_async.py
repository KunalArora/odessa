import unittest
from unittest.mock import patch, MagicMock
from os import path, environ
import logging
from functions.subscriptions import async
from tests.functions import test_helper


class AsyncSubscribeTestCase(unittest.TestCase):
    def setUp(self):
        test_helper.set_env_var(self)
        self.path = path.dirname(__file__)
        test_helper.seed_ddb_subscriptions(self)
        test_helper.seed_ec_subscriptions(self)
        self.mock_context = MagicMock()
        self.mock_context.aws_request_id = 'mock_aws_request_id'
        logging.getLogger('subscriptions:async').setLevel(100)

    def tearDown(self):
        test_helper.clear_db(self)
        test_helper.clear_cache(self)
        test_helper.create_table(self)

    def test_bad_request(self):
        async.run_subscribe({}, self.mock_context)
        with self.assertLogs('subscriptions:async', level='WARNING') as log:
            logging.getLogger('subscriptions:async').warning(
                'BadRequest on handler:subscribe')
            self.assertEqual(
                log.output[0],
                'WARNING:subscriptions:async:BadRequest on handler:subscribe')
        async.run_subscribe({"time_period": 30}, self.mock_context)
        with self.assertLogs('subscriptions:async', level='WARNING') as log:
            logging.getLogger('subscriptions:async').warning(
                'BadRequest on handler:subscribe')
            self.assertEqual(
                log.output[0],
                'WARNING:subscriptions:async:BadRequest on handler:subscribe')
        async.run_subscribe(
            {"time_period": 30,
             "device_id": "ffffffff-ffff-ffff-ffff-ffffff0wrong"},
            self.mock_context)
        with self.assertLogs('subscriptions:async', level='WARNING') as log:
            logging.getLogger('subscriptions:async').warning(
                'BadRequest on handler:subscribe')
            self.assertEqual(
                log.output[0],
                'WARNING:subscriptions:async:BadRequest on handler:subscribe')
        async.run_subscribe(
            {"time_period": 30, "device_id": []}, self.mock_context)
        with self.assertLogs('subscriptions:async', level='WARNING') as log:
            logging.getLogger('subscriptions:async').warning(
                'BadRequest on handler:subscribe')
            self.assertEqual(
                log.output[0],
                'WARNING:subscriptions:async:BadRequest on handler:subscribe')
        async.run_subscribe({"device_id": []}, self.mock_context)
        with self.assertLogs('subscriptions:async', level='WARNING') as log:
            logging.getLogger('subscriptions:async').warning(
                'BadRequest on handler:subscribe')
            self.assertEqual(
                log.output[0],
                'WARNING:subscriptions:async:BadRequest on handler:subscribe')

    @patch('boc.base.Base.post_content')
    def test_subscribe_success(self, mock):
        mock.return_value = {
            'success': True,
            'code': 200,
            'message': 'Success.',
            'subscribe': [
                {'error_code': '200',
                 'object_id': '1.3.6.1.2.1.25.3.2.1.3.1',
                 'message': 'No error.'},
                {'error_code': '200',
                 'object_id': '1.3.6.1.2.1.2.2.1.6.1',
                 'message': 'No error.'},
                {'error_code': '200',
                 'object_id': '1.3.6.1.2.1.1.6.0',
                 'message': 'No error.'},
                {'error_code': '200',
                 'object_id': '1.3.6.1.2.1.1.4.0',
                 'message': 'No error.'}
            ]
        }

        before = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000011#0')
        self.assertEqual(len(before['Items'][0]['oids']), 4)
        self.assertEqual(int(before['Items'][0]['status']), 1202)
        async.run_subscribe({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000011",
            "log_service_id": "0", "time_period": -1}, self.mock_context)
        mock.assert_called_with(
            'https://dev-connections.mysora.net/svc_api/devices/subscribe',
            {'service_id': '2',
             'device_id': 'ffffffff-ffff-ffff-ffff-ffffff000011',
             'subscription_info[0][object_id]': '1.3.6.1.2.1.25.3.2.1.3.1',
             'subscription_info[0][time_period]': 60,
             'subscription_info[1][object_id]': '1.3.6.1.2.1.2.2.1.6.1',
             'subscription_info[1][time_period]': 60,
             'subscription_info[2][object_id]': '1.3.6.1.2.1.1.6.0',
             'subscription_info[2][time_period]': 60,
             'subscription_info[3][object_id]': '1.3.6.1.2.1.1.4.0',
             'subscription_info[3][time_period]': 60,
             'callback': 'http://dummy.com',
             'is_fwd': 'true'}, 300)
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000011#0')
        self.assertEqual(len(after['Items'][0]['oids']), 4)
        self.assertEqual(int(after['Items'][0]['status']), 1200)

    @patch('boc.base.Base.post_content')
    def test_subscribe_non_existing_device(self, mock):
        before = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff0wrong#0')
        self.assertEqual(len(before['Items']), 0)
        async.run_subscribe({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff0wrong",
            "log_service_id": "0", "time_period": 1}, self.mock_context)
        mock.assert_not_called()
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff0wrong#0')
        self.assertEqual(len(after['Items']), 0)

    @patch('boc.base.Base.post_content')
    def test_subscribe_non_existing_log_service_id(self, mock):
        before = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000011#1')
        self.assertEqual(len(before['Items']), 0)
        async.run_subscribe({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000011",
            "log_service_id": "1", "time_period": 1}, self.mock_context)
        mock.assert_not_called()
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000011#1')
        self.assertEqual(len(after['Items']), 0)

    @patch('boc.base.Base.post_content')
    def test_subscribe_error_success(self, mock):
        mock.return_value = {
            'success': False,
            'code': 500,
            'message': 'Error',
            'subscribe': [{
                    'error_code': '463',
                    'object_id': '1.3.6.1.2.1.1.4.0',
                    'message': 'Already subscribed'},
                {
                    'error_code': '463',
                    'object_id': '1.3.6.1.2.1.1.6.0',
                    'message': 'Already subscribed'},
                {
                    'error_code': '463',
                    'object_id': '1.3.6.1.2.1.2.2.1.6.1',
                    'message': 'Already subscribed'},
                {
                    'error_code': '463',
                    'object_id': '1.3.6.1.2.1.25.3.2.1.3.1',
                    'message': 'Already subscribed'}]}

        before = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000011#0')
        self.assertEqual(len(before['Items'][0]['oids']), 4)
        self.assertEqual(int(before['Items'][0]['status']), 1202)
        async.run_subscribe({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000011",
            "log_service_id": "0", "time_period": 1000}, self.mock_context)
        mock.assert_called_with(
            'https://dev-connections.mysora.net/svc_api/devices/subscribe',
            {'service_id': '2',
             'device_id': 'ffffffff-ffff-ffff-ffff-ffffff000011',
             'subscription_info[0][object_id]': '1.3.6.1.2.1.25.3.2.1.3.1',
             'subscription_info[0][time_period]': 18000,
             'subscription_info[1][object_id]': '1.3.6.1.2.1.2.2.1.6.1',
             'subscription_info[1][time_period]': 18000,
             'subscription_info[2][object_id]': '1.3.6.1.2.1.1.6.0',
             'subscription_info[2][time_period]': 18000,
             'subscription_info[3][object_id]': '1.3.6.1.2.1.1.4.0',
             'subscription_info[3][time_period]': 18000,
             'callback': 'http://dummy.com',
             'is_fwd': 'true'}, 300)
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000011#0')
        self.assertEqual(len(after['Items'][0]['oids']), 4)
        self.assertEqual(int(after['Items'][0]['status']), 1200)

    @patch('boc.base.Base.post_content')
    def test_subscribe_partial_success(self, mock):
        mock.return_value = {
            'success': True,
            'code': 207,
            'message': 'Partial success.',
            'subscribe': [{
                    'error_code': '463',
                    'object_id': '1.3.6.1.2.1.1.4.0',
                    'message': 'Already subscribed'},
                {
                    'error_code': '200',
                    'object_id': '1.3.6.1.2.1.1.6.0',
                    'message': 'No Error'},
                {
                    'error_code': '463',
                    'object_id': '1.3.6.1.2.1.2.2.1.6.1',
                    'message': 'Already subscribed'},
                {
                    'error_code': '404',
                    'object_id': '1.3.6.1.2.1.25.3.2.1.3.1',
                    'message': 'No such object id'}]}

        before = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000011#0')
        self.assertEqual(len(before['Items'][0]['oids']), 4)
        self.assertEqual(int(before['Items'][0]['status']), 1202)
        async.run_subscribe({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000011",
            "log_service_id": "0", "time_period": "45"}, self.mock_context)
        mock.assert_called_with(
            'https://dev-connections.mysora.net/svc_api/devices/subscribe',
            {'service_id': '2',
             'device_id': 'ffffffff-ffff-ffff-ffff-ffffff000011',
             'subscription_info[0][object_id]': '1.3.6.1.2.1.25.3.2.1.3.1',
             'subscription_info[0][time_period]': 3600,
             'subscription_info[1][object_id]': '1.3.6.1.2.1.2.2.1.6.1',
             'subscription_info[1][time_period]': 3600,
             'subscription_info[2][object_id]': '1.3.6.1.2.1.1.6.0',
             'subscription_info[2][time_period]': 3600,
             'subscription_info[3][object_id]': '1.3.6.1.2.1.1.4.0',
             'subscription_info[3][time_period]': 3600,
             'callback': 'http://dummy.com',
             'is_fwd': 'true'}, 300)
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000011#0')
        self.assertEqual(len(after['Items'][0]['oids']), 3)
        self.assertEqual(int(after['Items'][0]['status']), 1200)

    @patch('boc.base.Base.post_content')
    def test_subscribe_partial_error(self, mock):
        mock.return_value = {
            'success': True,
            'code': 207,
            'message': 'Partial success.',
            'subscribe': [{
                    'error_code': '463',
                    'object_id': '1.3.6.1.2.1.1.4.0',
                    'message': 'Already subscribed'},
                {
                    'error_code': '200',
                    'object_id': '1.3.6.1.2.1.1.6.0',
                    'message': 'No Error'},
                {
                    'error_code': '505',
                    'object_id': '1.3.6.1.2.1.2.2.1.6.1',
                    'message': 'Device not recognized'},
                {
                    'error_code': '404',
                    'object_id': '1.3.6.1.2.1.25.3.2.1.3.1',
                    'message': 'No such object id'}]}

        before = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000011#0')
        self.assertEqual(len(before['Items'][0]['oids']), 4)
        self.assertEqual(int(before['Items'][0]['status']), 1202)
        async.run_subscribe({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000011",
            "log_service_id": "0", "time_period": 45}, self.mock_context)
        mock.assert_called()
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000011#0')
        self.assertEqual(len(after['Items'][0]['oids']), 3)
        self.assertEqual(int(after['Items'][0]['status']), 1207)

    @patch('boc.base.Base.post_content')
    def test_subscribe_error(self, mock):
        mock.return_value = {
            'success': False,
            'code': 500,
            'message': 'Error.',
            'subscribe': [{
                    'error_code': '500',
                    'object_id': '1.3.6.1.2.1.1.4.0',
                    'message': 'Other error.'},
                {
                    'error_code': '500',
                    'object_id': '1.3.6.1.2.1.1.6.0',
                    'message': 'Other error.'},
                {
                    'error_code': '500',
                    'object_id': '1.3.6.1.2.1.2.2.1.6.1',
                    'message': 'Other error.'},
                {
                    'error_code': '500',
                    'object_id': '1.3.6.1.2.1.25.3.2.1.3.1',
                    'message': 'Other error.'}]}

        before = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000011#0')
        self.assertEqual(len(before['Items'][0]['oids']), 4)
        self.assertEqual(int(before['Items'][0]['status']), 1202)
        async.run_subscribe({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000011",
            "log_service_id": "0", "time_period": 45}, self.mock_context)
        mock.assert_called_with(
            'https://dev-connections.mysora.net/svc_api/devices/subscribe',
            {'service_id': '2',
             'device_id': 'ffffffff-ffff-ffff-ffff-ffffff000011',
             'subscription_info[0][object_id]': '1.3.6.1.2.1.25.3.2.1.3.1',
             'subscription_info[0][time_period]': 2700,
             'subscription_info[1][object_id]': '1.3.6.1.2.1.2.2.1.6.1',
             'subscription_info[1][time_period]': 2700,
             'subscription_info[2][object_id]': '1.3.6.1.2.1.1.6.0',
             'subscription_info[2][time_period]': 2700,
             'subscription_info[3][object_id]': '1.3.6.1.2.1.1.4.0',
             'subscription_info[3][time_period]': 2700,
             'callback': 'http://dummy.com',
             'is_fwd': 'true'}, 300)
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000011#0')
        self.assertEqual(len(after['Items'][0]['oids']), 4)
        self.assertEqual(int(after['Items'][0]['status']), 1500)

    @patch('boc.base.Base.post_content')
    def test_subscribe_unknown_error(self, mock):
        mock.return_value = {
            'success': False,
            'code': 999,
            'message': 'Unknown.'}
        before = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000011#0')
        self.assertEqual(len(before['Items'][0]['oids']), 4)
        self.assertEqual(int(before['Items'][0]['status']), 1202)
        async.run_subscribe({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000011",
            "log_service_id": "0", "time_period": 45}, self.mock_context)
        mock.assert_called()
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000011#0')
        self.assertEqual(len(after['Items'][0]['oids']), 4)
        self.assertEqual(int(after['Items'][0]['status']), 1999)

    @patch('boc.base.Base.post_content')
    def test_subscribe_offline_device(self, mock):
        mock.return_value = {
            'success': True,
            'code': 210,
            'message': 'Success but device offline'}
        before = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000011#0')
        self.assertEqual(len(before['Items'][0]['oids']), 4)
        self.assertEqual(int(before['Items'][0]['status']), 1202)
        async.run_subscribe({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000011",
            "log_service_id": "0", "time_period": 45}, self.mock_context)
        mock.assert_called()
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000011#0')
        self.assertEqual(len(after['Items'][0]['oids']), 4)
        self.assertEqual(int(after['Items'][0]['status']), 1201)

    @patch('boc.base.Base.post_content')
    def test_subscribe_unrecognized_device(self, mock):
        mock.return_value = {
            'success': False,
            'code': 505,
            'message': 'Device not recognized'}
        before = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000011#0')
        self.assertEqual(len(before['Items'][0]['oids']), 4)
        self.assertEqual(int(before['Items'][0]['status']), 1202)
        async.run_subscribe({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000011",
            "log_service_id": "0", "time_period": 45}, self.mock_context)
        mock.assert_called()
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000011#0')
        self.assertEqual(len(after['Items'][0]['oids']), 4)
        self.assertEqual(int(after['Items'][0]['status']), 404)

    @patch('boc.base.Base.post_content')
    def test_subscribe_duplicate_request(self, mock):
        environ['REDIS_ENDPOINT_URL'] = ''
        mock.return_value = {
            'success': True,
            'code': 200,
            'message': 'Success.',
            'subscribe': [
                {'error_code': '200',
                 'object_id': '1.3.6.1.2.1.25.3.2.1.3.1',
                 'message': 'No error.'}
            ]
        }

        before = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000011#0')
        self.assertTrue('latest_async_id' not in before['Items'][0])
        async.run_subscribe({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000011",
            "log_service_id": "0", "time_period": 60}, self.mock_context)
        mock.assert_called()
        mock.reset_mock()
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000011#0')
        self.assertEqual(int(after['Items'][0]['status']), 1200)
        self.assertTrue('latest_async_id' in after['Items'][0])
        async.run_subscribe({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000011",
            "log_service_id": "0", "time_period": 60}, self.mock_context)
        mock.assert_not_called()

    @patch('boc.base.Base.post_content')
    def test_subscribe_duplicate_request_with_error(self, mock):
        environ['REDIS_ENDPOINT_URL'] = ''
        mock.return_value = {
            'success': False,
            'code': 999,
            'message': 'Unknown.'
        }

        before = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000011#0')
        self.assertTrue('latest_async_id' not in before['Items'][0])
        async.run_subscribe({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000011",
            "log_service_id": "0", "time_period": 60}, self.mock_context)
        mock.assert_called()
        mock.reset_mock()
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000011#0')
        self.assertEqual(int(after['Items'][0]['status']), 1999)
        self.assertTrue('latest_async_id' not in after['Items'][0])
        mock.return_value = {
            'success': True,
            'code': 200,
            'message': 'Success.',
            'subscribe': [
                {'error_code': '200',
                 'object_id': '1.3.6.1.2.1.25.3.2.1.3.1',
                 'message': 'No error.'}
            ]
        }
        async.run_subscribe({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000011",
            "log_service_id": "0", "time_period": 60}, self.mock_context)
        mock.assert_called()
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000011#0')
        self.assertTrue('latest_async_id' in after['Items'][0])


class AsyncUnsubscribeTestCase(unittest.TestCase):
    def setUp(self):
        test_helper.set_env_var(self)
        self.path = path.dirname(__file__)
        test_helper.seed_ddb_subscriptions(self)
        test_helper.seed_ec_subscriptions(self)
        logging.getLogger('subscriptions:async').setLevel(100)

    def tearDown(self):
        test_helper.clear_db(self)
        test_helper.clear_cache(self)
        test_helper.create_table(self)

    def test_bad_request(self):
        async.run_unsubscribe({}, 'dummy')
        with self.assertLogs('subscriptions:async', level='WARNING') as log:
            logging.getLogger('subscriptions:async').warning(
                'BadRequest on handler:unsubscribe')
            self.assertEqual(
                log.output[0],
                'WARNING:subscriptions:async:BadRequest on handler:unsubscribe')
        async.run_unsubscribe(
            {"log_service_id": "1"}, 'dummy')
        with self.assertLogs('subscriptions:async', level='WARNING') as log:
            logging.getLogger('subscriptions:async').warning(
                'BadRequest on handler:unsubscribe')
            self.assertEqual(
                log.output[0],
                'WARNING:subscriptions:async:BadRequest on handler:unsubscribe')
        async.run_unsubscribe(
            {"device_id": "ffffffff-ffff-ffff-ffff-ffffff000012"},
            'dummy')
        with self.assertLogs('subscriptions:async', level='WARNING') as log:
            logging.getLogger('subscriptions:async').warning(
                'BadRequest on handler:unsubscribe')
            self.assertEqual(
                log.output[0],
                'WARNING:subscriptions:async:BadRequest on handler:unsubscribe')

    @patch('boc.base.Base.post_content')
    def test_unsubscribe_success(self, mock):
        mock.return_value = {
            'success': True,
            'code': 200,
            'message': 'Success.'}
        before = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000012#0')
        self.assertEqual(len(before['Items'][0]['oids']), 4)
        self.assertEqual(int(before['Items'][0]['status']), 2202)
        async.run_unsubscribe({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000012",
            "log_service_id": "0"}, 'dummy')
        mock.assert_called()
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000012#0')
        self.assertEqual(len(after['Items']), 1)
        self.assertEqual(int(after['Items'][0]['status']), 2200)

    @patch('boc.base.Base.post_content')
    def test_unsubscribe_non_existing_device(self, mock):
        before = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff0wrong#0')
        self.assertEqual(len(before['Items']), 0)
        async.run_unsubscribe({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff0wrong",
            "log_service_id": "0", "time_period": 1}, 'dummy')
        mock.assert_not_called()
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff0wrong#0')
        self.assertEqual(len(after['Items']), 0)

    @patch('boc.base.Base.post_content')
    def test_unsubscribe_non_existing_log_service_id(self, mock):
        before = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000011#1')
        self.assertEqual(len(before['Items']), 0)
        async.run_unsubscribe({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000011",
            "log_service_id": "1", "time_period": 1}, 'dummy')
        mock.assert_not_called()
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000011#1')
        self.assertEqual(len(after['Items']), 0)

    @patch('boc.base.Base.post_content')
    def test_unsubscribe_device_not_found(self, mock):
        mock.return_value = {
            'success': False,
            'code': 505,
            'message': 'Device not recognized.'}
        before = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000012#0')
        self.assertEqual(len(before['Items'][0]['oids']), 4)
        self.assertEqual(int(before['Items'][0]['status']), 2202)
        async.run_unsubscribe({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000012",
            "log_service_id": "0"}, 'dummy')
        mock.assert_called()
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000012#0')
        self.assertEqual(len(after['Items']), 1)
        self.assertEqual(int(after['Items'][0]['status']), 2200)

    @patch('boc.base.Base.post_content')
    def test_unsubscribe_unknown_error(self, mock):
        mock.return_value = {
            'success': False,
            'code': 999,
            'message': 'Unknown error.'}
        before = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000012#0')
        self.assertEqual(len(before['Items'][0]['oids']), 4)
        self.assertEqual(int(before['Items'][0]['status']), 2202)
        async.run_unsubscribe({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000012",
            "log_service_id": "0"}, 'dummy')
        mock.assert_called()
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000012#0')
        self.assertEqual(len(after['Items'][0]['oids']), 4)
        self.assertEqual(int(after['Items'][0]['status']), 2999)

    @patch('boc.base.Base.post_content')
    def test_unsubscribe_partial_error(self, mock):
        mock.return_value = {
            'success': True,
            'code': 207,
            'message': 'Partial success.',
            'unsubscribe': [{
                'error_code': '200',
                'object_id': '1.3.6.1.2.1.1.4.0',
                'message': 'No error.'},
                {
                'error_code': '404',
                'object_id': '1.3.6.1.2.1.1.6.0',
                'message': 'No such OID'},
                {
                'error_code': '464',
                'object_id': '1.3.6.1.2.1.2.2.1.6.1',
                'message': 'Not subscribed from service'},
                {
                'error_code': '500',
                'object_id': '1.3.6.1.2.1.25.3.2.1.3.1',
                'message': 'Other error.'}]}

        before = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000012#0')
        self.assertEqual(len(before['Items'][0]['oids']), 4)
        self.assertEqual(int(before['Items'][0]['status']), 2202)
        async.run_unsubscribe({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000012",
            "log_service_id": "0"}, 'dummy')
        mock.assert_called()
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000012#0')
        self.assertEqual(len(after['Items'][0]['oids']), 4)
        self.assertEqual(int(after['Items'][0]['status']), 2207)

    @patch('boc.base.Base.post_content')
    def test_unsubscribe_partial_success(self, mock):
        mock.return_value = {
            'success': True,
            'code': 207,
            'message': 'Partial success.',
            'unsubscribe': [{
                'error_code': '200',
                'object_id': '1.3.6.1.2.1.1.4.0',
                'message': 'No error.'},
                {
                'error_code': '404',
                'object_id': '1.3.6.1.2.1.1.6.0',
                'message': 'No such OID'},
                {
                'error_code': '464',
                'object_id': '1.3.6.1.2.1.2.2.1.6.1',
                'message': 'Not subscribed from service'},
                {
                'error_code': '210',
                'object_id': '1.3.6.1.2.1.25.3.2.1.3.1',
                'message': 'Success, but the device is offline'}]}

        before = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000012#0')
        self.assertEqual(len(before['Items'][0]['oids']), 4)
        self.assertEqual(int(before['Items'][0]['status']), 2202)
        async.run_unsubscribe({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000012",
            "log_service_id": "0"}, 'dummy')
        mock.assert_called()
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000012#0')
        self.assertEqual(len(after['Items']), 1)
        self.assertEqual(int(after['Items'][0]['status']), 2200)

    @patch('boc.base.Base.post_content')
    def test_unsubscribe_success_but_device_offline(self, mock):
        mock.return_value = {
            'success': True,
            'code': 210,
            'message': 'Success, but the device is offline'}

        before = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000012#0')
        self.assertEqual(len(before['Items'][0]['oids']), 4)
        self.assertEqual(int(before['Items'][0]['status']), 2202)
        async.run_unsubscribe({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000012",
            "log_service_id": "0"}, 'dummy')
        mock.assert_called()
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000012#0')
        self.assertEqual(len(after['Items'][0]['oids']), 4)
        self.assertEqual(int(after['Items'][0]['status']), 2210)


class AsyncNotifyResultTestCase(unittest.TestCase):
    def setUp(self):
        test_helper.set_env_var(self)
        self.path = path.dirname(__file__)
        test_helper.seed_ddb_subscriptions(self)
        test_helper.seed_ec_subscriptions(self)
        self.mock_context = MagicMock()
        self.mock_context.aws_request_id = 'mock_aws_request_id'
        logging.getLogger('subscriptions:async').setLevel(100)

    def tearDown(self):
        test_helper.clear_db(self)
        test_helper.clear_cache(self)
        test_helper.create_table(self)

    def test_bad_request(self):
        async.run_get_notify_result({}, self.mock_context)
        with self.assertLogs('subscriptions:async', level='WARNING') as log:
            logging.getLogger('subscriptions:async').warning(
                'BadRequest on handler:run_get_notify_result')
            self.assertEqual(
                log.output[0],
                'WARNING:subscriptions:async:BadRequest on handler:run_get_notify_result')
        async.run_get_notify_result(
            {"log_service_id": "1"}, self.mock_context)
        with self.assertLogs('subscriptions:async', level='WARNING') as log:
            logging.getLogger('subscriptions:async').warning(
                'BadRequest on handler:run_get_notify_result')
            self.assertEqual(
                log.output[0],
                'WARNING:subscriptions:async:BadRequest on handler:run_get_notify_result')
        async.run_get_notify_result(
            {"device_id": "ffffffff-ffff-ffff-ffff-ffffff000012"},
            self.mock_context)
        with self.assertLogs('subscriptions:async', level='WARNING') as log:
            logging.getLogger('subscriptions:async').warning(
                'BadRequest on handler:run_get_notify_result')
            self.assertEqual(
                log.output[0],
                'WARNING:subscriptions:async:BadRequest on handler:run_get_notify_result')

    @patch('boc.base.Base.post_content')
    def test_notify_online(self, mock):
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
                 {'error_code': '524',
                  'object_id': '1.3.6.1.2.1.2.2.1.6.1',
                  'status': '',
                  'user_id': '184878',
                  'timestamp': '0'},
                 {'error_code': '200',
                  'object_id': '1.3.6.1.2.1.25.3.2.1.3.1',
                  'status': '70726F78792E62726F746865722E636F2E6A70',
                  'user_id': '184878',
                  'timestamp': '2017-06-30 07:09:00'}]}
        before = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000014#0')
        self.assertEqual(len(before['Items'][0]['oids']), 4)
        self.assertEqual(int(before['Items'][0]['status']), 1201)
        async.run_get_notify_result({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000014",
            "log_service_id": "0"}, self.mock_context)
        mock.assert_called()
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000014#0')
        self.assertEqual(len(after['Items'][0]['oids']), 3)
        self.assertEqual(int(after['Items'][0]['status']), 1200)

    @patch('boc.base.Base.post_content')
    def test_notify_offline(self, mock):
        mock.return_value = {
            'success': True,
            'message': 'Success.',
            'code': 200,
            'notifications':
                [{'error_code': '404',
                  'object_id': '1.3.6.1.2.1.1.4.0',
                  'status': '0',
                  'user_id': '184878',
                  'timestamp': '2017-06-30 07:09:00'},
                 {'error_code': '404',
                  'object_id': '1.3.6.1.2.1.1.6.0',
                  'status': '0',
                  'user_id': '184878',
                  'timestamp': '2017-06-30 07:09:00'},
                 {'error_code': '404',
                  'object_id': '1.3.6.1.2.1.2.2.1.6.1',
                  'status': '0',
                  'user_id': '184878',
                  'timestamp': '2017-06-30 07:09:00'},
                 {'error_code': '404',
                  'object_id': '1.3.6.1.2.1.25.3.2.1.3.1',
                  'status': '0',
                  'user_id': '184878',
                  'timestamp': '2017-06-30 07:09:00'}]}
        before = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000014#0')
        self.assertEqual(len(before['Items'][0]['oids']), 4)
        self.assertEqual(int(before['Items'][0]['status']), 1201)
        async.run_get_notify_result({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000014",
            "log_service_id": "0"}, self.mock_context)
        mock.assert_called()
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000014#0')
        self.assertEqual(len(after['Items'][0]['oids']), 4)
        self.assertEqual(int(after['Items'][0]['status']), 1201)

    @patch('boc.base.Base.post_content')
    def test_notify_error(self, mock):
        mock.return_value = {
            'success': False,
            'message': 'Unknown Error',
            'code': 999}
        before = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000014#0')
        self.assertEqual(len(before['Items'][0]['oids']), 4)
        self.assertEqual(int(before['Items'][0]['status']), 1201)
        async.run_get_notify_result({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000014",
            "log_service_id": "0"}, self.mock_context)
        mock.assert_called()
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000014#0')
        self.assertEqual(len(after['Items'][0]['oids']), 4)
        self.assertEqual(int(after['Items'][0]['status']), 1201)

    @patch('boc.base.Base.post_content')
    def test_notify_duplicate_request(self, mock):
        environ['REDIS_ENDPOINT_URL'] = ''
        mock.return_value = {
            'success': True,
            'message': 'Success.',
            'code': 200,
            'notifications':
                [{'error_code': '200',
                  'object_id': '1.3.6.1.2.1.1.4.0',
                  'status': '70726F78792E62726F746865722E636F2E6A70',
                  'user_id': '184878',
                  'timestamp': '2017-06-30 07:09:00'}]}
        before = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000014#0')
        self.assertEqual(int(before['Items'][0]['status']), 1201)
        async.run_get_notify_result({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000014",
            "log_service_id": "0"}, self.mock_context)
        mock.assert_called()
        mock.reset_mock()
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000014#0')
        self.assertEqual(int(after['Items'][0]['status']), 1200)
        self.assertNotEqual(before['Items'][0]['updated_at'], after['Items'][0]['updated_at'])
        self.assertNotEqual(before['Items'][0]['latest_async_id'], after['Items'][0]['latest_async_id'])
        async.run_get_notify_result({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000014",
            "log_service_id": "0"}, self.mock_context)
        mock.assert_not_called()
        duplicate = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000014#0')
        self.assertEqual(after['Items'][0]['updated_at'], duplicate['Items'][0]['updated_at'])
        self.assertEqual(after['Items'][0]['latest_async_id'], duplicate['Items'][0]['latest_async_id'])

    @patch('boc.base.Base.post_content')
    def test_notify_duplicate_request_with_error(self, mock):
        environ['REDIS_ENDPOINT_URL'] = ''
        mock.return_value = {
            'success': False,
            'message': 'Unknown.',
            'code': 999
            }
        before = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000014#0')
        self.assertEqual(int(before['Items'][0]['status']), 1201)
        async.run_get_notify_result({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000014",
            "log_service_id": "0"}, self.mock_context)
        mock.assert_called()
        mock.reset_mock()
        error = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000014#0')
        self.assertEqual(int(before['Items'][0]['status']), int(error['Items'][0]['status']))
        self.assertEqual(before['Items'][0]['latest_async_id'], error['Items'][0]['latest_async_id'])
        mock.return_value = {
            'success': True,
            'message': 'Success.',
            'code': 200,
            'notifications':
                [{'error_code': '200',
                  'object_id': '1.3.6.1.2.1.1.4.0',
                  'status': '70726F78792E62726F746865722E636F2E6A70',
                  'user_id': '184878',
                  'timestamp': '2017-06-30 07:09:00'}]}
        async.run_get_notify_result({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000014",
            "log_service_id": "0"}, self.mock_context)
        mock.assert_called()
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000014#0')
        self.assertEqual(int(after['Items'][0]['status']), 1200)
        self.assertNotEqual(before['Items'][0]['updated_at'], after['Items'][0]['updated_at'])
        self.assertNotEqual(before['Items'][0]['latest_async_id'], after['Items'][0]['latest_async_id'])


def main():
    unittest.main()


if __name__ == '__main__':
    main()
