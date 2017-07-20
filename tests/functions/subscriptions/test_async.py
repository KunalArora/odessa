import unittest
from unittest.mock import patch
from os import path
import logging
from functions.subscriptions import async
from tests.functions import test_helper


class AsyncSubscribeTestCase(unittest.TestCase):
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
        async.run_subscribe({}, 'dummy')
        with self.assertLogs('subscriptions:async', level='WARNING') as log:
            logging.getLogger('subscriptions:async').warning(
                'BadRequest on handler:subscribe')
            self.assertEqual(
                log.output[0],
                'WARNING:subscriptions:async:BadRequest on handler:subscribe')
        async.run_subscribe({"time_period": 30}, 'dummy')
        with self.assertLogs('subscriptions:async', level='WARNING') as log:
            logging.getLogger('subscriptions:async').warning(
                'BadRequest on handler:subscribe')
            self.assertEqual(
                log.output[0],
                'WARNING:subscriptions:async:BadRequest on handler:subscribe')
        async.run_subscribe(
            {"time_period": 30,
             "device_id": "ffffffff-ffff-ffff-ffff-ffffff0wrong"},
            'dummy')
        with self.assertLogs('subscriptions:async', level='WARNING') as log:
            logging.getLogger('subscriptions:async').warning(
                'BadRequest on handler:subscribe')
            self.assertEqual(
                log.output[0],
                'WARNING:subscriptions:async:BadRequest on handler:subscribe')
        async.run_subscribe(
            {"time_period": 30, "device_id": []}, 'dummy')
        with self.assertLogs('subscriptions:async', level='WARNING') as log:
            logging.getLogger('subscriptions:async').warning(
                'BadRequest on handler:subscribe')
            self.assertEqual(
                log.output[0],
                'WARNING:subscriptions:async:BadRequest on handler:subscribe')
        async.run_subscribe({"device_id": []}, 'dummy')
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
            'message': 'Success.'
        }

        before = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000011#0')
        self.assertEqual(len(before['Items']), 4)
        for subscription in before['Items']:
            self.assertEqual(int(subscription['status']), 1202)
        async.run_subscribe({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000011",
            "log_service_id": "0", "time_period": 1}, 'dummy')
        mock.assert_called_with(
            'https://dev-connections.mysora.net/svc_api/devices/subscribe',
            {'service_id': '2',
             'device_id': 'ffffffff-ffff-ffff-ffff-ffffff000011',
             'subscription_info[0][object_id]': '1.3.6.1.2.1.25.3.2.1.3.1',
             'subscription_info[0][time_period]': 30,
             'subscription_info[1][object_id]': '1.3.6.1.2.1.2.2.1.6.1',
             'subscription_info[1][time_period]': 30,
             'subscription_info[2][object_id]': '1.3.6.1.2.1.1.6.0',
             'subscription_info[2][time_period]': 30,
             'subscription_info[3][object_id]': '1.3.6.1.2.1.1.4.0',
             'subscription_info[3][time_period]': 30,
             'callback': 'http://dummy.com',
             'is_fwd': 'true'}, 300)
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000011#0')
        self.assertEqual(len(after['Items']), 4)
        for subscription in after['Items']:
            self.assertEqual(int(subscription['status']), 1200)

    @patch('boc.base.Base.post_content')
    def test_subscribe_non_existing_device(self, mock):
        before = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff0wrong#0')
        self.assertEqual(len(before['Items']), 0)
        async.run_subscribe({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff0wrong",
            "log_service_id": "0", "time_period": 1}, 'dummy')
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
            "log_service_id": "1", "time_period": 1}, 'dummy')
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
        self.assertEqual(len(before['Items']), 4)
        for subscription in before['Items']:
            self.assertEqual(int(subscription['status']), 1202)
        async.run_subscribe({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000011",
            "log_service_id": "0", "time_period": 1000}, 'dummy')
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
             'subscription_info[3][time_period]': 18000}, 300)
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000011#0')
        self.assertEqual(len(after['Items']), 4)
        for subscription in after['Items']:
            self.assertEqual(int(subscription['status']), 1200)

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
        self.assertEqual(len(before['Items']), 4)
        for subscription in before['Items']:
            self.assertEqual(int(subscription['status']), 1202)
        async.run_subscribe({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000011",
            "log_service_id": "0", "time_period": "45"}, 'dummy')
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
             'subscription_info[3][time_period]': 3600}, 300)
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000011#0')
        self.assertEqual(len(after['Items']), 3)
        for subscription in after['Items']:
            self.assertEqual(int(subscription['status']), 1200)

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
        self.assertEqual(len(before['Items']), 4)
        for subscription in before['Items']:
            self.assertEqual(int(subscription['status']), 1202)
        async.run_subscribe({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000011",
            "log_service_id": "0", "time_period": 45}, 'dummy')
        mock.assert_called()
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000011#0')
        self.assertEqual(len(after['Items']), 3)
        for subscription in after['Items']:
            self.assertEqual(int(subscription['status']), 1207)

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
        self.assertEqual(len(before['Items']), 4)
        for subscription in before['Items']:
            self.assertEqual(int(subscription['status']), 1202)
        async.run_subscribe({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000011",
            "log_service_id": "0", "time_period": 45}, 'dummy')
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
             'subscription_info[3][time_period]': 2700}, 300)
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000011#0')
        self.assertEqual(len(after['Items']), 4)
        for subscription in after['Items']:
            self.assertEqual(int(subscription['status']), 1500)

    @patch('boc.base.Base.post_content')
    def test_subscribe_unknown_error(self, mock):
        mock.return_value = {
            'success': False,
            'code': 999,
            'message': 'Unknown.'}
        before = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000011#0')
        self.assertEqual(len(before['Items']), 4)
        for subscription in before['Items']:
            self.assertEqual(int(subscription['status']), 1202)
        async.run_subscribe({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000011",
            "log_service_id": "0", "time_period": 45}, 'dummy')
        mock.assert_called()
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000011#0')
        self.assertEqual(len(after['Items']), 4)
        for subscription in after['Items']:
            self.assertEqual(int(subscription['status']), 1999)

    @patch('boc.base.Base.post_content')
    def test_subscribe_offline_device(self, mock):
        mock.return_value = {
            'success': True,
            'code': 210,
            'message': 'Success but device offline'}
        before = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000011#0')
        self.assertEqual(len(before['Items']), 4)
        for subscription in before['Items']:
            self.assertEqual(int(subscription['status']), 1202)
        async.run_subscribe({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000011",
            "log_service_id": "0", "time_period": 45}, 'dummy')
        mock.assert_called()
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000011#0')
        self.assertEqual(len(after['Items']), 4)
        for subscription in after['Items']:
            self.assertEqual(int(subscription['status']), 1201)

    @patch('boc.base.Base.post_content')
    def test_subscribe_unrecognized_device(self, mock):
        mock.return_value = {
            'success': False,
            'code': 505,
            'message': 'Device not recognized'}
        before = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000011#0')
        self.assertEqual(len(before['Items']), 4)
        for subscription in before['Items']:
            self.assertEqual(int(subscription['status']), 1202)
        async.run_subscribe({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000011",
            "log_service_id": "0", "time_period": 45}, 'dummy')
        mock.assert_called()
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000011#0')
        self.assertEqual(len(after['Items']), 4)
        for subscription in after['Items']:
            self.assertEqual(int(subscription['status']), 404)


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
        self.assertEqual(len(before['Items']), 4)
        for subscription in before['Items']:
            self.assertEqual(int(subscription['status']), 2202)
        async.run_unsubscribe({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000012",
            "log_service_id": "0"}, 'dummy')
        mock.assert_called()
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000012#0')
        self.assertEqual(len(after['Items']), 0)

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
        self.assertEqual(len(before['Items']), 4)
        for subscription in before['Items']:
            self.assertEqual(int(subscription['status']), 2202)
        async.run_unsubscribe({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000012",
            "log_service_id": "0"}, 'dummy')
        mock.assert_called()
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000012#0')
        self.assertEqual(len(after['Items']), 0)

    @patch('boc.base.Base.post_content')
    def test_unsubscribe_unknown_error(self, mock):
        mock.return_value = {
            'success': False,
            'code': 999,
            'message': 'Unknown error.'}
        before = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000012#0')
        self.assertEqual(len(before['Items']), 4)
        for subscription in before['Items']:
            self.assertEqual(int(subscription['status']), 2202)
        async.run_unsubscribe({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000012",
            "log_service_id": "0"}, 'dummy')
        mock.assert_called()
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000012#0')
        self.assertEqual(len(after['Items']), 4)
        for subscription in after['Items']:
            self.assertEqual(int(subscription['status']), 2999)

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
        self.assertEqual(len(before['Items']), 4)
        for subscription in before['Items']:
            self.assertEqual(int(subscription['status']), 2202)
        async.run_unsubscribe({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000012",
            "log_service_id": "0"}, 'dummy')
        mock.assert_called()
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000012#0')
        self.assertEqual(len(after['Items']), 4)
        for subscription in after['Items']:
            self.assertEqual(int(subscription['status']), 2207)

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
        self.assertEqual(len(before['Items']), 4)
        for subscription in before['Items']:
            self.assertEqual(int(subscription['status']), 2202)
        async.run_unsubscribe({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000012",
            "log_service_id": "0"}, 'dummy')
        mock.assert_called()
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000012#0')
        self.assertEqual(len(after['Items']), 0)

    @patch('boc.base.Base.post_content')
    def test_unsubscribe_success_but_device_offline(self, mock):
        mock.return_value = {
            'success': True,
            'code': 210,
            'message': 'Success, but the device is offline'}

        before = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000012#0')
        self.assertEqual(len(before['Items']), 4)
        for subscription in before['Items']:
            self.assertEqual(int(subscription['status']), 2202)
        async.run_unsubscribe({
            "device_id": "ffffffff-ffff-ffff-ffff-ffffff000012",
            "log_service_id": "0"}, 'dummy')
        mock.assert_called()
        after = test_helper.get_device(
            self, 'ffffffff-ffff-ffff-ffff-ffffff000012#0')
        self.assertEqual(len(after['Items']), 0)


def main():
    unittest.main()


if __name__ == '__main__':
    main()
