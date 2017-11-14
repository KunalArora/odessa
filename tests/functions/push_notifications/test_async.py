import unittest
import logging
from unittest.mock import patch, Mock
from os import path
from urllib import parse
from urllib.error import HTTPError
from functions.push_notifications import async
from tests.functions import test_helper


class AsyncSendPushNotificationTestCase(unittest.TestCase):
    def setUp(self):
        test_helper.set_env_var(self)
        self.path = path.dirname(__file__)
        logging.getLogger('push_notifications:async').setLevel(100)

    @patch('urllib.request.urlopen')
    @patch('functions.push_notifications.async.logger')
    def test_send_push_notification_success(self, mock_logger, mock):
        url_read_mock = Mock()
        url_read_mock.code = 200
        mock.return_value = url_read_mock
        async.send_push_notification({
            "reporting_id": "eeeeeeee-eeee-eeee-eeee-eeeeeeee0001",
            "object_id": "1.3.6.1.4.1.2435.2.3.9.4.2.1.5.5.8.0",
            "timestamp": "2017-02-04T12:23:01",
            "data": [{"feature_name": "TonerInk_LifeBlack", "value": "6000"}],
            "notify_url": "http://dummy.com"}, 'dummy')
        mock.assert_called_with(
            'http://dummy.com',
            parse.urlencode(
                {
                    "reporting_id": "eeeeeeee-eeee-eeee-eeee-eeeeeeee0001",
                    "object_id": "1.3.6.1.4.1.2435.2.3.9.4.2.1.5.5.8.0",
                    "timestamp": "2017-02-04T12:23:01",
                    "data": [{"feature_name": "TonerInk_LifeBlack", "value": "6000"}]
                }
            ).encode('ascii')
        )
        self.assertEqual(mock_logger.info.call_count, 2)
        mock_logger.info.assert_called_with('async:send_push_notification successfully sent notification to http://dummy.com.')
        mock_logger.error.assert_not_called()

    @patch('urllib.request.urlopen')
    @patch('functions.push_notifications.async.logger')
    def test_send_notification_server_error(self, mock_logger, mock):
        mock.side_effect = HTTPError('http://dummy.com', 500, 'Internal Server Error', 'test', Mock(return_value='test'))
        with self.assertRaises(HTTPError):
            async.send_push_notification({
                "reporting_id": "eeeeeeee-eeee-eeee-eeee-eeeeeeee0001",
                "object_id": "1.3.6.1.4.1.2435.2.3.9.4.2.1.5.5.8.0",
                "timestamp": "2017-02-04T12:23:01",
                "data": [{"feature_name": "TonerInk_LifeBlack", "value": "6000"}],
                "notify_url": "http://dummy.com"}, 'dummy')
        self.assertEqual(mock_logger.info.call_count, 1)
        mock_logger.error.assert_not_called()

    @patch('urllib.request.urlopen')
    @patch('functions.push_notifications.async.logger')
    def test_send_notification_client_error(self, mock_logger, mock):
        mock.side_effect = HTTPError('http://dummy.com', 404, 'Internal Server Error', 'test', Mock(return_value='test'))
        async.send_push_notification({
            "reporting_id": "eeeeeeee-eeee-eeee-eeee-eeeeeeee0001",
            "object_id": "1.3.6.1.4.1.2435.2.3.9.4.2.1.5.5.8.0",
            "timestamp": "2017-02-04T12:23:01",
            "data": [{"feature_name": "TonerInk_LifeBlack", "value": "6000"}],
            "notify_url": "http://dummy.com"}, 'dummy')
        self.assertEqual(mock_logger.info.call_count, 1)
        self.assertEqual(mock_logger.error.call_count, 2)


def main():
    unittest.main()


if __name__ == '__main__':
    main()
