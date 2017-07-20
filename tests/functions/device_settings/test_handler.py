from boc.base import Base
from boc.device_info import DeviceInfo
from boc.exceptions import ParamsMissingError
from functions.device_settings.handler import get
from functions.device_settings.handler import set
import json
import logging
from os import path
from tests.functions import test_helper
from unittest.mock import patch
from unittest import TestCase
from urllib import error


class TestGetDeviceSettings(TestCase):
    def setUp(self):
        test_helper.set_env_var(self)
        self.path = path.dirname(__file__)
        test_helper.seed_ddb_device_settings(self)
        logging.getLogger('get_device_settings').setLevel(100)

    def tearDown(self):
        test_helper.clear_db(self)
        test_helper.create_table(self)

    @patch.object(DeviceInfo, 'get', return_value={"success": "true", "message": "Success.", "code": 200})
    def test_should_get_device_settings_successfully(self, mock_get):
        response = get(
            {"body": "{\"device_id\": \"da23bd9a-86da-2580-cefa-d05acfff7eb4\", \"setting\": [{\"object_id\": \"1.3.6.1.4.1.2435.2.4.3.2435.5.36.33.0\"}, {\"object_id\": \"1.3.6.1.4.1.2435.2.4.3.2435.10.1.12.1.3.1\"}]}"}, '')
        res_json = json.loads(response['body'])
        self.assertEqual(200, res_json['code'])

    @patch.object(DeviceInfo, 'get')
    def test_missing_device_id_on_get_settings(self, mock_get):
        mock_get.side_effect = ParamsMissingError('Missing field device_id')
        response = get(
            {"body": "{\"device_id\": \"\", \"setting\": [{\"object_id\": \"1.3.6.1.4.1.2435.2.4.3.2435.5.36.33.0\"}, {\"object_id\": \"1.3.6.1.4.1.2435.2.4.3.2435.10.1.12.1.3.1\"}]}"}, '')
        self.assertEqual(503, mock_get.side_effect.code)
        res_json = json.loads(response['body'])
        self.assertEqual(503, res_json['code'])

    @patch.object(DeviceInfo, 'get')
    def test_missing_object_id_on_get_settings(self, mock_get):
        mock_get.side_effect = ParamsMissingError('Missing field object_id')
        response = get(
            {"body": "{\"device_id\": \"da23bd9a-86da-2580-cefa-d05acfff7eb4\", \"setting\": [{\"object_id\": \"\"}, {\"object_id\": \"1.3.6.1.4.1.2435.2.4.3.2435.10.1.12.1.3.1\"}]}"}, '')
        self.assertEqual(503, mock_get.side_effect.code)
        res_json = json.loads(response['body'])
        self.assertEqual(503, res_json['code'])

    @patch.object(Base, 'post_content')
    def test_boc_connection_error_on_get_settings(self, mock_post_content):
        mock_post_content.side_effect = error.HTTPError(
            '', 404, 'Not Found', '', '')
        response = get(
            {"body": "{\"device_id\": \"da23bd9a-86da-2580-cefa-d05acfff7eb4\", \"setting\": [{\"object_id\": \"1.3.6.1.4.1.2435.2.4.3.2435.5.36.33.0\"}, {\"object_id\": \"1.3.6.1.4.1.2435.2.4.3.2435.10.1.12.1.3.1\"}]}"}, '')
        self.assertEqual(404, mock_post_content.side_effect.code)
        res_json = json.loads(response['body'])
        self.assertEqual(563, res_json['code'])

    @patch.object(Base, 'post_content')
    def test_json_decode_response_error_on_get_settings(self, mock_post_content):
        mock_post_content.side_effect = ValueError
        response = get(
            {"body": "{\"device_id\": \"da23bd9a-86da-2580-cefa-d05acfff7eb4\", \"setting\": [{\"object_id\": \"1.3.6.1.4.1.2435.2.4.3.2435.5.36.33.0\"}, {\"object_id\": \"1.3.6.1.4.1.2435.2.4.3.2435.10.1.12.1.3.1\"}]}"}, '')
        res_json = json.loads(response['body'])
        self.assertEqual(560, res_json['code'])

    def test_invalid_log_service_id_on_get_settings(self):
        response = get(
            {"body": "{\"log_service_id\": \"Invalid\", \"device_id\": \"da23bd9a-86da-2580-cefa-d05acfff7eb4\", \"setting\": [{\"object_id\": \"1.3.6.1.4.1.2435.2.4.3.2435.5.36.33.0\"}, {\"object_id\": \"1.3.6.1.4.1.2435.2.4.3.2435.10.1.12.1.3.1\"}]}"}, '')
        res_json = json.loads(response['body'])
        self.assertEqual(res_json['code'], 400)
        self.assertEqual(res_json['message'], 'Bad Request')

    @patch.object(DeviceInfo, 'get')
    def test_valid_log_service_id_on_get_settings_success(self, mock_get):
        mock_get.return_value={"success": "true", "message": "Success.", "code": 200}
        response = get(
            {"body": "{\"log_service_id\": \"0\", \"device_id\": \"da23bd9a-86da-2580-cefa-d05acfff7eb4\", \"setting\": [{\"object_id\": \"1.3.6.1.4.1.2435.2.4.3.2435.5.36.33.0\"}, {\"object_id\": \"1.3.6.1.4.1.2435.2.4.3.2435.10.1.12.1.3.1\"}]}"}, '')
        res_json = json.loads(response['body'])
        self.assertEqual(res_json['code'], 200)

class TestSetDeviceSettings(TestCase):
    def setUp(self):
        test_helper.set_env_var(self)
        self.path = path.dirname(__file__)
        test_helper.seed_ddb_device_settings(self)
        logging.getLogger('set_device_settings').setLevel(100)

    def tearDown(self):
        test_helper.clear_db(self)
        test_helper.create_table(self)

    @patch.object(DeviceInfo, 'set', return_value={"success": "true", "message": "Success.", "code": 200})
    def test_should_set_device_settings_successfully(self, mock_set):
        response = set(
            {"body": "{\"device_id\": \"da23bd9a-86da-2580-cefa-d05acfff7eb4\", \"setting\": [{\"object_id\": \"1.3.6.1.4.1.2435.2.4.3.2435.5.36.14.0\", \"value\": \"1\"}]}"}, '')
        res_json = json.loads(response['body'])
        self.assertEqual(200, res_json['code'])

    @patch.object(DeviceInfo, 'set')
    def test_missing_device_id_on_set_settings(self, mock_set):
        mock_set.side_effect = ParamsMissingError('Missing field device_id')
        response = set(
            {"body": "{\"device_id\": \"\", \"setting\": [{\"object_id\": \"1.3.6.1.4.1.2435.2.4.3.2435.5.36.14.0\", \"value\": \"1\"}]}"}, '')
        self.assertEqual(503, mock_set.side_effect.code)
        res_json = json.loads(response['body'])
        self.assertEqual(503, res_json['code'])

    @patch.object(DeviceInfo, 'set')
    def test_missing_object_id_on_set_settings(self, mock_set):
        mock_set.side_effect = ParamsMissingError('Missing field object_id')
        response = set(
            {"body": "{\"device_id\": \"da23bd9a-86da-2580-cefa-d05acfff7eb4\", \"setting\": [{\"object_id\": \"\", \"value\": \"1\"}]}"}, '')
        self.assertEqual(503, mock_set.side_effect.code)
        res_json = json.loads(response['body'])
        self.assertEqual(503, res_json['code'])

    @patch.object(Base, 'post_content')
    def test_boc_connection_error_on_set_settings(self, mock_post_content):
        mock_post_content.side_effect = error.HTTPError(
            '', 404, 'Not Found', '', '')
        response = set(
            {"body": "{\"device_id\": \"da23bd9a-86da-2580-cefa-d05acfff7eb4\", \"setting\": [{\"object_id\": \"1.3.6.1.4.1.2435.2.4.3.2435.5.36.14.0\", \"value\": \"1\"}]}"}, '')
        self.assertEqual(404, mock_post_content.side_effect.code)
        res_json = json.loads(response['body'])
        self.assertEqual(563, res_json['code'])

    @patch.object(Base, 'post_content')
    def test_json_decode_response_error_on_set_settings(self, mock_post_content):
        mock_post_content.side_effect = ValueError
        response = set(
            {"body": "{\"device_id\": \"da23bd9a-86da-2580-cefa-d05acfff7eb4\", \"setting\": [{\"object_id\": \"1.3.6.1.4.1.2435.2.4.3.2435.5.36.14.0\", \"value\": \"1\"}]}"}, '')
        res_json = json.loads(response['body'])
        self.assertEqual(560, res_json['code'])

    def test_invalid_log_service_id_on_set_settings(self):
        response = set(
            {"body": "{\"log_service_id\": \"Invalid\", \"device_id\": \"da23bd9a-86da-2580-cefa-d05acfff7eb4\", \"setting\": [{\"object_id\": \"1.3.6.1.4.1.2435.2.4.3.2435.5.36.14.0\", \"value\": \"1\"}]}"}, '')
        res_json = json.loads(response['body'])
        self.assertEqual(res_json['code'], 400)
        self.assertEqual(res_json['message'], 'Bad Request')

    @patch.object(DeviceInfo, 'set')
    def test_valid_log_service_id_on_set_settings_success(self, mock_set):
        mock_set.return_value={"success": "true", "message": "Success.", "code": 200}
        response = set(
            {"body": "{\"log_service_id\": \"0\", \"device_id\": \"da23bd9a-86da-2580-cefa-d05acfff7eb4\", \"setting\": [{\"object_id\": \"1.3.6.1.4.1.2435.2.4.3.2435.5.36.14.0\", \"value\": \"1\"}]}"}, '')
        res_json = json.loads(response['body'])
        self.assertEqual(200, res_json['code'])
