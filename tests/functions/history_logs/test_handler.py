from botocore.exceptions import ConnectionError
from functions.history_logs import handler
import json
import logging
from models.device_log import DeviceLog
from os import path
from tests.functions import test_helper
import unittest
from unittest.mock import patch


class TestGetHistoryLogs(unittest.TestCase):
    def setUp(self):
        test_helper.set_env_var(self)
        self.path = path.dirname(__file__)
        test_helper.seed_ddb_history_logs(self)
        logging.getLogger('get_history_logs').setLevel(100)

    def tearDown(self):
        test_helper.clear_db(self)
        test_helper.create_table(self)

    def test_simple_bad_request_on_get_history_logs(self):
        output = handler.get_history_logs(
            {'body': json.dumps({})}, 'dummy')
        self.assertEqual(json.loads(output['body'])['code'], 400)
        output = handler.get_history_logs(
            {'body': json.dumps({"device_id": ''})}, 'dummy'
        )
        self.assertEqual(json.loads(output['body'])['code'], 400)
        output = handler.get_history_logs(
            {'body': json.dumps({"invalid_parameter": ''})}, 'dummy'
        )
        self.assertEqual(json.loads(output['body'])['code'], 400)
        response = handler.get_history_logs(
            {"body": "{\"device_id\": \"da23bd9a-86da-2580-cefa-d05acfff7eb4\\\"}"}, 'dummy')
        output = json.loads(response['body'])
        self.assertEqual(output['code'], 400)
        self.assertEqual(output['message'], "Request Body has incorrect format")

    def test_bad_request_params_missing_on_get_history_logs(self):
        with open(
                f'{self.path}/../../data/history_logs/bad_requests/get_history_logs_params_missing.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_logs({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 400)
        self.assertEqual(output['message'],
                         "Parameters Missing: ['time_unit']")
        self.assertFalse(output['data'])

    def test_bad_request_incorrect_time_format_on_get_history_logs(self):
        with open(
                f'{self.path}/../../data/history_logs/bad_requests/get_history_logs_incorrect_from_time_format.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_logs({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 400)
        self.assertEqual(
            output['message'], f"Parameter 'from' has incorrect value: {json.loads(input)['from']}")
        self.assertTrue(output['device_id'])
        self.assertFalse(output['data'])

        with open(
                f'{self.path}/../../data/history_logs/bad_requests/get_history_logs_incorrect_to_time_format.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_logs({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 400)
        self.assertEqual(
            output['message'], f"Parameter 'to' has incorrect value: {json.loads(input)['to']}")
        self.assertTrue(output['device_id'])
        self.assertFalse(output['data'])

        with open(
                f'{self.path}/../../data/history_logs/bad_requests/get_history_logs_time_parameter_not_a_string.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_logs({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 400)
        self.assertEqual(
            output['message'], f"Parameter 'from' has incorrect value: {json.loads(input)['from']}")
        self.assertTrue(output['device_id'])
        self.assertFalse(output['data'])


    def test_bad_request_from_value_larger_than_to_value_on_get_history_logs(self):
        with open(
                f'{self.path}/../../data/history_logs/bad_requests/get_history_logs_from_value_larger_than_to_value.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_logs({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 400)
        self.assertEqual(
            output['message'], "Parameter 'from' should be less than parameter 'to'")
        self.assertTrue(output['device_id'])
        self.assertFalse(output['data'])

    def test_bad_request_time_unit_incorrect_value_on_get_history_logs(self):
        with open(
                f'{self.path}/../../data/history_logs/bad_requests/get_history_logs_time_unit_incorrect_value.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_logs({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 400)
        self.assertEqual(
            output['message'], f"Parameter 'time_unit' has incorrect value: {json.loads(input)['time_unit']}")
        self.assertTrue(output['device_id'])
        self.assertFalse(output['data'])

    def test_bad_request_features_not_a_list_or_string_on_get_history_logs(self):
        with open(
                f'{self.path}/../../data/history_logs/bad_requests/get_history_logs_features_not_a_list_or_string.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_logs({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 400)
        self.assertEqual(output['message'],
                         "Parameter 'features' should be a list or string")
        self.assertTrue(output['device_id'])
        self.assertFalse(output['data'])

    def test_bad_request_device_id_invalid_format_on_get_history_logs(self):
        with open(
                f'{self.path}/../../data/history_logs/bad_requests/get_history_logs_device_id_invalid_format.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_logs({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 400)
        self.assertEqual(
            output['message'], f"Parameter 'device_id' = '{json.loads(input)['device_id']}' has incorrect format")
        self.assertFalse(output['data'])

        with open(
                f'{self.path}/../../data/history_logs/bad_requests/get_history_logs_device_id_parameter_as_list.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_logs({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 400)
        self.assertEqual(
            output['message'], f"Parameter 'device_id' = '{json.loads(input)['device_id']}' has incorrect format")
        self.assertFalse(output['data'])

    def test_bad_request_device_id_value_empty_on_get_history_logs(self):
        with open(
                f'{self.path}/../../data/history_logs/bad_requests/get_history_logs_device_id_value_empty.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_logs({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 400)
        self.assertEqual(
            output['message'], f"Parameter 'device_id' = '{json.loads(input)['device_id']}' has incorrect format")
        self.assertFalse(output['data'])

    def test_bad_request_log_service_id_invalid_value_on_get_history_logs(self):
        with open(
                f'{self.path}/../../data/history_logs/bad_requests/get_history_logs_log_service_id_invalid_value.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_logs({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 400)
        self.assertEqual(
            output['message'], f"Parameter 'log_service_id' has invalid value: {json.loads(input)['log_service_id']}")
        self.assertTrue(output['device_id'])
        self.assertFalse(output['data'])

    def test_device_not_found_on_get_history_logs(self):
        with open(
                f'{self.path}/../../data/history_logs/bad_requests/get_history_logs_device_not_found.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_logs({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 404)
        self.assertEqual(
            output['message'], "Device Not Found")
        self.assertEqual(output['device_id'], json.loads(input)['device_id'])
        self.assertEqual(output['data'], [])

    def test_simple_success_on_get_history_logs(self):
        with open(
                f'{self.path}/../../data/history_logs/success/get_history_logs_success.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_logs({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 200)
        self.assertEqual(output['message'], 'Success')
        self.assertTrue(output['data'])
        self.assertTrue(output['device_id'])
        for time in output['data'][0]['updated']:
            self.assertTrue(DeviceLog.parse_time(self, time))

    def test_success_for_integer_and_default_log_service_id_on_get_history_logs(self):
        with open(
                f'{self.path}/../../data/history_logs/success/get_history_logs_success_for_integer_log_service_id.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_logs({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 200)
        self.assertEqual(output['message'], 'Success')
        self.assertTrue(output['data'])

        with open(
                f'{self.path}/../../data/history_logs/success/get_history_logs_success_for_default_log_service_id.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_logs({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 200)
        self.assertEqual(output['message'], 'Success')
        self.assertTrue(output['data'])

    def test_success_parameter_features_is_string_on_get_history_logs(self):
        with open(
                f'{self.path}/../../data/history_logs/success/get_history_logs_success_parameter_features_is_string.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_logs({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 207)
        self.assertEqual(output['message'], 'Partial Success')
        self.assertEqual(len(output['data']), 3)

    def test_logs_not_found_on_get_history_logs(self):
        with open(
                f'{self.path}/../../data/history_logs/success/get_history_logs_logs_not_found.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_logs({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 204)
        self.assertTrue(output['device_id'])
        for feature in output['data']:
            self.assertEqual(feature['error_code'], 204)
            self.assertFalse('value' in feature)
            self.assertFalse('updated' in feature)

    def test_all_features_not_found_on_get_history_logs(self):
        with open(
                f'{self.path}/../../data/history_logs/bad_requests/get_history_logs_all_features_not_found.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_logs({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 400)
        self.assertEqual(output['message'], 'Features Not Found')
        for feature in output['data']:
            self.assertEqual(feature['error_code'], 404)
            self.assertFalse('value' in feature)
            self.assertFalse('updated' in feature)

    def test_few_features_not_found_on_get_history_logs(self):
        with open(
                f'{self.path}/../../data/history_logs/success/get_history_logs_few_features_not_found.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_logs({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 207)
        self.assertEqual(output['message'], 'Partial Success')
        not_found_count = 0
        for feature in output['data']:
            if feature['error_code'] == 404:
                not_found_count += 1
        self.assertTrue(not_found_count >= 1)

    def test_duplicate_features_on_get_history_logs(self):
        with open(
                f'{self.path}/../../data/history_logs/success/get_history_logs_duplicate_features.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_logs({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 200)
        self.assertEqual(output['message'], 'Success')
        self.assertTrue(len(output['data']) == 1)

    def test_few_parser_errors_on_get_history_logs(self):
        with open(
                f'{self.path}/../../data/history_logs/success/get_history_logs_few_parser_errors.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_logs({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 207)
        self.assertEqual(output['message'], 'Partial Success')
        empty_string_count = 0
        for feature in output['data']:
            for value in feature['value']:
                if value == '':
                    empty_string_count += 1
        self.assertTrue(empty_string_count >= 1)

    def test_all_parser_errors_on_get_history_logs(self):
        with open(
                f'{self.path}/../../data/history_logs/errors/get_history_logs_all_parser_errors.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_logs({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 560)
        self.assertEqual(output['message'], 'Error')
        for feature in output['data']:
            for value in feature['value']:
                self.assertEqual(value, '')
            self.assertEqual(feature['error_code'], 500)
            self.assertEqual(feature['message'], "Parser Error")

    def test_parser_errors_and_logs_not_found_on_get_history_logs(self):
        with open(
                f'{self.path}/../../data/history_logs/success/get_history_logs_parser_errors_and_logs_not_found.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_logs({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 207)
        self.assertEqual(output['message'], 'Partial Success')
        for feature in output['data']:
            self.assertTrue(feature['error_code'] in (204, 500))

    def test_multiple_features_one_oid_OK_on_get_history_logs(self):
        with open(
                f'{self.path}/../../data/history_logs/success/get_history_logs_multiple_features_one_oid_OK.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_logs({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 207)
        self.assertEqual(output['message'], 'Partial Success')
        for feature in output['data']:
            self.assertTrue(feature['error_code'] in (200, 204))

    def test_multiple_oids_OK_on_get_history_logs(self):
        with open(
                f'{self.path}/../../data/history_logs/success/get_history_logs_multiple_oids_OK.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_logs({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 207)
        self.assertEqual(output['message'], 'Partial Success')
        for feature in output['data']:
            self.assertTrue(feature['error_code'] in (200, 204, 214))

    @patch.object(DeviceLog, 'get_history_logs')
    def test_database_connection_error_on_get_history_logs(self, mock):
        mock.side_effect = ConnectionError
        with open(
                f'{self.path}/../../data/history_logs/success/get_history_logs_success.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_logs({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 561)
        self.assertEqual(output['message'], 'Failed to connect with DB')
        self.assertFalse(output['data'])

    def test_success_for_device_status_subscribed_offline_on_get_history_logs(self):
        with open(
                f'{self.path}/../../data/history_logs/success/get_history_logs_success_for_device_status_subscribed_offline.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_logs({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 200)
        self.assertEqual(output['message'], 'Success')
        self.assertTrue(output['data'] != [])
        self.assertEqual(len(output['data'][0]['value']), 2)
        self.assertEqual(len(output['data'][0]['updated']), 2)

    def test_success_for_unsubscribed_device_on_get_history_logs(self):
        with open(
                f'{self.path}/../../data/history_logs/success/get_history_logs_success_for_unsubscribed_device.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_logs({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 200)
        self.assertEqual(output['message'], 'Success')
        self.assertTrue(output['data'] != [])
        self.assertEqual(len(output['data'][0]['value']), 5)
        self.assertEqual(len(output['data'][0]['updated']), 5)

    def test_few_features_not_subscribed_on_get_history_logs(self):
        with open(
                f'{self.path}/../../data/history_logs/success/get_history_logs_few_features_not_subscribed.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_logs({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 207)
        self.assertEqual(output['message'], 'Partial Success')
        unsubscribed_features_count = 0
        for feature in output['data']:
            if feature['error_code'] == 214:
                unsubscribed_features_count += 1
        self.assertEqual(unsubscribed_features_count, 2)

    def test_all_features_not_subscribed_on_get_history_logs(self):
        with open(
                f'{self.path}/../../data/history_logs/success/get_history_logs_all_features_not_subscribed.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_logs({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 214)
        self.assertEqual(output['message'], 'Features Not Subscribed')
        for feature in output['data']:
            self.assertEqual(feature['error_code'], 214)
            self.assertFalse('value' in feature)
            self.assertFalse('updated' in feature)

    def test_adjust_features_values_in_response_on_get_history_logs(self):
        with open(
                f'{self.path}/../../data/history_logs/success/get_history_logs_adjust_features_values_in_response.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_logs({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 200)
        self.assertEqual(output['message'], 'Success')
        self.assertEqual(output['data'][0]['error_code'], 200)
        for val in output['data'][0]['value']:
            self.assertEqual(val, '32')

    def test_empty_string_oid_values_in_response_on_get_history_logs(self):
        with open(
                f'{self.path}/../../data/history_logs/success/get_history_logs_empty_string_oid_values_in_response.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_logs({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 200)
        self.assertEqual(output['message'], 'Success')
        self.assertEqual(output['data'][0]['error_code'], 200)
        for val in output['data'][0]['value']:
            self.assertEqual(val, '32')

    def test_charset_on_get_history_logs(self):
        with open(
                f'{self.path}/../../data/history_logs/errors/get_history_logs_charset_value_not_found_leading_to_parser_error.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_logs({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 560)
        self.assertEqual(output['message'], 'Error')
        self.assertEqual(output['data'][0]['error_code'], 500)
        for val in output['data'][0]['value']:
            self.assertFalse(val)
        for time in output['data'][0]['updated']:
            self.assertTrue(time)

        with open(
                f'{self.path}/../../data/history_logs/success/get_history_logs_charset_success.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_logs({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 200)
        self.assertEqual(output['message'], 'Success')
        self.assertEqual(output['data'][0]['error_code'], 200)
        for val in output['data'][0]['value']:
            self.assertTrue(val)
            self.assertEqual(type(val), str)
        for time in output['data'][0]['updated']:
            self.assertTrue(time)

    def test_device_subscription_record_has_no_oids_field_on_get_history_logs(self):
        with open(
                f'{self.path}/../../data/history_logs/success/get_history_logs_device_subscription_record_has_no_oids_field.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_logs({'body': input}, 'dummy')
        output = json.loads(output['body'])
        # Should not be device not found as device record exists in fixtures or all unsupported features
        self.assertTrue(output['code'] not in (404, 214))
        for feature in output['data']:
            # Should not return response 'unsupported feature' as can't detect
            self.assertTrue(feature['error_code'] != 214)
            self.assertEqual(feature['error_code'], 204)
        self.assertEqual(output['code'], 204)
        self.assertEqual(output['message'], 'Logs Not Found')
