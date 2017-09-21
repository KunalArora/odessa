from botocore.exceptions import ConnectionError
from functions.history_statuses import handler
from helpers import time_functions
import json
import logging
from models.device_network_status import DeviceNetworkStatus
from os import path
from tests.functions import test_helper
import unittest
from unittest.mock import patch


class TestGetHistoryStatuses(unittest.TestCase):
    def setUp(self):
        test_helper.set_env_var(self)
        self.path = path.dirname(__file__)
        test_helper.seed_ddb_history_statuses(self)
        logging.getLogger('get_history_statuses').setLevel(100)

    def tearDown(self):
        test_helper.clear_db(self)
        test_helper.create_table(self)

    def test_simple_bad_request_on_get_history_statuses(self):
        output = handler.get_history_statuses(
            {'body': json.dumps({})}, 'dummy')
        self.assertEqual(json.loads(output['body'])['code'], 400)
        output = handler.get_history_statuses(
            {'body': json.dumps({"device_id": ''})}, 'dummy'
        )
        self.assertEqual(json.loads(output['body'])['code'], 400)
        output = handler.get_history_statuses(
            {'body': json.dumps({"invalid_parameter": ''})}, 'dummy'
        )
        self.assertEqual(json.loads(output['body'])['code'], 400)
        response = handler.get_history_statuses(
            {"body": "{\"device_id\": \"da23bd9a-86da-2580-cefa-d05acfff7eb4\\\"}"}, 'dummy')
        output = json.loads(response['body'])
        self.assertEqual(output['code'], 400)
        self.assertEqual(output['message'],
                         "Request Body has incorrect format")

    def test_bad_request_params_missing_on_get_history_statuses(self):
        with open(
                f'{self.path}/../../data/history_statuses/bad_requests/'
                'get_history_statuses_params_missing.json'
        ) as data_file:
            input = json.load(data_file)
        input_device_id = json.dumps(input[0])
        output = handler.get_history_statuses(
            {'body': input_device_id}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 400)
        self.assertEqual(output['message'],
                         "Parameters Missing: from")
        self.assertFalse(output['data'])
        self.assertTrue('device_id' in output and output['device_id'])
        self.assertTrue('reporting_id' not in output)

        input_reporting_id = json.dumps(input[1])
        output = handler.get_history_statuses(
            {'body': input_reporting_id}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 400)
        self.assertEqual(output['message'],
                         "Parameters Missing: to")
        self.assertFalse(output['data'])
        self.assertTrue('device_id' not in output)
        self.assertTrue('reporting_id' in output and output['reporting_id'])

    def test_bad_request_device_id_and_reporting_id_missing_on_get_history_statuses(self):
        with open(
                f'{self.path}/../../data/history_statuses/bad_requests/'
                'get_history_statuses_device_id_and_reporting_id_missing.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_statuses({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 400)
        self.assertEqual(
            output['message'], f"Parameters Missing: reporting_id/device_id")
        self.assertFalse(output['data'])
        self.assertTrue('device_id' not in output)
        self.assertTrue('reporting_id' not in output)

    def test_bad_request_incorrect_time_format_on_get_history_statuses(self):
        with open(
                f'{self.path}/../../data/history_statuses/bad_requests/'
                'get_history_statuses_incorrect_from_time_format.json'
        ) as data_file:
            input = json.load(data_file)
        input_device_id = json.dumps(input[0])
        output = handler.get_history_statuses(
            {'body': input_device_id}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 400)
        self.assertEqual(
            output['message'], f"Parameter 'from' has incorrect value: {json.loads(input_device_id)['from']}")
        self.assertFalse(output['data'])
        self.assertTrue('device_id' in output and output['device_id'])
        self.assertTrue('reporting_id' not in output)

        input_reporting_id = json.dumps(input[1])
        output = handler.get_history_statuses(
            {'body': input_reporting_id}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 400)
        self.assertEqual(
            output['message'], f"Parameter 'from' has incorrect value: {json.loads(input_reporting_id)['from']}")
        self.assertTrue('device_id' not in output)
        self.assertTrue('reporting_id' in output and output['reporting_id'])

        with open(
                f'{self.path}/../../data/history_statuses/bad_requests/'
                'get_history_statuses_incorrect_to_time_format.json'
        ) as data_file:
            input = json.load(data_file)
        input_device_id = json.dumps(input[0])
        output = handler.get_history_statuses(
            {'body': input_device_id}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 400)
        self.assertEqual(
            output['message'], f"Parameter 'to' has incorrect value: {json.loads(input_device_id)['to']}")
        self.assertFalse(output['data'])
        self.assertTrue('device_id' in output and output['device_id'])
        self.assertTrue('reporting_id' not in output)

        input_reporting_id = json.dumps(input[1])
        output = handler.get_history_statuses(
            {'body': input_reporting_id}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 400)
        self.assertEqual(
            output['message'], f"Parameter 'to' has incorrect value: {json.loads(input_reporting_id)['to']}")
        self.assertFalse(output['data'])
        self.assertTrue('device_id' not in output)
        self.assertTrue('reporting_id' in output and output['reporting_id'])

        with open(
                f'{self.path}/../../data/history_statuses/bad_requests/'
                'get_history_statuses_time_parameter_not_a_string.json'
        ) as data_file:
            input = json.load(data_file)
        input_device_id = json.dumps(input[0])
        output = handler.get_history_statuses(
            {'body': input_device_id}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 400)
        self.assertEqual(
            output['message'], f"Parameter 'from' has incorrect value: {json.loads(input_device_id)['from']}")
        self.assertFalse(output['data'])
        self.assertTrue('device_id' in output and output['device_id'])
        self.assertTrue('reporting_id' not in output)

        input_reporting_id = json.dumps(input[1])
        output = handler.get_history_statuses(
            {'body': input_reporting_id}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 400)
        self.assertEqual(
            output['message'], f"Parameter 'to' has incorrect value: {json.loads(input_reporting_id)['to']}")
        self.assertFalse(output['data'])
        self.assertTrue('device_id' not in output)
        self.assertTrue('reporting_id' in output and output['reporting_id'])

    def test_bad_request_from_value_larger_than_to_value_on_get_history_statuses(self):
        with open(
                f'{self.path}/../../data/history_statuses/bad_requests/'
                'get_history_statuses_from_value_larger_than_to_value.json'
        ) as data_file:
            input = json.load(data_file)
        input_device_id = json.dumps(input[0])
        output = handler.get_history_statuses(
            {'body': input_device_id}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 400)
        self.assertEqual(
            output['message'], f"Parameter 'from' = {json.loads(input_device_id)['from']} "
            f"should be less than parameter 'to' = {json.loads(input_device_id)['to']}")
        self.assertFalse(output['data'])
        self.assertTrue('device_id' in output and output['device_id'])
        self.assertTrue('reporting_id' not in output)

        input_reporting_id = json.dumps(input[1])
        output = handler.get_history_statuses(
            {'body': input_reporting_id}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 400)
        self.assertEqual(
            output['message'], f"Parameter 'from' = {json.loads(input_reporting_id)['from']} "
            f"should be less than parameter 'to' = {json.loads(input_reporting_id)['to']}")
        self.assertFalse(output['data'])
        self.assertTrue('device_id' not in output)
        self.assertTrue('reporting_id' in output and output['reporting_id'])

    def test_bad_request_device_id_invalid_format_on_get_history_statuses(self):
        with open(
                f'{self.path}/../../data/history_statuses/bad_requests/'
                'get_history_statuses_device_id_invalid_format.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_statuses({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 400)
        self.assertEqual(
            output['message'], f"Parameter 'device_id' = "
            f"'{json.loads(input)['device_id']}' has incorrect value")
        self.assertFalse(output['data'])

        with open(
                f'{self.path}/../../data/history_statuses/bad_requests/'
                'get_history_statuses_device_id_parameter_as_list.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_statuses({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 400)
        self.assertEqual(
            output['message'], f"Parameter 'device_id' = "
            f"'{json.loads(input)['device_id']}' has incorrect value")
        self.assertFalse(output['data'])

    def test_bad_request_device_id_value_empty_on_get_history_statuses(self):
        with open(
                f'{self.path}/../../data/history_statuses/bad_requests/'
                'get_history_statuses_device_id_value_empty.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_statuses({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 400)
        self.assertEqual(
            output['message'], f"Parameter 'device_id' = "
            f"'{json.loads(input)['device_id']}' has incorrect value")
        self.assertFalse(output['data'])
        self.assertTrue('reporting_id' not in output)

    def test_bad_request_reporting_id_value_empty_on_get_history_statuses(self):
        with open(
                f'{self.path}/../../data/history_statuses/bad_requests/'
                'get_history_statuses_reporting_id_value_empty.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_statuses({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 400)
        self.assertEqual(
            output['message'], f"Parameter 'reporting_id' = "
            f"'{json.loads(input)['reporting_id']}' has incorrect value")
        self.assertFalse(output['data'])
        self.assertTrue('device_id' not in output)

    def test_bad_request_log_service_id_invalid_value_on_get_history_statuses(self):
        with open(
                f'{self.path}/../../data/history_statuses/bad_requests/'
                'get_history_statuses_log_service_id_invalid_value.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_statuses({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 400)
        self.assertEqual(
            output['message'], f"Parameter 'log_service_id' has incorrect "
            f"value: {json.loads(input)['log_service_id']}")
        self.assertTrue(output['device_id'])
        self.assertFalse(output['data'])

        with open(
                f'{self.path}/../../data/history_statuses/bad_requests/'
                'get_history_statuses_log_service_id_empty_value.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_statuses({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 400)
        self.assertEqual(
            output['message'], f"Parameter 'log_service_id' has incorrect "
            f"value: {json.loads(input)['log_service_id']}")
        self.assertTrue('device_id' not in output)
        self.assertTrue('reporting_id' in output and output['reporting_id'])
        self.assertFalse(output['data'])

    def test_device_not_found_on_get_history_statuses(self):
        with open(
                f'{self.path}/../../data/history_statuses/bad_requests/'
                'get_history_statuses_device_not_found.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_statuses({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 404)
        self.assertEqual(
            output['message'], "Device Not Found")
        self.assertEqual(output['device_id'], json.loads(input)['device_id'])
        self.assertEqual(output['data'], [])

    def test_reporting_id_not_found_on_get_history_statuses(self):
        with open(
                f'{self.path}/../../data/history_statuses/bad_requests/'
                'get_history_statuses_reporting_id_not_found.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_statuses({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 404)
        self.assertEqual(
            output['message'], "Reporting ID Not Found")
        self.assertEqual(output['reporting_id'],
                         json.loads(input)['reporting_id'])
        self.assertEqual(output['data'], [])

    def test_simple_success_on_get_history_statuses(self):
        with open(
                f'{self.path}/../../data/history_statuses/success/'
                'get_history_statuses_success.json'
        ) as data_file:
            input = json.load(data_file)
        input_device_id = json.dumps(input[0])
        output = handler.get_history_statuses(
            {'body': input_device_id}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 200)
        self.assertEqual(output['message'], 'Success')
        self.assertTrue(output['data'])
        for status in output['data']['value']:
            self.assertTrue(status in ("online", "offline"))
        for time in output['data']['updated']:
            self.assertTrue(time_functions.parse_time_with_tz(time))
        self.assertTrue('device_id' in output and output['device_id'])
        self.assertTrue('reporting_id' not in output)

        input_reporting_id = json.dumps(input[1])
        output = handler.get_history_statuses(
            {'body': input_reporting_id}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 200)
        self.assertEqual(output['message'], 'Success')
        self.assertTrue(output['data'])
        for status in output['data']['value']:
            self.assertTrue(status in ("online", "offline"))
        for time in output['data']['updated']:
            self.assertTrue(time_functions.parse_time_with_tz(time))
        self.assertTrue('device_id' not in output)
        self.assertTrue('reporting_id' in output and output['reporting_id'])

    def test_success_for_integer_log_service_id_on_get_history_statuses(self):
        with open(
                f'{self.path}/../../data/history_statuses/success/'
                'get_history_statuses_success_for_integer_log_service_id.json'
        ) as data_file:
            input = json.load(data_file)
        input_device_id = json.dumps(input[0])
        output = handler.get_history_statuses(
            {'body': input_device_id}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 200)
        self.assertEqual(output['message'], 'Success')
        self.assertTrue(output['data'])
        for status in output['data']['value']:
            self.assertTrue(status in ("online", "offline"))
        for time in output['data']['updated']:
            self.assertTrue(time_functions.parse_time_with_tz(time))
        self.assertTrue('device_id' in output and output['device_id'])
        self.assertTrue('reporting_id' not in output)

        input_reporting_id = json.dumps(input[1])
        output = handler.get_history_statuses(
            {'body': input_reporting_id}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 200)
        self.assertEqual(output['message'], 'Success')
        self.assertTrue(output['data'])
        for status in output['data']['value']:
            self.assertTrue(status in ("online", "offline"))
        for time in output['data']['updated']:
            self.assertTrue(time_functions.parse_time_with_tz(time))
        self.assertTrue('device_id' not in output)
        self.assertTrue('reporting_id' in output and output['reporting_id'])

    @patch.object(DeviceNetworkStatus, 'get_status_history')
    def test_database_connection_error_device_id_on_get_history_statuses(self, mock):
        mock.side_effect = ConnectionError
        with open(
                f'{self.path}/../../data/history_statuses/success/'
                'get_history_statuses_success.json'
        ) as data_file:
            input = json.load(data_file)
        input_device_id = json.dumps(input[0])
        output = handler.get_history_statuses(
            {'body': input_device_id}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 561)
        self.assertEqual(output['message'], 'Failed to connect with DB')
        self.assertFalse(output['data'])
        self.assertTrue('device_id' in output and output['device_id'])
        self.assertTrue('reporting_id' not in output)

    @patch.object(DeviceNetworkStatus, 'get_status_history')
    def test_database_connection_error_reporting_id_on_get_history_statuses(self, mock):
        mock.side_effect = ConnectionError
        with open(
                f'{self.path}/../../data/history_statuses/success/'
                'get_history_statuses_success.json'
        ) as data_file:
            input = json.load(data_file)
        input_reporting_id = json.dumps(input[1])
        output = handler.get_history_statuses(
            {'body': input_reporting_id}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 561)
        self.assertEqual(output['message'], 'Failed to connect with DB')
        self.assertFalse(output['data'])
        self.assertTrue('device_id' not in output)
        self.assertTrue('reporting_id' in output and output['reporting_id'])

    def test_both_reporting_id_and_device_id_in_request_for_get_history_statuses(self):
        # Reporting ID will be given priority
        with open(
                f'{self.path}/../../data/history_statuses/success/'
                'get_history_statuses_both_reporting_id_and_device_id_in_request.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_statuses({'body': input}, 'dummy')
        output = json.loads(output['body'])
        for date_time in output['data']['updated']:
            self.assertTrue(
                time_functions.parse_time_with_tz(
                    date_time) >= time_functions.parse_time(
                        "2017-07-01T00:00:00"
                ))
        self.assertTrue('device_id' not in output)
        self.assertTrue('reporting_id' in output and output['reporting_id'])

    def test_extra_parameters_okay_on_get_history_statuses(self):
        # Extra unnecessary parameters will be ignored
        with open(
                f'{self.path}/../../data/history_statuses/success/'
                'get_history_statuses_extra_parameters_okay.json'
        ) as data_file:
            input = json.load(data_file)
        input_device_id = json.dumps(input[0])
        output = handler.get_history_statuses(
            {'body': input_device_id}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 200)
        self.assertEqual(output['message'], 'Success')
        self.assertTrue(output['data'])
        self.assertTrue('device_id' in output and output['device_id'])
        self.assertTrue('reporting_id' not in output)

        input_reporting_id = json.dumps(input[1])
        output = handler.get_history_statuses(
            {'body': input_reporting_id}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 200)
        self.assertEqual(output['message'], 'Success')
        self.assertTrue(output['data'])
        self.assertTrue('device_id' not in output)
        self.assertTrue('reporting_id' in output and output['reporting_id'])

    def test_always_send_one_previous_record_on_get_history_statuses(self):
        with open(
                f'{self.path}/../../data/history_statuses/success/'
                'get_history_statuses_success.json'
        ) as data_file:
            input = json.load(data_file)
        input_device_id = json.dumps(input[0])
        output = handler.get_history_statuses(
            {'body': input_device_id}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 200)
        self.assertEqual(output['message'], 'Success')
        self.assertTrue(output['data'])
        self.assertEqual(output['data']['updated'][0],
                         json.loads(input_device_id)['from'])
        self.assertTrue('device_id' in output and output['device_id'])
        self.assertTrue('reporting_id' not in output)

        input_reporting_id = json.dumps(input[1])
        output = handler.get_history_statuses(
            {'body': input_reporting_id}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 200)
        self.assertEqual(output['message'], 'Success')
        self.assertTrue(output['data'])
        self.assertEqual(output['data']['updated'][0],
                         "2017-07-01T00:00:00+00:00")
        self.assertTrue('device_id' not in output)
        self.assertTrue('reporting_id' in output and output['reporting_id'])

    def test_send_offline_status_in_case_of_no_records_on_get_history_statuses(self):
        with open(
                f'{self.path}/../../data/history_statuses/success/'
                'get_history_statuses_send_offline_status_in_case_of_no_records.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_statuses({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 200)
        self.assertEqual(output['message'], "Success")
        self.assertEqual(output['device_id'], json.loads(input)['device_id'])
        self.assertTrue(output['data'])
        self.assertEqual(len(output['data']['value']), 1)
        self.assertEqual(output['data']['value'][0], 'offline')
        self.assertEqual(output['data']['updated'][0],
                         json.loads(input)['from'])
        self.assertTrue('device_id' in output and output['device_id'])
        self.assertTrue('reporting_id' not in output)

    def test_logs_not_found_for_reporting_id_on_get_history_statuses(self):
        with open(
                f'{self.path}/../../data/history_statuses/success/'
                'get_history_statuses_logs_not_found_for_reporting_id.json'
        ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_history_statuses({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 204)
        self.assertEqual(output['message'], 'Logs Not Found')
        self.assertFalse(output['data'])
        self.assertTrue('device_id' not in output)
        self.assertTrue('reporting_id' in output and output['reporting_id'])
