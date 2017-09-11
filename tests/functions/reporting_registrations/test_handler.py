import boto3
from os import environ
import unittest
import logging
import json
from functions.reporting_registrations import handler
from tests.functions import test_helper
from unittest.mock import patch
from models.reporting_registration import ReportingRegistration
from botocore.exceptions import ConnectionError

def run_func(**keyword_args):
    return handler.save_reporting_registration(
        keyword_args['event'],
        keyword_args['context'])

class TestReportingRegistrations(unittest.TestCase):
    def setUp(self):
        test_helper.set_env_var(self)
        test_helper.seed_ddb_reporting_registrations(self)
        logging.getLogger('reporting_registrations').setLevel(100)
        self.dynamodb = boto3.resource(
                            'dynamodb', endpoint_url=environ['DYNAMO_ENDPOINT_URL'])

    def tearDown(self):
        test_helper.clear_db(self)
        test_helper.create_table(self)

    def test_with_invalid_request(self):
        res = run_func(
            event = { "body" : "{"
            },
            context = []
        )
        res_json = json.loads(res['body'])
        self.assertTrue(2, len(res_json))
        self.assertEqual(400, res_json['code'])
        self.assertEqual('JSON request format is invalid.', res_json['message'])

    def test_reporting_id_request_parameter_missing(self):
        res = run_func(
            event = { "body" : "{\"device_id\": \"ffffffff-ffff-ffff-ffff-"
            "ffffffff0001\", \"communication_type\": \"cloud\","
            "\"log_service_id\": \"0\"}"
            },
            context = []
        )
        res_json = json.loads(res['body'])
        self.assertTrue(2, len(res_json))
        self.assertEqual(400, res_json['code'])
        self.assertEqual('Reporting Id is not present.', res_json['message'])

    def test_reporting_id_request_parameter_empty(self):
        res = run_func(
            event = { "body" : "{\"reporting_id\":\"\",\"device_id\":"
            "\"ffffffff-ffff-ffff-ffff-ffffffff0001\", \"communication_type\":"
            "\"cloud\",\"log_service_id\": \"0\"}"
            },
            context = []
        )
        res_json = json.loads(res['body'])
        self.assertTrue(2, len(res_json))
        self.assertEqual(400, res_json['code'])
        self.assertEqual('Reporting Id is empty.', res_json['message'])

    def test_reporting_id_request_parameter_not_string(self):
        res = run_func(
            event = { "body" : "{\"reporting_id\":1,\"device_id\":\"ffffffff-ffff-ffff-ffff-"
            "ffffffff0001\", \"communication_type\":\"cloud\","
            "\"log_service_id\": \"0\"}"
            },
            context = []
        )
        res_json = json.loads(res['body'])
        self.assertTrue(2, len(res_json))
        self.assertEqual(400, res_json['code'])
        self.assertEqual('Reporting Id is not in string format.', res_json['message'])

    def test_communication_type_request_parameter_missing(self):
        res = run_func(
            event = { "body" : "{\"reporting_id\":\"413c29f2-9187-4aa8-"
            "8b15-200de7641d7e\",\"device_id\": \"ffffffff-ffff-ffff-ffff-"
            "ffffffff0001\", \"log_service_id\": \"0\"}"
            },
            context = []
        )
        res_json = json.loads(res['body'])
        self.assertTrue(2, len(res_json))
        self.assertEqual(400, res_json['code'])
        self.assertEqual('Communication type is not present.', res_json['message'])

    def test_communication_type_request_parameter_empty(self):
        res = run_func(
            event = { "body" : "{\"reporting_id\":\"413c29f2-9187-4aa8-"
            "8b15-200de7641d7e\",\"device_id\":\"ffffffff-ffff-ffff-ffff-"
            "ffffffff0001\",\"communication_type\":\"\",\"log_service_id\": \"0\"}"
            },
            context = []
        )
        res_json = json.loads(res['body'])
        self.assertTrue(2, len(res_json))
        self.assertEqual(400, res_json['code'])
        self.assertEqual('Communication type is empty.', res_json['message'])

    def test_communication_type_request_parameter_not_string(self):
        res = run_func(
            event = { "body" : "{\"reporting_id\":\"413c29f2-9187-4aa8-"
            "8b15-200de7641d7e\",\"device_id\":\"ffffffff-ffff-ffff-ffff-"
            "ffffffff0001\",\"communication_type\":4,\"log_service_id\": \"0\"}"
            },
            context = []
        )
        res_json = json.loads(res['body'])
        self.assertTrue(2, len(res_json))
        self.assertEqual(400, res_json['code'])
        self.assertEqual('Communication type is not in string format.', res_json['message'])

    def test_communication_type_request_parameter_not_cloud_or_email(self):
        res = run_func(
            event = { "body" : "{\"reporting_id\":\"413c29f2-9187-4aa8-"
            "8b15-200de7641d7e\",\"device_id\":\"ffffffff-ffff-ffff-ffff-"
            "ffffffff0001\",\"communication_type\":\"xyz\",\"log_service_id\": \"0\"}"
            },
            context = []
        )
        res_json = json.loads(res['body'])
        self.assertTrue(2, len(res_json))
        self.assertEqual(400, res_json['code'])
        self.assertEqual('Communication type is invalid.', res_json['message'])

    def test_communication_type_cloud_device_id_missing(self):
        res = run_func(
            event = { "body" : "{\"reporting_id\":\"413c29f2-9187-4aa8-"
            "8b15-200de7641d7e\",\"communication_type\":\"cloud\","
            "\"log_service_id\": \"0\"}"
            },
            context = []
        )
        res_json = json.loads(res['body'])
        self.assertTrue(2, len(res_json))
        self.assertEqual(400, res_json['code'])
        self.assertEqual('Device Id is not present.', res_json['message'])

    def test_communication_type_cloud_serial_number_present_device_id_missing(self):
        res = run_func(
            event = { "body" : "{\"reporting_id\":\"413c29f2-9187-4aa8-"
            "8b15-200de7641d7e\",\"communication_type\":\"cloud\","
            "\"serial_number\":\"000GAFT567A12GY\",\"log_service_id\": \"0\"}"
            },
            context = []
        )
        res_json = json.loads(res['body'])
        self.assertTrue(2, len(res_json))
        self.assertEqual(400, res_json['code'])
        self.assertEqual('Device Id is not present.', res_json['message'])

    def test_communication_type_email_serial_number_missing(self):
        res = run_func(
            event = { "body" : "{\"reporting_id\":\"413c29f2-9187-4aa8-"
            "8b15-200de7641d7e\",\"communication_type\":\"email\","
            "\"log_service_id\": \"0\"}"
            },
            context = []
        )
        res_json = json.loads(res['body'])
        self.assertTrue(2, len(res_json))
        self.assertEqual(400, res_json['code'])
        self.assertEqual('Serial number is not present.', res_json['message'])

    def test_communication_type_email_device_id_present_serial_number_missing(self):
        res = run_func(
            event = { "body" : "{\"reporting_id\":\"413c29f2-9187-4aa8-"
            "8b15-200de7641d7e\",\"communication_type\":\"email\","
            "\"device_id\":\"ffffffff-ffff-ffff-ffff-ffffffff0001\","
            "\"log_service_id\": \"0\"}"
            },
            context = []
        )
        res_json = json.loads(res['body'])
        self.assertTrue(2, len(res_json))
        self.assertEqual(400, res_json['code'])
        self.assertEqual('Serial number is not present.', res_json['message'])

    def test_communication_type_cloud_device_id_present_but_empty(self):
        res = run_func(
            event = { "body" : "{\"reporting_id\":\"413c29f2-9187-4aa8-"
            "8b15-200de7641d7e\",\"communication_type\":\"cloud\","
            "\"device_id\":\"\",\"log_service_id\": \"0\"}"
            },
            context = []
        )
        res_json = json.loads(res['body'])
        self.assertTrue(2, len(res_json))
        self.assertEqual(400, res_json['code'])
        self.assertEqual('Device Id is empty.', res_json['message'])

    def test_communication_type_cloud_device_id_present_but_not_string(self):
        res = run_func(
            event = { "body" : "{\"reporting_id\":\"413c29f2-9187-4aa8-"
            "8b15-200de7641d7e\",\"communication_type\":\"cloud\","
            "\"device_id\":3,\"log_service_id\": \"0\"}"
            },
            context = []
        )
        res_json = json.loads(res['body'])
        self.assertTrue(2, len(res_json))
        self.assertEqual(400, res_json['code'])
        self.assertEqual('Device Id is not in string format.', res_json['message'])

    def test_communication_type_email_serial_number_present_but_empty(self):
        res = run_func(
            event = { "body" : "{\"reporting_id\":\"413c29f2-9187-4aa8-"
            "8b15-200de7641d7e\",\"communication_type\":\"email\","
            "\"serial_number\":\"\",\"log_service_id\": \"0\"}"
            },
            context = []
        )
        res_json = json.loads(res['body'])
        self.assertTrue(2, len(res_json))
        self.assertEqual(400, res_json['code'])
        self.assertEqual('Serial number is empty.', res_json['message'])

    def test_communication_type_email_serial_number_present_but_not_string(self):
        res = run_func(
            event = { "body" : "{\"reporting_id\":\"413c29f2-9187-4aa8-"
            "8b15-200de7641d7e\",\"communication_type\":\"email\","
            "\"serial_number\":3,\"log_service_id\": \"0\"}"
            },
            context = []
        )
        res_json = json.loads(res['body'])
        self.assertTrue(2, len(res_json))
        self.assertEqual(400, res_json['code'])
        self.assertEqual('Serial number is not in string format.', res_json['message'])

    def test_log_service_id_not_string(self):
        res = run_func(
            event = { "body" : "{\"reporting_id\":\"413c29f2-9187-4aa8-"
            "8b15-200de7641d7e\",\"communication_type\":\"email\","
            "\"serial_number\":\"000GAFT567A12GY\",\"log_service_id\": [\"xyz\"]}"
            },
            context = []
        )
        res_json = json.loads(res['body'])
        self.assertTrue(2, len(res_json))
        self.assertEqual(400, res_json['code'])
        self.assertEqual('Log service Id is not in string format.', res_json['message'])

    def test_log_service_id_not_exist_in_db(self):
        res = run_func(
            event = { "body" : "{\"reporting_id\":\"413c29f2-9187-4aa8-"
            "8b15-200de7641d7e\",\"device_id\": \"ffffffff-ffff-ffff-ffff-"
            "ffffffff0001\", \"communication_type\": \"cloud\","
            "\"log_service_id\": \"4\"}"
            },
            context = []
        )
        res_json = json.loads(res['body'])
        self.assertTrue(2, len(res_json))
        self.assertEqual(400, res_json['code'])
        self.assertEqual('Log service Id does not exist.', res_json['message'])

    @patch.object(ReportingRegistration, 'create')
    def test_database_connection_error(self, mock):
        mock.side_effect = ConnectionError
        res = run_func(
            event = { "body" : "{\"reporting_id\":\"413c29f2-9187-4aa8-"
            "8b15-200de7641d7e\",\"device_id\":\"ffffffff-ffff-ffff-ffff-"
            "ffffffff0001\",\"communication_type\":\"cloud\","
            "\"log_service_id\": \"0\"}"
            },
            context = []
        )
        self.assertRaises(ConnectionError)
        res_json = json.loads(res['body'])
        self.assertTrue(2, len(res_json))
        self.assertEqual(561, res_json['code'])
        self.assertEqual('Failed to connect with DB', res_json['message'])

    def test_communication_type_cloud_device_not_subscribed(self):
        res = run_func(
            event = { "body" : "{\"reporting_id\":\"413c29f2-9187-4aa8-"
            "8b15-200de7641d7e\",\"device_id\":\"ffffffff-ffff-ffff-ffff-"
            "ffffffff0002\",\"communication_type\":\"cloud\","
            "\"log_service_id\": \"0\"}"
            },
            context = []
        )
        res_json = json.loads(res['body'])
        self.assertTrue(2, len(res_json))
        self.assertEqual(404, res_json['code'])
        self.assertEqual('Device Not Found', res_json['message'])

    def test_communication_type_cloud_device_id_present_only_success(self):
        table = self.dynamodb.Table('reporting_registrations')
        before_keys = table.scan()['Items']
        res = run_func(
            event = { "body" : "{\"reporting_id\":\"413c29f2-9187-4aa8-"
            "8b15-200de7641d7e\",\"device_id\":\"ffffffff-ffff-ffff-ffff-"
            "ffffffff0001\",\"communication_type\":\"cloud\","
            "\"log_service_id\": \"0\"}"
            },
            context = []
        )
        after_keys = table.scan()['Items']
        res_json = json.loads(res['body'])
        self.assertTrue(2, len(res_json))
        self.assertEqual(200, res_json['code'])
        self.assertEqual('Success', res_json['message'])
        self.assertEqual((len(after_keys)-len(before_keys)),1)
        self.assertTrue('reporting_id' in after_keys[0])
        self.assertTrue('timestamp' in after_keys[0])
        self.assertTrue('communication_type' in after_keys[0])
        self.assertTrue('device_id' in after_keys[0])
        self.assertTrue('serial_number' not in after_keys[0])
        self.assertEqual(4, len(after_keys[0]))
        self.assertEqual('cloud', after_keys[0]['communication_type'])

    def test_communication_type_cloud_device_id_and_serial_number_present_success(self):
        table = self.dynamodb.Table('reporting_registrations')
        before_keys = table.scan()['Items']
        res = run_func(
            event = { "body" : "{\"reporting_id\":\"413c29f2-9187-4aa8-"
            "8b15-200de7641d7e\",\"device_id\":\"ffffffff-ffff-ffff-ffff-"
            "ffffffff0001\",\"serial_number\":\"000GAFT567A12GY\","
            "\"communication_type\":\"cloud\",\"log_service_id\": \"0\"}"
            },
            context = []
        )
        after_keys = table.scan()['Items']
        res_json = json.loads(res['body'])
        self.assertTrue(2, len(res_json))
        self.assertEqual(200, res_json['code'])
        self.assertEqual('Success', res_json['message'])
        self.assertEqual((len(after_keys)-len(before_keys)),1)
        self.assertTrue('reporting_id' in after_keys[0])
        self.assertTrue('timestamp' in after_keys[0])
        self.assertTrue('communication_type' in after_keys[0])
        self.assertTrue('device_id' in after_keys[0])
        self.assertTrue('serial_number' not in after_keys[0])
        self.assertEqual(4, len(after_keys[0]))
        self.assertEqual('cloud', after_keys[0]['communication_type'])

    def test_communication_type_email_serial_number_present_only_success(self):
        table = self.dynamodb.Table('reporting_registrations')
        before_keys = table.scan()['Items']
        res = run_func(
            event = { "body" : "{\"reporting_id\":\"413c29f2-9187-4aa8-"
            "8b15-200de7641d7e\",\"serial_number\":\"000GAFT567A12GY\","
            "\"communication_type\":\"email\",\"log_service_id\": \"0\"}"
            },
            context = []
        )
        after_keys = table.scan()['Items']
        res_json = json.loads(res['body'])
        self.assertTrue(2, len(res_json))
        self.assertEqual(200, res_json['code'])
        self.assertEqual('Success', res_json['message'])
        self.assertEqual((len(after_keys)-len(before_keys)),1)
        self.assertTrue('reporting_id' in after_keys[0])
        self.assertTrue('timestamp' in after_keys[0])
        self.assertTrue('communication_type' in after_keys[0])
        self.assertTrue('serial_number' in after_keys[0])
        self.assertTrue('device_id' not in after_keys[0])
        self.assertEqual(4, len(after_keys[0]))
        self.assertEqual('email', after_keys[0]['communication_type'])

    def test_communication_type_email_serial_number_and_device_id_present_success(self):
        table = self.dynamodb.Table('reporting_registrations')
        before_keys = table.scan()['Items']
        res = run_func(
            event = { "body" : "{\"reporting_id\":\"413c29f2-9187-4aa8-"
            "8b15-200de7641d7e\",\"device_id\":\"ffffffff-ffff-ffff-ffff-"
            "ffffffff0001\",\"serial_number\":\"000GAFT567A12GY\","
            "\"communication_type\":\"email\",\"log_service_id\": \"0\"}"
            },
            context = []
        )
        after_keys = table.scan()['Items']
        res_json = json.loads(res['body'])
        self.assertTrue(2, len(res_json))
        self.assertEqual(200, res_json['code'])
        self.assertEqual('Success', res_json['message'])
        self.assertEqual((len(after_keys)-len(before_keys)),1)
        self.assertTrue('reporting_id' in after_keys[0])
        self.assertTrue('timestamp' in after_keys[0])
        self.assertTrue('communication_type' in after_keys[0])
        self.assertTrue('serial_number' in after_keys[0])
        self.assertTrue('device_id' not in after_keys[0])
        self.assertEqual(4, len(after_keys[0]))
        self.assertEqual('email', after_keys[0]['communication_type'])

    def test_communication_type_email_not_in_lowercase(self):
        table = self.dynamodb.Table('reporting_registrations')
        before_keys = table.scan()['Items']
        res = run_func(
            event = { "body" : "{\"reporting_id\":\"413c29f2-9187-4aa8-"
            "8b15-200de7641d7e\",\"serial_number\":\"000GAFT567A12GY\","
            "\"communication_type\":\"EmAil\",\"log_service_id\": \"0\"}"
            },
            context = []
        )
        after_keys = table.scan()['Items']
        res_json = json.loads(res['body'])
        self.assertTrue(2, len(res_json))
        self.assertEqual(200, res_json['code'])
        self.assertEqual('Success', res_json['message'])
        self.assertEqual((len(after_keys)-len(before_keys)),1)
        self.assertEqual(4, len(after_keys[0]))
        self.assertEqual('email', after_keys[0]['communication_type'])

    def test_communication_type_cloud_not_in_lowercase(self):
        table = self.dynamodb.Table('reporting_registrations')
        before_keys = table.scan()['Items']
        res = run_func(
            event = { "body" : "{\"reporting_id\":\"413c29f2-9187-4aa8-"
            "8b15-200de7641d7e\",\"device_id\":\"ffffffff-ffff-ffff-ffff-"
            "ffffffff0001\",\"communication_type\":\"cLOud\","
            "\"log_service_id\": \"0\"}"
            },
            context = []
        )
        after_keys = table.scan()['Items']
        res_json = json.loads(res['body'])
        self.assertTrue(2, len(res_json))
        self.assertEqual(200, res_json['code'])
        self.assertEqual('Success', res_json['message'])
        self.assertEqual((len(after_keys)-len(before_keys)),1)
        self.assertEqual(4, len(after_keys[0]))
        self.assertEqual('cloud', after_keys[0]['communication_type'])
