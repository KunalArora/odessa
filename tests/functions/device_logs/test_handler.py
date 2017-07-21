import unittest
import json
import re
from functions.device_logs import handler
from functions.helper import *
from tests.functions import test_helper

def run_func(**keyword_args):
    return handler.get_latest_logs(
        keyword_args['event'],
        keyword_args['context'])


class TestDeviceLogs(unittest.TestCase):
    def setUp(self):
        test_helper.set_env_var(self)
        test_helper.seed_ddb_device_logs(self)
        test_helper.seed_ec_device_logs(self)
        logging.getLogger('device_logs').setLevel(100)

    def tearDown(self):
        test_helper.clear_cache(self)
        test_helper.clear_db(self)
        test_helper.create_table(self)

    def test_with_invalid_request(self):
        res = run_func(
            event = { "body" : "{"
            },
            context = []
        )
        self.assertRaises(TypeError)
        res_json = json.loads(res['body'])
        self.assertEqual({"code": 400, "devices": [], "message": "Bad Request"}, res_json)


    def test_invalid_log_service_id_request(self):
        res = run_func(
            event = { "body" : "{\"device_id\": [\"3f2504e0-4f89-11d3-9a0c-0305e82c3301\", \"ffffffff-ffff-ffff-ffff-ffffffff0001\"], \"log_service_id\": \"4\"}"
            },
            context = []
        )
        self.assertRaises(ServiceIdError)
        res_json = json.loads(res['body'])
        self.assertEqual({"code": 400, "devices": [], "message": "Bad Request"}, res_json)

    def test_empty_device(self):
        res = run_func(
            event = { "body" : "{ \"device_id\": [], \"log_service_id\": \"0\" }"
            },
            context = []
        )
        self.assertRaises(DeviceIdParameterError)
        res_json = json.loads(res['body'])
        self.assertEqual({"code": 400, "devices": [], "message": "Bad Request"}, res_json)

    def test_null_device(self):
        res = run_func(
            event = { "body": "{\"log_service_id\": \"0\"}"
            },
            context = []
        )
        self.assertRaises(DeviceIdParameterError)
        res_json = json.loads(res['body'])
        self.assertEqual({"code": 400, "devices": [], "message": "Bad Request"}, res_json)

    def test_empty_request_body(self):
        res = run_func(
            event = { "body" : "{}"
            },
            context = []
        )
        self.assertRaises(TypeError)
        res_json = json.loads(res['body'])
        self.assertEqual({"code": 400, "devices": [], "message": "Bad Request"}, res_json)

    def test_exist_and_empty_device(self):
        res = run_func(
            event = { "body" : "{\"device_id\": [\"ffffffff-ffff-ffff-ffff-ffffffff0001\", \"\"],\"log_service_id\": \"0\"}"
            },
            context = []
        )
        res_json = json.loads(res['body'])
        for r in res_json['devices']:
            if not r['device_id']:
                self.assertEqual(404, r['error_code'])

    def test_single_device_ok(self):
        res = run_func(
            event = { "body" : "{\"device_id\": [\"ffffffff-ffff-ffff-ffff-ffffffff0001\"], \"log_service_id\": \"0\"}"
            },
            context = []
        )
        res_json = json.loads(res['body'])
        self.assertEqual(1, len(res_json['devices']))
        self.assertEqual("ffffffff-ffff-ffff-ffff-ffffffff0001", res_json['devices'][0]['device_id'])
        self.assertTrue(res_json['devices'][0]['data'])

    def test_multiple_device_ok(self):
        deviceIdList = ["ffffffff-ffff-ffff-ffff-ffffffff0000", "ffffffff-ffff-ffff-ffff-ffffffff0001"]
        res = run_func(
            event = { "body" : "{\"device_id\": [\"ffffffff-ffff-ffff-ffff-ffffffff0001\", \"ffffffff-ffff-ffff-ffff-ffffffff0000\"], \"log_service_id\": \"0\"}"
            },
            context = []
        )
        res_json = json.loads(res['body'])
        self.assertEqual(2, len(res_json['devices']))
        for value in res_json['devices']:
            self.assertTrue(value['device_id'] in deviceIdList)
            if value['error_code'] != 404:
                self.assertTrue(value['data'])
            else:
                self.assertFalse(value['data'])


    def test_duplicate_device(self):
        res = run_func(
            event = { "body" : "{\"device_id\": [\"ffffffff-ffff-ffff-ffff-ffffffff0001\", \"ffffffff-ffff-ffff-ffff-ffffffff0001\"], \"log_service_id\": \"0\"}"
            },
            context = []
        )
        res_json = json.loads(res['body'])
        self.assertEqual(1, len(res_json['devices']))
        self.assertEqual("ffffffff-ffff-ffff-ffff-ffffffff0001", res_json['devices'][0]['device_id'])
        self.assertTrue(res_json['devices'][0]['data'])

    def test_multiple_non_exist_devices(self):
        res = run_func(
            event = { "body" : "{\"device_id\": [\"ffffffff-ffff-ffff-bbbb-ffffffff0003\", \"ffffffff-ffff-aaaa-ffff-ffffffff0004\"], \"log_service_id\": \"0\"}"
            },
            context = []
        )
        res_json = json.loads(res['body'])
        self.assertEqual(2, len(res_json['devices']))
        for value in res_json['devices']:
            self.assertEqual(404, value['error_code'])
            self.assertEqual("Device Not Found", value['message'])
        self.assertEqual(404, res_json['code'])

    def test_partial_exist_device(self):
        res = run_func(
            event = { "body" : "{\"device_id\": [\"ffffffff-ffff-ffff-ffff-ffffffff0001\", \"ffffffff-ffff-ffff-ffff-ffffffff0002\", \"ffffffff-ffff-ffff-ffff-ffffffff0003\"], \"log_service_id\": \"0\" }"
            },
            context = []
        )
        res_json = json.loads(res['body'])
        self.assertEqual(3, len(res_json['devices']))
        self.assertEqual(207, res_json['code'])
        self.assertEqual("Partial Success", res_json['message'])
        for idx, value in enumerate(res_json['devices']):
            if value['error_code'] == 404:
                self.assertFalse(value['data'])
            else:
                self.assertTrue(value['data'])

    def test_date_format(self):
        datetime_format = re.compile(r'^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}$')
        res = run_func(
            event = { "body" : "{\"device_id\": [\"ffffffff-ffff-ffff-ffff-ffffffff0001\"], \"log_service_id\": \"0\"}"
            },
            context = []
        )
        res_json = json.loads(res['body'])
        self.assertEqual(1, len(res_json['devices']))
        self.assertEqual("ffffffff-ffff-ffff-ffff-ffffffff0001", res_json['devices'][0]['device_id'])
        self.assertTrue(res_json['devices'][0]['data'])
        for data in res_json['devices'][0]['data']:
            if data['updated']:
                self.assertTrue(datetime_format.search(data['updated']))
            else:
                self.assertFalse(datetime_format.search(data['updated']))

    def test_with_invalid_oid_value(self):
        res = run_func(
            event = { "body" : "{\"device_id\": [\"ffffffff-ffff-ffff-ffff-ffffffff0001\"], \"log_service_id\": \"0\"}"
            },
            context = []
        )
        res_json = json.loads(res['body'])
        self.assertEqual(1, len(res_json['devices']))
        self.assertEqual("ffffffff-ffff-ffff-ffff-ffffffff0001", res_json['devices'][0]['device_id'])
        self.assertTrue(res_json['devices'][0]['data'])
        for data in res_json['devices'][0]['data']:
            if data['message'] == 'Parser Error':
                self.assertEqual(500, data['error_code'])
                self.assertTrue(data['feature'])

    def test_with_network_status_missing(self):
        res = run_func(
            event = { "body" : "{\"device_id\": [\"ffffffff-ffff-ffff-ffff-ffffffff0002\"], \"log_service_id\": \"0\"}"
            },
            context = []
        )
        res_json = json.loads(res['body'])
        self.assertEqual(1, len(res_json['devices']))
        self.assertEqual("ffffffff-ffff-ffff-ffff-ffffffff0002", res_json['devices'][0]['device_id'])
        self.assertTrue(res_json['devices'][0]['data'])
        for data in res_json['devices'][0]['data']:
            self.assertNotEqual('Online_Offline', data['feature'])
