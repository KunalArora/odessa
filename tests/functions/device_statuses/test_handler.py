import unittest
import json
import logging
from os import path
from functions.device_statuses import handler
from tests.functions import test_helper


class GetDeviceStatusesTestCase(unittest.TestCase):
    def setUp(self):
        test_helper.set_env_var(self)
        self.path = path.dirname(__file__)
        test_helper.seed_ddb_device_statuses(self)
        logging.getLogger('device_statuses').setLevel(100)

    def tearDown(self):
        test_helper.clear_db(self)
        test_helper.create_table(self)

    def test_bad_request(self):
        output = handler.get_device_statuses(
            {'body': json.dumps({})}, 'dummy')
        self.assertEqual(json.loads(output['body'])["code"], 400)
        self.assertEqual(json.loads(output['body'])["devices"], [])
        output = handler.get_device_statuses(
            {'body': json.dumps(
                {'from': '2016-06-30T12:00:00+00:00'})},
            'dummy')
        self.assertEqual(json.loads(output['body'])["code"], 400)
        output = handler.get_device_statuses(
            {'body': json.dumps(
                {'from': '2016-06-30T12:00:00',
                 'reporting_id': 'eeeeeeee-eeee-eeee-eeee-eeeeeeee0001',
                 'features': ['Drum_Unit', 'TonerInk_Black']})},
            'dummy')
        self.assertEqual(json.loads(output['body'])["code"], 400)
        output = handler.get_device_statuses(
            {'body': json.dumps(
                {'features': 'unknown_feature',
                 'from': 'bad_date',
                 'reporting_id': 'eeeeeeee-eeee-eeee-eeee-eeeeeeee0001'})},
            'dummy')
        self.assertEqual(json.loads(output['body'])["code"], 400)
        output = handler.get_device_statuses(
            {'body': json.dumps(
                {'features': 'Drum_Unit',
                 'reporting_id': []})},
            'dummy')
        self.assertEqual(json.loads(output['body'])["code"], 400)
        output = handler.get_device_statuses(
            {'body': json.dumps(
                {'features': [],
                 'reporting_id': ['eeeeeeee-eeee-eeee-eeee-eeeeeeee0001']})},
            'dummy')
        self.assertEqual(json.loads(output['body'])["code"], 400)
        output = handler.get_device_statuses(
            {'body': json.dumps(
                {'features': ['unknown1', 'unknown2', 'unknown3'],
                 'reporting_id': ['eeeeeeee-eeee-eeee-eeee-eeeeeeee0001']})},
            'dummy')
        self.assertEqual(json.loads(output['body'])["code"], 400)

    def test_single_status_success(self):
        with open(
                f'{self.path}/../../data/device_statuses/handler/single_success.json'
                ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_device_statuses({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 200)
        self.assertEqual(len(output["devices"]), 1)
        self.assertEqual(len(output["devices"][0]['data']), 3)
        for feature in output["devices"][0]['data']:
            self.assertEqual(feature['error_code'], 200)
            self.assertTrue(feature['feature'] in json.loads(input)['features'])
            self.assertTrue('value' in feature)
            self.assertTrue('updated' in feature)
            self.assertEqual(feature['message'], 'Success')

    def test_single_status_not_found(self):
        with open(
                f'{self.path}/../../data/device_statuses/handler/single_logs_not_found.json'
                ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_device_statuses({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 204)
        self.assertEqual(len(output["devices"]), 1)
        self.assertEqual(len(output["devices"][0]['data']), 0)

    def test_single_status_old_feature(self):
        with open(
                f'{self.path}/../../data/device_statuses/handler/single_old_feature.json'
                ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_device_statuses({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 200)
        self.assertEqual(len(output["devices"]), 1)
        self.assertEqual(len(output["devices"][0]['data']), 2)

    def test_single_status_unknown_feature(self):
        with open(
                f'{self.path}/../../data/device_statuses/handler/single_unknown_feature.json'
                ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_device_statuses({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 207)
        self.assertEqual(len(output["devices"]), 1)
        self.assertEqual(len(output["devices"][0]['data']), 4)
        for feature in output["devices"][0]['data']:
            if feature['error_code'] == 404:
                self.assertTrue(feature['feature'] in json.loads(input)['features'])
                self.assertFalse('value' in feature)
                self.assertFalse('updated' in feature)
                self.assertEqual(feature['message'], 'Feature Not Found')
            else:
                self.assertEqual(feature['error_code'], 200)
                self.assertTrue(feature['feature'] in json.loads(input)['features'])
                self.assertTrue('value' in feature)
                self.assertTrue('updated' in feature)
                self.assertEqual(feature['message'], 'Success')

    def test_single_status_unknown_feature_no_logs(self):
        with open(
                f'{self.path}/../../data/device_statuses/handler/single_unknown_feature_no_logs.json'
                ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_device_statuses({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 207)
        self.assertEqual(len(output["devices"]), 1)
        self.assertEqual(len(output["devices"][0]['data']), 1)
        self.assertEqual(output["devices"][0]['data'][0]['error_code'], 404)
        self.assertEqual(output["devices"][0]['error_code'], 207)

    def test_single_status_count_type_feature(self):
        with open(
                f'{self.path}/../../data/device_statuses/handler/single_status_count_type_feature.json'
                ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_device_statuses({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 200)
        self.assertEqual(len(output["devices"]), 1)
        self.assertEqual(len(output["devices"][0]['data']), 1)
        self.assertEqual(output["devices"][0]['data'][0]['error_code'], 200)
        self.assertEqual(output["devices"][0]['error_code'], 200)

    def test_single_status_counter_only_feature(self):
        with open(
                f'{self.path}/../../data/device_statuses/handler/single_status_counter_only_feature.json'
                ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_device_statuses({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 200)
        self.assertEqual(len(output["devices"]), 1)
        self.assertEqual(len(output["devices"][0]['data']), 1)
        self.assertEqual(output["devices"][0]['data'][0]['error_code'], 200)
        self.assertEqual(output["devices"][0]['error_code'], 200)

    def test_multi_status_success(self):
        with open(
                f'{self.path}/../../data/device_statuses/handler/multi_success.json'
                ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_device_statuses({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 200)
        self.assertEqual(len(output["devices"]), 2)
        self.assertEqual(len(output["devices"][0]['data']), 5)
        self.assertEqual(output["devices"][0]['error_code'], 200)
        self.assertEqual(len(output["devices"][1]['data']), 4)
        self.assertEqual(output["devices"][1]['error_code'], 200)

    def test_multi_status_partial_success_not_found(self):
        with open(
                f'{self.path}/../../data/device_statuses/handler/multi_partial_success_not_found.json'
                ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_device_statuses({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 207)
        self.assertEqual(len(output["devices"]), 2)
        self.assertEqual(len(output["devices"][0]['data']), 0)
        self.assertEqual(output["devices"][0]['error_code'], 404)
        self.assertEqual(len(output["devices"][1]['data']), 4)
        self.assertEqual(output["devices"][1]['error_code'], 200)

    def test_multi_status_partial_success_no_logs(self):
        with open(
                f'{self.path}/../../data/device_statuses/handler/multi_partial_success_no_logs.json'
                ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_device_statuses({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 207)
        self.assertEqual(len(output["devices"]), 2)
        self.assertEqual(len(output["devices"][0]['data']), 4)
        self.assertEqual(output["devices"][0]['error_code'], 200)
        self.assertEqual(len(output["devices"][1]['data']), 0)
        self.assertEqual(output["devices"][1]['error_code'], 204)

    def test_multi_status_unknown_feature(self):
        with open(
                f'{self.path}/../../data/device_statuses/handler/multi_unknown_feature.json'
                ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_device_statuses({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 400)
        self.assertEqual(len(output["devices"]), 0)

    def test_multi_status_not_found(self):
        with open(
                f'{self.path}/../../data/device_statuses/handler/multi_unknown_status.json'
                ) as data_file:
            input = json.dumps(json.load(data_file))
        output = handler.get_device_statuses({'body': input}, 'dummy')
        output = json.loads(output['body'])
        self.assertEqual(output['code'], 404)
        self.assertEqual(len(output["devices"]), 2)
        self.assertEqual(output["devices"][0]['error_code'], 404)
        self.assertEqual(output["devices"][1]['error_code'], 404)
