import logging
import unittest
import uuid
from functions.device_events import handler
from tests.functions import test_helper


class TestDeviceEventsHandler(unittest.TestCase):
    def setUp(self):
        logging.getLogger('device_events').setLevel(100)
        test_helper.create_table(self)
        test_helper.seed_ddb_subscriptions(self)

    def tearDown(self):
        test_helper.clear_db(self)

    def test_handle_delete_event(self):
        event = {'Records': [{
            'Sns': {
                'Message': f'{{"device_id":"ffffffff-ffff-ffff-ffff-ffffff000000","event":"delete"}}'}}]}
        handler.handle_device_events(event=event, context=[])
        res = test_helper.get_device(self, 'ffffffff-ffff-ffff-ffff-ffffff000000#0')
        self.assertEqual(res['Items'][0]['status'], 2200)

    def test_handle_unknown_event(self):
        event = {'Records': [{
            'Sns': {
                'Message': f'{{"device_id":"ffffffff-ffff-ffff-ffff-ffffffffffff","event":"unkown"}}'}}]}
        handler.handle_device_events(event=event, context=[])
        res = test_helper.get_device(self, 'ffffffff-ffff-ffff-ffff-ffffff000000#0')
        self.assertEqual(res['Items'][0]['status'], 1200)

    def test_handle_not_exist_device_event(self):
        event = {'Records': [{
            'Sns': {
                'Message': f'{{"device_id":"{str(uuid.uuid1())}","event":"delete"}}'}}]}
        self.assertTrue(handler.handle_device_events(event=event, context=[]))

    def test_handle_invalid_message(self):
        event = {'Records': [{
            'Sns': {
                'Message': f'{{device_id:{str(uuid.uuid1())},event:delete}}'}}]}
        self.assertTrue(handler.handle_device_events(event=event, context=[]))

