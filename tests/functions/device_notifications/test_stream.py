import redis
import unittest
from functions.device_notifications import stream
from functions.helper import *
from tests.functions import test_helper

def run_logs_func(**keyword_args):
    return stream.save_notify_logs_cache(
        keyword_args['event'],
        keyword_args['context'])

def run_status_func(**keyword_args):
    return stream.save_notify_status_cache(
        keyword_args['event'],
        keyword_args['context'])

class TestDeviceNotificationsStream(unittest.TestCase):
    def setUp(self):
        self.r = redis.StrictRedis(host='localhost', port=6379, db=0)
        test_helper.set_env_var(self)
        test_helper.seed_ddb_device_notifications(self)
        test_helper.seed_ec_device_notifications(self)
        logging.getLogger('device_notifications').setLevel(100)

    def tearDown(self):
        test_helper.clear_cache(self)
        test_helper.clear_db(self)
        test_helper.create_table(self)

    def test_save_notify_logs_cache_invalid_request(self):
        res = run_logs_func(
            event = "{",
            context = []
        )
        self.assertEqual(None, res)
        self.assertRaises(TypeError)

    def test_save_notify_logs_cache_success_single_event(self):
        before_keys = self.r.scan()
        before_size = self.r.dbsize()
        event_data = json.load(open('tests/data/stream/logs_success_single_event.json'))
        res = run_logs_func(
            event = event_data,
            context= []
        )
        after_keys = self.r.scan()
        after_size = self.r.dbsize()
        self.assertNotEqual(before_keys, after_keys)
        self.assertEqual((after_size - before_size), 1)

    def test_save_notify_logs_cache_success_multiple_event(self):
        before_keys = self.r.scan()
        before_size = self.r.dbsize()
        event_data = json.load(open('tests/data/stream/logs_success_multiple_event.json'))
        res = run_logs_func(
            event = event_data,
            context= []
        )
        after_keys = self.r.scan()
        after_size = self.r.dbsize()
        self.assertNotEqual(before_keys, after_keys)
        self.assertEqual((after_size - before_size), 2)

    def test_save_notify_status_cache_invalid_request(self):
        res = run_status_func(
            event = "{",
            context = []
        )
        self.assertEqual(None, res)
        self.assertRaises(TypeError)

    def test_save_notify_status_cache_success_single_event(self):
        before_keys = self.r.scan()
        before_size = self.r.dbsize()
        event_data = json.load(open('tests/data/stream/status_success_single_event.json'))
        res = run_status_func(
            event = event_data,
            context= []
        )
        after_keys = self.r.scan()
        after_size = self.r.dbsize()
        self.assertNotEqual(before_keys, after_keys)
        self.assertEqual((after_size - before_size), 1)

    def test_save_notify_status_cache_success_multiple_event(self):
        before_keys = self.r.scan()
        before_size = self.r.dbsize()
        event_data = json.load(open('tests/data/stream/status_success_multiple_event.json'))
        res = run_status_func(
            event = event_data,
            context= []
        )
        after_keys = self.r.scan()
        after_size = self.r.dbsize()
        self.assertNotEqual(before_keys, after_keys)
        self.assertEqual((after_size - before_size), 2)
