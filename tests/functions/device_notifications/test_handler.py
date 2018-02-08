import json
import boto3
import unittest
import logging
from functions.device_notifications import handler
from functions import helper
from tests.functions import test_helper
from unittest.mock import patch
from models.device_log import DeviceLog
from models.device_network_status import DeviceNetworkStatus
from botocore.exceptions import ConnectionError
from boto3.dynamodb.conditions import Key

def run_logs_func(**keyword_args):
    return handler.save_notify_logs_db(
        keyword_args['event'],
        keyword_args['context'])


def run_status_func(**keyword_args):
    return handler.save_notify_status_db(
        keyword_args['event'],
        keyword_args['context'])

class TestDeviceNotificationsHandler(unittest.TestCase):
    def setUp(self):
        self.dynamodb = boto3.resource(
            "dynamodb", endpoint_url="http://localhost:8000")
        test_helper.set_env_var(self)
        test_helper.seed_ddb_device_notifications(self)
        test_helper.seed_ec_device_notifications(self)
        logging.getLogger('device_notifications').setLevel(100)

    def tearDown(self):
        test_helper.clear_cache(self)
        test_helper.clear_db(self)
        test_helper.create_table(self)

    def test_save_notify_logs_db_invalid_request(self):
        res = run_logs_func(
            event= { "body" : "{"
            },
            context=[]
        )
        self.assertRaises(ValueError)
        res_json = json.loads(res['body'])
        self.assertEqual(400, res_json['code'])
        self.assertEqual('Bad Request', res_json['message'])

    def test_save_notify_logs_db_device_null(self):
        res = run_logs_func(
            event= { "body" : "{\\x22notification\\x22:""[{\\x22object_id\\x22:"
                "\\x221.3.6.1.4.1.2435.2.3.9.4.2.1.5.5.20.0\\x22,"
                "\\x22value\\x22:\\x22820100A00103890100730100860100770100FF\\x22,"
                "\\x22timestamp\\x22:\\x221499157335\\x22}]}"
            },
            context=[]
        )
        self.assertRaises(helper.DeviceIdParameterError)
        res_json = json.loads(res['body'])
        self.assertEqual(400, res_json['code'])
        self.assertEqual('Bad Request', res_json['message'])

    def test_save_notify_logs_db_device_empty(self):
        res = run_logs_func(
            event= { "body" : "{\\x22device_id\\x22:\\x22\\x22,"
                "\\x22notification\\x22:[{\\x22object_id\\x22:"
                "\\x221.3.6.1.4.1.2435.2.3.9.4.2.1.5.5.20.0\\x22,"
                "\\x22value\\x22:\\x22820100A00103890100730100860100770100FF\\x22,"
                "\\x22timestamp\\x22:\\x221499157335\\x22}]}"
            },
            context=[]
        )
        self.assertRaises(helper.DeviceIdParameterError)
        res_json = json.loads(res['body'])
        self.assertEqual(400, res_json['code'])
        self.assertEqual('Bad Request', res_json['message'])

    def test_save_notify_logs_db_notification_null(self):
        res = run_logs_func(
            event= { "body" : "{\\x22device_id\\x22:"
                "\\x22ffffffff-ffff-ffff-ffff-ffffffff0001\\x22}"
            },
            context=[]
        )
        self.assertRaises(helper.NotificationError)
        res_json = json.loads(res['body'])
        self.assertEqual(400, res_json['code'])
        self.assertEqual('Bad Request', res_json['message'])

    def test_save_notify_logs_db_notification_empty(self):
        res = run_logs_func(
            event= { "body" : "{\\x22device_id\\x22:"
                "\\x22ffffffff-ffff-ffff-ffff-ffffffff0001\\x22,"
                "\\x22notification\\x22:[]}"
            },
            context=[]
        )
        self.assertRaises(helper.NotificationError)
        res_json = json.loads(res['body'])
        self.assertEqual(400, res_json['code'])
        self.assertEqual('Bad Request', res_json['message'])

    def test_save_notify_logs_db_success(self):
        before = self.dynamodb.Table('device_logs').scan()
        res = run_logs_func(
            event= { "body" : "{\\x22device_id\\x22:"
                "\\x22ffffffff-ffff-ffff-ffff-ffffffff0001\\x22,"
                "\\x22notification\\x22:"
                "[{\\x22object_id\\x22:\\x221.3.6.1.2.1.1.4.0\\x22,"
                "\\x22value\\x22:\\x2262hegfjbfguygf967654E666F726D6174796B406575726F6B61732E706C\\x22,"
                "\\x22timestamp\\x22:1499157335},"
                "{\\x22object_id\\x22:\\x221.3.6.1.4.1.2435.2.3.9.4.2.1.5.5.20.0\\x22,"
                "\\x22value\\x22:\\x22820100A00103890100730100860100770100FF\\x22,"
                "\\x22timestamp\\x22:1499157335},"
                "{\\x22object_id\\x22:\\x221.3.6.1.4.1.2435.2.3.9.4.2.1.5.5.8.0\\x22,"
                "\\x22value\\x22:\\x22630104000000011101040000ACE6410104000004B0310104000000026F0104000001908101040000000A86010400000007670104000000016B0104000021985401040000000166010400000001350104000000016A0104000021986C0104000027106D010400002198FF\\x22,"
                "\\x22timestamp\\x22:1499157335},"
                "{\\x22object_id\\x22:\\x221.3.6.1.4.1.2435.2.3.9.4.2.1.5.5.8.0\\x22,"
                "\\x22value\\x22:\\x22630104000000011101040000ACE6410104000004B0310104000000026F0104000001908101040000000A86010400000007670104000000016B0104000021985401040000000166010400000001350104000000016A0104000021986C0104000027106D010400002198FF\\x22,"
                "\\x22timestamp\\x22:1499157335},"
                "{\\x22object_id\\x22:\\x221.3.6.1.4.1.2435.2.3.9.4.2.1.5.5.20.0\\x22,"
                "\\x22value\\x22:\\x22820100A00103890100730100860100770100FF\\x22,"
                "\\x22timestamp\\x22:1499157335}]}"
            },
            context=[]
        )
        after = self.dynamodb.Table('device_logs').scan()
        self.assertNotEqual(before, after)
        res_json = json.loads(res['body'])
        self.assertEqual(200, res_json['code'])
        self.assertEqual('Success', res_json['message'])

    @patch.object(DeviceLog, 'put_logs')
    def test_save_notify_logs_db_database_connection_error(self, mock):
        mock.side_effect = ConnectionError
        res = run_logs_func(
            event= { "body" : "{\\x22device_id\\x22:"
                "\\x22ffffffff-ffff-ffff-ffff-ffffffff0001\\x22,"
                "\\x22notification\\x22:"
                "[{\\x22object_id\\x22:\\x221.3.6.1.2.1.1.4.0\\x22,"
                "\\x22value\\x22:\\x2262hegfjbfguygf967654E666F726D6174796B406575726F6B61732E706C\\x22,"
                "\\x22timestamp\\x22:1499157335},"
                "{\\x22object_id\\x22:\\x221.3.6.1.4.1.2435.2.3.9.4.2.1.5.5.20.0\\x22,"
                "\\x22value\\x22:\\x22820100A00103890100730100860100770100FF\\x22,"
                "\\x22timestamp\\x22:1499157335},"
                "{\\x22object_id\\x22:\\x221.3.6.1.4.1.2435.2.3.9.4.2.1.5.5.8.0\\x22,"
                "\\x22value\\x22:\\x22630104000000011101040000ACE6410104000004B0310104000000026F0104000001908101040000000A86010400000007670104000000016B0104000021985401040000000166010400000001350104000000016A0104000021986C0104000027106D010400002198FF\\x22,"
                "\\x22timestamp\\x22:1499157335},"
                "{\\x22object_id\\x22:\\x221.3.6.1.4.1.2435.2.3.9.4.2.1.5.5.8.0\\x22,"
                "\\x22value\\x22:\\x22630104000000011101040000ACE6410104000004B0310104000000026F0104000001908101040000000A86010400000007670104000000016B0104000021985401040000000166010400000001350104000000016A0104000021986C0104000027106D010400002198FF\\x22,"
                "\\x22timestamp\\x22:1499157335},"
                "{\\x22object_id\\x22:\\x221.3.6.1.4.1.2435.2.3.9.4.2.1.5.5.20.0\\x22,"
                "\\x22value\\x22:\\x22820100A00103890100730100860100770100FF\\x22,"
                "\\x22timestamp\\x22:1499157335}]}"
            },
            context=[]
        )
        self.assertRaises(ConnectionError)
        res_json = json.loads(res['body'])
        self.assertTrue(2, len(res_json))
        self.assertEqual(561, res_json['code'])
        self.assertEqual('Failed to connect with DB', res_json['message'])


    def test_save_notify_status_db_invalid_request(self):
        res = run_status_func(
            event= "{",
            context=[]
        )
        self.assertEqual(None, res)
        self.assertRaises(TypeError)

    def test_save_notify_status_db_device_null(self):
        res = run_status_func(
            event={"Records": [{"Sns": {"Timestamp": "2016-06-30T11:30:"
            "23.345Z", "Message": "[{\"service_name\":\"BAS\",\"event\":"
            "\"online_hook\",\"timestamp\":\"1498731372\"}]"}}]
            },
            context=[]
        )
        self.assertEqual(None, res)
        self.assertRaises(helper.DeviceIdParameterError)

    def test_save_notify_status_db_device_empty(self):
        res = run_status_func(
            event={"Records": [{"Sns": {"Timestamp": "2016-06-30T11:30:"
            "23.345Z","Message": "[{\"device_id\":\"\",\"service_name\":\"BAS\","
            "\"event\":\"online_hook\",\"timestamp\":\"1498731372\"}]"}}]
            },
            context=[]
        )
        self.assertEqual(None, res)
        self.assertRaises(helper.DeviceIdParameterError)

    def test_save_notify_status_db_event_null(self):
        res = run_status_func(
            event={"Records": [{"Sns": {"Timestamp": "2016-06-30T11:30:"
            "23.345Z","Message": "[{\"device_id\":"
            "\"babeface-f548-4a64-8266-f08d2acb416c\","
            "\"service_name\":\"BAS\",\"timestamp\":\"1498731372\"}]"}}]
            },
            context=[]
        )
        self.assertEqual(None, res)
        self.assertRaises(helper.EventParameterError)

    def test_save_notify_status_db_event_empty(self):
        res = run_status_func(
            event={"Records": [{"Sns": {"Timestamp": "2016-06-30T11:30:"
            "23.345Z","Message": "[{\"device_id\":"
            "\"babeface-f548-4a64-8266-f08d2acb416c\",\"service_name\":"
            "\"BAS\",\"event\":\"\",\"timestamp\":\"1498731372\"}]"}}]
            },
            context=[]
        )
        self.assertEqual(None, res)
        self.assertRaises(helper.EventParameterError)

    def test_save_notify_status_db_message_null(self):
        res = run_status_func(
            event={"Records": [{"Sns": {"Timestamp": "2016-06-30T11:30:"
            "23.345Z", "Message":"[]"}}]
            },
            context=[]
        )
        self.assertEqual(None, res)
        self.assertRaises(helper.NotificationError)

    def test_save_notify_status_db_success_single_device(self):
        before = self.dynamodb.Table('device_network_statuses').scan()
        res = run_status_func(
            event={"Records": [{"Sns": {"Timestamp": "2016-06-30T11:30:"
            "23.345Z","Message": "[{\"device_id\":"
            "\"babeface-f548-4a64-8266-f08d2acb416c\",\"service_name\":\"BAS\","
            "\"event\":\"online_hook\",\"timestamp\":\"1498731372\"}]"}}]
            },
            context=[]
        )
        after = self.dynamodb.Table('device_network_statuses').scan()
        self.assertNotEqual(before, after)
        self.assertEqual((len(after['Items'])-len(before['Items'])), 1)

    def test_save_notify_status_db_success_multiple_device(self):
        before = self.dynamodb.Table('device_network_statuses').scan()
        res = run_status_func(
            event={"Records": [{"Sns": {"Timestamp": "2016-06-30T11:30:"
            "23.345Z","Message": "[{\"device_id\":"
            "\"babeface-f548-4a64-8266-f08d2acb416c\",\"service_name\":\"BAS\","
            "\"event\":\"online_hook\",\"timestamp\":\"1498731372\"},"
            " {\"device_id\":\"65749832-4567-1234-8765-987654321234\","
            "\"service_name\":\"CMPS\",\"event\":\"offline_hook\","
            "\"timestamp\":\"1495763472\"}]"}}]
            },
            context=[]
        )
        after = self.dynamodb.Table('device_network_statuses').scan()
        self.assertNotEqual(before, after)
        self.assertEqual((len(after['Items'])-len(before['Items'])), 2)

    def test_save_multiple_notify_status_db_single_device_overwrite(self):
        # Notification timestamp chronological
        before = self.dynamodb.Table('device_network_statuses').scan()
        run_status_func(
            event={"Records": [{"Sns": {"Timestamp": "2018-06-30T11:30:"
            "23.345Z","Message": "[{\"device_id\":"
            "\"babeface-f548-4a64-8266-f08d2acb416c\",\"service_name\":\"BAS\","
            "\"event\":\"online_hook\",\"timestamp\":\"1498731372\"}]"}}]
            },
            context=[]
        )
        run_status_func(
            event={"Records": [{"Sns": {"Timestamp": "2018-06-30T12:30:"
            "23.345Z","Message": "[{\"device_id\":"
            "\"babeface-f548-4a64-8266-f08d2acb416c\",\"service_name\":\"BAS\","
            "\"event\":\"offline_hook\",\"timestamp\":\"1498731372\"}]"}}]
            },
            context=[]
        )
        after = self.dynamodb.Table('device_network_statuses').scan()
        item = self.dynamodb.Table('device_network_statuses').query(
            KeyConditionExpression=Key('id').eq('babeface-f548-4a64-8266-f08d2acb416c')
        )['Items'][0]

        self.assertNotEqual(before, after)
        self.assertEqual((len(after['Items'])-len(before['Items'])), 1)
        self.assertEqual(item['status'], 'offline')

    def test_save_multiple_notify_status_db_single_device_ignore(self):
        # Notification timestamp not chronological
        before = self.dynamodb.Table('device_network_statuses').scan()
        run_status_func(
            event={"Records": [{"Sns": {"Timestamp": "2018-06-30T11:30:"
            "23.345Z","Message": "[{\"device_id\":"
            "\"babeface-f548-4a64-8266-f08d2acb416c\",\"service_name\":\"BAS\","
            "\"event\":\"online_hook\",\"timestamp\":\"1498731372\"}]"}}]
            },
            context=[]
        )
        run_status_func(
            event={"Records": [{"Sns": {"Timestamp": "2018-06-30T10:30:"
            "23.345Z","Message": "[{\"device_id\":"
            "\"babeface-f548-4a64-8266-f08d2acb416c\",\"service_name\":\"BAS\","
            "\"event\":\"offline_hook\",\"timestamp\":\"1498731372\"}]"}}]
            },
            context=[]
        )
        after = self.dynamodb.Table('device_network_statuses').scan()
        item = self.dynamodb.Table('device_network_statuses').query(
            KeyConditionExpression=Key('id').eq('babeface-f548-4a64-8266-f08d2acb416c')
        )['Items'][0]

        self.assertNotEqual(before, after)
        self.assertEqual((len(after['Items'])-len(before['Items'])), 1)
        self.assertEqual(item['status'], 'online')

    def test_save_notify_status_db_ec_present_device(self):
        before = self.dynamodb.Table('device_network_statuses').scan()
        res = run_status_func(
            event={"Records": [{"Sns": {"Timestamp": "2016-06-30T11:30:"
            "23.345Z","Message": "[{\"device_id\":"
            "\"ffffffff-ffff-ffff-ffff-ffffffff0001\",\"service_name\":\"BAS\","
            "\"event\":\"online_hook\",\"timestamp\":\"1484225106\"}]"}}]
            },
            context=[]
        )
        after = self.dynamodb.Table('device_network_statuses').scan()
        self.assertEqual((len(after['Items'])-len(before['Items'])), 0)

    @patch.object(DeviceNetworkStatus, 'put_status')
    def test_save_notify_status_db_database_connection_error(self, mock):
        mock.side_effect = ConnectionError
        res = run_status_func(
            event={"Records": [{"Sns": {"Timestamp": "2016-06-30T11:30:"
            "23.345Z","Message": "[{\"device_id\":"
            "\"babeface-f548-4a64-8266-f08d2acb416c\",\"service_name\":\"BAS\","
            "\"event\":\"online_hook\",\"timestamp\":\"1498731372\"}]"}}]
            },
            context=[]
        )
        self.assertEqual(None, res)
        self.assertRaises(ConnectionError)
