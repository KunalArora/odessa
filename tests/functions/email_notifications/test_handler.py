import logging
import unittest
import json
import boto3
from tests.functions import test_helper
from functions.email_notifications import handler
from unittest.mock import patch
from botocore.exceptions import ConnectionError
from botocore.exceptions import ClientError
from models.device_email_log import DeviceEmailLog

def run_func(**keyword_args):
	return handler.save_mail_report(
        keyword_args['event'],
        keyword_args['context'])

class EmailNotificationTestCase(unittest.TestCase):
	def setUp(self):
		self.dynamodb = boto3.resource(
		                'dynamodb', endpoint_url='http://localhost:8000')
		test_helper.set_env_var(self)
		test_helper.seed_s3(self)
#		logging.getLogger('email_notifications').setLevel(100)

	def tearDown(self):
		test_helper.delete_s3_bucket(self)
		test_helper.clear_db(self)
		test_helper.create_table(self)
		test_helper.seed_s3(self)

	def test_invalid_request(self):
		res = run_func(
			event= { "mail" : "{" },
			context= []
			)
		self.assertEqual(None, res)
		self.assertRaises(ValueError)

	@patch.object(DeviceEmailLog, 'create')
	def test_database_connection_error_on_email_notifications(self, mock):
		mock.side_effect = ConnectionError
		event_data = json.load(open('tests/data/email_notifications/successful_csv_event.json'))
		res = run_func (
			event= event_data,
			context= []
			)
		self.assertEqual(None, res)
		self.assertRaises(ConnectionError)

	def test_no_such_bucket(self):
		event_data = json.load(open('tests/data/email_notifications/no_such_bucket_event.json'))
		res = run_func(
			event= event_data,
			context= []
			)
		self.assertEqual(None, res)
		self.assertRaises(ClientError)

	def test_email_not_enabled_country(self):
		table = self.dynamodb.Table('device_email_logs')
		before_keys = table.scan()['Items']
		event_data = json.load(open('tests/data/email_notifications/email_not_enabled_country_event.json'))
		res = run_func(
			event = event_data,
			context = []
			)
		after_keys = table.scan()['Items']
		self.assertEqual(before_keys, after_keys)
		self.assertEqual((len(after_keys)-len(before_keys)), 0)

	def test_successful_csv_email_data(self):
		table = self.dynamodb.Table('device_email_logs')
		before_keys = table.scan()['Items']
		event_data = json.load(open('tests/data/email_notifications/successful_csv_event.json'))
		res = run_func(
			event = event_data,
			context = []
			)
		after_keys = table.scan()['Items']
		self.assertNotEqual(before_keys, after_keys)
		self.assertEqual((len(after_keys)-len(before_keys)), 1)
		self.assertTrue('serial_number' in after_keys[0])
		self.assertTrue('timestamp' in after_keys[0])

	def test_successful_xml_email_data(self):
		table = self.dynamodb.Table('device_email_logs')
		before_keys = table.scan()['Items']
		event_data = json.load(open('tests/data/email_notifications/successful_xml_event.json'))
		res = run_func(
			event = event_data,
			context = []
			)
		after_keys = table.scan()['Items']
		self.assertNotEqual(before_keys, after_keys)
		self.assertEqual((len(after_keys)-len(before_keys)), 1)
		self.assertTrue('serial_number' in after_keys[0])
		self.assertTrue('timestamp' in after_keys[0])


def main():
	unittest.main()

if __name__ == '__main__':
    main()
