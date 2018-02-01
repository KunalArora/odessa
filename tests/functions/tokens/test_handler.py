import unittest
import json
import datetime
from functions.tokens import handler
from unittest.mock import patch
from botocore.exceptions import ClientError
from os import environ


class TestOneTimeTokens(unittest.TestCase):
    def setUp(self):
        environ['LAMBDA_ROLE_ARN'] = ''

    @patch('boto3.client')
    def test_get_token_success(self, mock):
        mock().assume_role.return_value = {
            'Credentials': {
                'SessionToken': 'session_token',
                'SecretAccessKey': 'secret_access_key',
                'AccessKeyId': 'access_key_id',
                'Expiration': datetime.datetime.now()
            }
        }
        res = json.loads(handler.get_one_time_token('', '')['body'])
        self.assertTrue('session_token' in res)
        self.assertTrue('secret_access_key' in res)
        self.assertTrue('access_key_id' in res)
        self.assertTrue('expiration' in res)

    @patch('boto3.client')
    def test_get_token_client_error(self, mock):
        mock().assume_role.side_effect = ClientError(
            {'Error': {'Code': '403', 'Message': 'Unauthorized'}}, 'AssumeRole'
        )
        res = handler.get_one_time_token('', '')

        self.assertTrue('statusCode' in res)
        self.assertTrue(res['statusCode'], 403)
