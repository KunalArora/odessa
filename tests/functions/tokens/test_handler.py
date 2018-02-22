import freezegun
import unittest
import json
import logging
import os
from functions.tokens import handler


class TestOneTimeTokens(unittest.TestCase):
    def setUp(self):
        os.environ['ONETIME_SECRET'] = 'secret'
        logging.getLogger('tokens').setLevel(100)

    def test_onetime_token_success(self):
        token = json.loads(handler.get_one_time_token('', '')['body'])
        policy = handler.auth({
            'authorizationToken': f'Bearer {token["session_token"]}',
            'methodArn': '/foo/bar'}, {})
        self.assertEqual(policy['policyDocument']['Statement'][0], {
            'Action': 'execute-api:Invoke',
            'Effect': 'Allow',
            'Resource': '/foo/bar'})

    def test_onetime_token_expired(self):
        with freezegun.freeze_time('2018-01-01 00:00:00'):
            token = json.loads(handler.get_one_time_token('', '')['body'])
        policy = handler.auth({
            'authorizationToken': f'Bearer {token["session_token"]}',
            'methodArn': '/foo/bar'}, {})
        self.assertEqual(policy['policyDocument']['Statement'][0], {
            'Action': 'execute-api:Invoke',
            'Effect': 'Deny',
            'Resource': '/foo/bar'})

    def test_onetime_token_invalid(self):
        policy = handler.auth({
            'authorizationToken': 'Bearer x.x.x',
            'methodArn': '/foo/bar'}, {})
        self.assertEqual(policy['policyDocument']['Statement'][0], {
           'Action': 'execute-api:Invoke',
           'Effect': 'Deny',
           'Resource': '/foo/bar'})
