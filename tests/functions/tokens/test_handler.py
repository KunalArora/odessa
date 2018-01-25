import unittest
import json
from functions.tokens import handler


class TestOneTimeTokens(unittest.TestCase):

    def test_get_token(self):
        res = json.loads(handler.get_one_time_token('', '')['body'])
        self.assertTrue('session_token' in res)
        self.assertTrue('secret_access_key' in res)
        self.assertTrue('access_key_id' in res)
        self.assertTrue('expiration' in res)
