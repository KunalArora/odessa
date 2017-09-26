import unittest
from models.service_oid import ServiceOid
from tests.functions import test_helper


class TestServiceOid(unittest.TestCase):
    def setUp(self):
        test_helper.create_table(self)
        test_helper.seed_ddb_device_settings(self)

    def tearDown(self):
        test_helper.clear_db(self)

    def test_ids(self):
        self.assertEqual(ServiceOid.ids(), ['0'])
