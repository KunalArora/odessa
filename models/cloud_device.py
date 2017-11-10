from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr
from helpers import time_functions
from models.base import Base


class CloudDevice(Base):
    def __init__(self):
        super().__init__()
        self.table = self.dynamodb.Table('reporting_registrations')

    def read(self, device_id):
        result = self.table.query(
            IndexName='cloud_devices',
            KeyConditionExpression=Key('device_id').eq(device_id),
            Limit=1,
            ScanIndexForward=False
            )
        if result['Items']:
            self.reporting_id = result['Items'][0]['reporting_id']
            self.device_id = result['Items'][0]['device_id']
            self.log_service_id = result['Items'][0]['log_service_id']

    def is_existing(self):
        return hasattr(self, 'reporting_id')
