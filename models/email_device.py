from boto3.dynamodb.conditions import Key
from models.base import Base


class EmailDevice(Base):
    def __init__(self):
        super().__init__()
        self.table = self.dynamodb.Table('reporting_registrations')

    def read(self, serial_number):
        result = self.table.query(
            IndexName='email_devices',
            KeyConditionExpression=Key('serial_number').eq(serial_number),
            Limit=1,
            ScanIndexForward=False
            )
        if result['Items']:
            self.reporting_id = result['Items'][0]['reporting_id']
            self.serial_number = result['Items'][0]['serial_number']
            self.log_service_id = result['Items'][0]['log_service_id']

    def is_existing(self):
        return hasattr(self, 'reporting_id')
