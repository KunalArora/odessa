from boto3.dynamodb.conditions import Key
from helpers import time_functions
from models.base import Base


class ReportingRegistration(Base):
    def __init__(self):
        super().__init__()
        self.table = self.dynamodb.Table('reporting_registrations')

    def create(self, data):
        store_data = {}
        store_data['reporting_id'] = data['reporting_id']
        store_data['timestamp'] = time_functions.current_utc_time()
        store_data['communication_type'] = data['communication_type']
        if data['communication_type'] == 'cloud':
            store_data['device_id'] = data['device_id']
        else:
            store_data['serial_number'] = data['serial_number']
        self.table.put_item(
            Item=store_data)

    def read(self, reporting_id):
        result = self.table.query(
            KeyConditionExpression=Key('reporting_id').eq(
                reporting_id))
        if result['Items']:
            return result['Items']
