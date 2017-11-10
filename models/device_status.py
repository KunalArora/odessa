from models.base import Base
import datetime
from botocore.exceptions import ClientError


class DeviceStatus(Base):
    def __init__(self):
        super().__init__()
        self.table = self.dynamodb.Table('device_statuses')

    def read(self, reporting_id, object_id):
        ddb_res = self.table.get_item(Key={
                'reporting_id': reporting_id,
                'object_id': object_id
            })
        if 'Item' in ddb_res:
            self.reporting_id = reporting_id
            self.object_id = object_id
            self.timestamp = ddb_res['Item']['timestamp']
            self.data = ddb_res['Item']['data']

            return self

    def insert(self, reporting_id, object_id, timestamp, data):
        created_at = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        self.table.put_item(
            Item={
                'reporting_id': reporting_id,
                'object_id': object_id,
                'timestamp': timestamp,
                'data': data,
                'created_at': created_at,
                'updated_at': created_at
            })
        self.reporting_id = reporting_id
        self.object_id = object_id
        self.timestamp = timestamp
        self.data = data

        return self

    def update(self, timestamp, data):
        updated_at = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        self.table.update_item(
            Key={
                'reporting_id': self.reporting_id,
                'object_id': self.object_id
            },
            ExpressionAttributeNames={'#d': 'data', '#t': 'timestamp'},
            UpdateExpression="set #d = :d, #t = :t, updated_at = :u",
            ExpressionAttributeValues={
                ':t': timestamp,
                ':d': data,
                ':u': updated_at
            }
        )
        self.timestamp = timestamp
        self.data = data

        return self

    def is_existing(self):
        return hasattr(self, 'reporting_id')
