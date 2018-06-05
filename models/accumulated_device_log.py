from models.base import Base
import logging

logger = logging.getLogger('accumulated_device_logs')
logger.setLevel(logging.INFO)

class AccumulatedDeviceLog(Base):
    def __init__(self):
        super().__init__()
        self.table = self.dynamodb.Table('accumulated_device_logs')
    
    def read(self, device_id, object_id, timestamp):
        ddb_res = self.table.get_item(
            Key={
                'id': f"{device_id}#{object_id}",
                'year_month': timestamp[0:7]
                }
        )
        if 'Item' in ddb_res:
            self.device_id = device_id
            self.object_id = object_id
            self.year_month = ddb_res['Item']['year_month']
            self.accumulated_log = ddb_res['Item']['accumulated_log']
        else:
            return None

        return self

    def insert(self, device_id, object_id, timestamp, rawdata):
        self.table.put_item(
            Item = {
                'id' : f"{device_id}#{object_id}",
                'year_month' : timestamp[0:7],
                'accumulated_log' : [{'value': rawdata, 'timestamp': timestamp}]
            }
        )
    
    def update(self, accumulated_log):
        self.table.update_item(
            Key={
                'id': f"{self.device_id}#{self.object_id}",
                'year_month': self.year_month
            },
            ExpressionAttributeNames={'#a': 'accumulated_log'},
            UpdateExpression="set #a = :a",
            ExpressionAttributeValues={
                ':a': accumulated_log,
            }
        )

    def is_existing(self):
        return hasattr(self, 'device_id')