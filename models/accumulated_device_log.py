import logging
from models.base import Base
from boto3.dynamodb.conditions import Key
from pymib.parse import parse
from helpers import time_functions
from constants.oids import CHARSET_OID
from models.device_log import DeviceLog

logger = logging.getLogger('accumulated_device_logs')
logger.setLevel(logging.INFO)

class AccumulatedDeviceLog(Base):
    def __init__(self):
        super().__init__()
        self.table = self.dynamodb.Table('accumulated_device_logs')
    
    def get_log_history(self, params, object_id_list, original_feature_list):
        device_id = params['device_id']
        from_time = params['from_time_unit']
        to_time = params['to_time_unit']
        device_log = DeviceLog()

        # Get the charset record of the device from the Database
        charset = device_log.get_charset(device_id)

        for object_id in object_id_list.keys():
            logs_pre_parse = []
            logs_parsed = []

            # dynamodb query
            db_res = self.table.query(
                KeyConditionExpression=Key('id').eq(device_id + '#' + object_id) &
                Key('year_month').between(
                    from_time[0:7], to_time[0:7]),
                )
            # create data in order to parse value
            if db_res['Items']:
                for res in db_res['Items']:
                    if res['accumulated_log']:
                        for log in res['accumulated_log']:
                            # pick up data between from_time to to_time
                            if res['year_month'] == from_time[0:7] or res['year_month'] == to_time[0:7]:
                                if log['timestamp'] < from_time or to_time < log['timestamp']:
                                    continue
                            logs_pre_parse.append({
                                object_id: {
                                    'id': device_id + '#' + object_id,
                                    'value': log['value'],
                                    'timestamp': (log['timestamp'])
                                }
                            })
            
            # parse data
            if logs_pre_parse:
                for log in logs_pre_parse:
                    if charset:
                        log.update({CHARSET_OID: charset})
                    log_parsed = parse(log)
                    if log_parsed:
                        # Filter the features which are in the original list
                        log_parsed[object_id]['value'] = {
                            key: val for key, val in log_parsed[object_id]['value'].items() if key in original_feature_list
                            }
                        # rename key
                        log_parsed[object_id]['features'] = log_parsed[object_id].pop('value')
                    
                    logs_parsed.append(log_parsed[object_id])

        return logs_parsed

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