from boto3.dynamodb.conditions import Key
from models.base import Base
from constants.oids import CHARSET_OID


class DeviceLog(Base):
    def __init__(self):
        super().__init__()

    def get_latest_logs(self, subscribed_data):
#        Retrieve latest logs from either ElastiCache or DynamoDb.
        table = self.dynamodb.Table('device_logs')
        db_res = []
        if(self.elasticache):
            redis_res = []
            for data in subscribed_data['Items']:
                device_id = (data['id'].split('#')[0])
                res = self.elasticache.hgetall("device_log:%s" %
                                     (device_id + '#' + data['oid']))
                if not res:
                    break
                else:
                    redis_res.append(super().convert(res))
            if len(redis_res) == len(subscribed_data):
                return ({'Items': redis_res})
        for data in subscribed_data['Items']:
            device_id = (data['id'].split('#')[0])
            res = table.query(
                Limit=1,
                ScanIndexForward=False,
                KeyConditionExpression=Key('id').eq(
                    device_id + '#' + data['oid'])
            )
            if res['Items']:
                db_res.append(res['Items'][0])
        return ({'Items': db_res})

    def is_exists_cache(self, notify_data):
#        Verify if the notified data is already stored in ElastiCache or not.
        response = []
        response.extend(notify_data['notification'])
        if(self.elasticache):
            for data in notify_data['notification']:
                res = self.elasticache.hgetall("device_log:%s" % (
                    notify_data['device_id'] + '#' + data['object_id']))
                res = super().convert(res)
                if res and res['value'] == data['value']:
                    response.remove(data)
        notify_data['notification'] = response
        return notify_data

    def is_exists_db(self, notify_data):
#        Verify if the notified data is already stored in Dynamodb or not.
        table = self.dynamodb.Table('device_logs')
        response = []
        for data in notify_data['notification']:
            res = table.query(
                Limit=1,
                ScanIndexForward=False,
                KeyConditionExpression=Key('id').eq(
                    notify_data['device_id'] + '#' + data['object_id']
                )
            )['Items']
            if not data['value']:
                data['value'] = " "
            if not res:
                response.append(data)
            elif res and res[0]['value'] != data['value']:
                response.append(data)
        notify_data['notification'] = response
        return notify_data

    def put_logs(self, notify_data):
#        Save the logs in the DynamoDb database for a particular device.
        table = self.dynamodb.Table('device_logs')
        with table.batch_writer(overwrite_by_pkeys=['id', 'timestamp']) as batch:
            for data in notify_data['notification']:
                batch.put_item(
                    Item={
                        'id': (notify_data['device_id'] + '#' +
                               data['object_id']),
                        'timestamp': super().time_convert(data['timestamp']),
                        'value': (data['value']) if data['value'] else " ",
                    }
                )

    def update_logs(self, notified_event):
#        Update the latest logs for the particular device in ElastiCache.
        if(self.elasticache):
            for data in notified_event['Records']:
                log_id = data['dynamodb']['Keys']['id']['S']
                timestamp = data['dynamodb']['NewImage']['timestamp']['S']
                val = data['dynamodb']['NewImage']['value']['S']
                value = val if val != " " else ''
                self.elasticache.hmset("device_log:%s" % (log_id),
                             {
                    'id': log_id,
                    'timestamp': timestamp,
                    'value': value,
                })

    def get_charset(self, device_id):
        object_id = CHARSET_OID
        table = self.dynamodb.Table('device_logs')
        if(self.elasticache):
            res = self.elasticache.hgetall("device_log:%s" % (device_id + '#' + object_id))
            if res:
                return (super().convert(res)['value'])
        res = table.query(
            KeyConditionExpression=Key('id').eq(device_id + '#' + object_id)
        )
        if res['Items']:
            return(res['Items'][0]['value'])