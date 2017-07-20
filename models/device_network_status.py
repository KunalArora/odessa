from boto3.dynamodb.conditions import Key
from models.base import Base


class DeviceNetworkStatus(Base):
    def __init__(self):
        super().__init__()

    def get_latest_status(self, device_id):
        #   Retrieve latest network status from either ElastiCache or DynamoDb.
        table = self.dynamodb.Table('device_network_statuses')
        if device_id:
            if(self.elasticache):
                redis_res = self.elasticache.hgetall("device_network_status:%s" % (device_id))
                if redis_res:
                    return(super().convert(redis_res))
            db_res = table.query(
                Limit=1,
                ScanIndexForward=False,
                KeyConditionExpression=Key('id').eq(device_id)
            )['Items']
            return (db_res)

    def is_exists_cache(self, notify_data):
        #   Verify if the notified data is already stored in ElastiCache or not.
        response = []
        response.extend(notify_data)
        if(self.elasticache):
            for data in notify_data:
                res = self.elasticache.hgetall("device_network_status:%s" %
                                     (data['device_id']))
                res = super().convert(res)
                status = (data['event'].split('_')[0])
                time = super().time_convert(data['timestamp'])
                if res and res['timestamp'] == time and res['status'] == status:
                    response.remove(data)
        return response

    def is_exists_db(self, notify_data):
        #   Verify if the notified data is already stored in Dynamodb or not.
        table = self.dynamodb.Table('device_network_statuses')
        response = []
        response.extend(notify_data)
        for data in notify_data:
            iso_time = super().time_convert(data['timestamp'])
            res = table.query(
                KeyConditionExpression=Key('id').eq(
                    data['device_id']) & Key('timestamp').eq(iso_time)
            )['Items']
            status = (data['event'].split('_')[0])
            if res and res[0]['status'] == status:
                response.remove(data)
        return response

    def put_status(self, notify_data):
        #   Save the network status in the DynamoDb database for a particular device.
        table = self.dynamodb.Table('device_network_statuses')
        with table.batch_writer(overwrite_by_pkeys=['id', 'timestamp']) as batch:
            for data in notify_data:
                status = (data['event'].split('_')[0])
                batch.put_item(
                    Item={
                        'id': (data['device_id']),
                        'timestamp': super().time_convert(data['timestamp']),
                        'status': status,
                    }
                )

    def update_status(self, notified_event):
        #   Update the latest network status (Online or Offline) for
        #   the particular device in ElastiCache.
        if(self.elasticache):
            for data in notified_event['Records']:
                device_id = (data['dynamodb']['Keys']['id']['S'])
                timestamp = (data['dynamodb']['NewImage']['timestamp']['S'])
                status = (data['dynamodb']['NewImage']['status']['S'])
                self.elasticache.hmset("device_network_status:%s" % (device_id),
                             {
                    'id': device_id,
                    'timestamp': timestamp,
                    'status': status,
                })
