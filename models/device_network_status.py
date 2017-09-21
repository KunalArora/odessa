from boto3.dynamodb.conditions import Key
from helpers import time_functions
from models.base import Base


class DeviceNetworkStatus(Base):
    def __init__(self):
        super().__init__()

    def get_latest_status(self, device_id):
        #   Retrieve latest network status from either ElastiCache or DynamoDb.
        table = self.dynamodb.Table('device_network_statuses')
        if device_id:
            if(self.elasticache):
                redis_res = self.elasticache.hgetall(
                    "device_network_status:%s" % (device_id))
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
                time = time_functions.time_convert(data['timestamp'])
                if res and res['timestamp'] == time and res['status'] == status:
                    response.remove(data)
        return response

    def is_exists_db(self, notify_data):
        #   Verify if the notified data is already stored in Dynamodb or not.
        table = self.dynamodb.Table('device_network_statuses')
        response = []
        response.extend(notify_data)
        for data in notify_data:
            iso_time = time_functions.time_convert(data['timestamp'])
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
                        'timestamp': time_functions.time_convert(data['timestamp']),
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

    # Retrieve one previous record from the timestamp
    # value 'from_time' for the 'device_id'
    def get_one_previous_record(self, device_id, from_time):
        table = self.dynamodb.Table('device_network_statuses')
        db_res = table.query(
            KeyConditionExpression=Key('id').eq(device_id) &
            Key('timestamp').lt(from_time),
            ProjectionExpression="#ts, #st",
            ExpressionAttributeNames={"#ts": "timestamp", "#st": "status"},
            Limit=1
        )
        if db_res['Items']:
            return db_res['Items'][0]

    # Retrieve all statuses for a device in a particular time interval
    # Note: Status of the device at the 'from' point of time is also
    # evaluated and sent back as response in except one special case desc below
    def get_status_history(self, params):
        table = self.dynamodb.Table('device_network_statuses')
        result = []
        response = table.query(
            KeyConditionExpression=Key('id').eq(params['device_id']) &
            Key('timestamp').between(
                params['from_time'], params['to_time']
            ),
            ProjectionExpression="#ts, #st",
            ExpressionAttributeNames={"#ts": "timestamp", "#st": "status"}
        )

        # Special Case: If a record already exists at the timestamp
        # value 'params['from_time']', then don't find one previous record
        if response['Items'] and response['Items'][0]['timestamp'] == params['from_time']:
            result.extend(response['Items'])
        else:
            # Find out one previous record
            previous_record = self.get_one_previous_record(
                params['device_id'], params['from_time'])
            if previous_record:
                previous_record['timestamp'] = params['from_time']
                result.append(previous_record)
            else:
                result.append(
                    {'status': 'offline', 'timestamp': params['from_time']})

            result.extend(response['Items'])

        while 'LastEvaluatedKey' in response:
            response = table.query(
                KeyConditionExpression=Key('id').eq(params['device_id']) &
                Key('timestamp').between(
                    params['from_time'], params['to_time']),
                ProjectionExpression="#ts, #st",
                ExpressionAttributeNames={"#ts": "timestamp", "#st": "status"},
                ExclusiveStartKey=response['LastEvaluatedKey'])
            if response['Items']:
                result.extend(response['Items'])

        return result
