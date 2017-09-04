from boto3.dynamodb.conditions import Key
from calendar import monthrange
from constants.odessa_response_codes import *
from constants.oids import CHARSET_OID
from datetime import datetime
from datetime import timedelta
from functions import helper
from constants.odessa_response_codes import *
from pymib.parse import parse
import concurrent.futures
import logging
from models.base import Base

logger = logging.getLogger('device_logs')
logger.setLevel(logging.INFO)


class DeviceLog(Base):
    def __init__(self):
        super().__init__()
        self.table = self.dynamodb.Table('device_logs')

    def get_log(self, data, device_id):
        table = self.dynamodb.Table('device_logs')
        key = device_id + '#' + data['oid']
        return table.query(
            Limit=1,
            ScanIndexForward=False,
            KeyConditionExpression=Key('id').eq(key))

    def get_latest_logs(self, subscribed_data):
        #   Retrieve latest logs from either ElastiCache or DynamoDb.
        table = self.dynamodb.Table('device_logs')
        db_res = []
        if subscribed_data:
            device_id = (subscribed_data[0]['id'].split('#')[0])
            if(self.elasticache):
                redis_res = []
                for data in subscribed_data[0]['oids']:
                    res = self.elasticache.hgetall("device_log:%s" %
                                                   (device_id + '#' + data['oid']))
                    if not res:
                        break
                    else:
                        redis_res.append(super().convert(res))
                if len(redis_res) == len(subscribed_data):
                    return ({'Items': redis_res})
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future_to_log = {executor.submit(
                    self.get_log, data, device_id): data for data in subscribed_data[0]['oids']}
                for future in concurrent.futures.as_completed(future_to_log):
                    if 'Items' in future.result() and future.result()['Items']:
                        db_res.append(future.result()['Items'][0])
        return ({'Items': db_res})

    def is_exists_cache(self, notify_data):
        #   Verify if the notified data is already stored in ElastiCache or not.
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
        #   Verify if the notified data is already stored in Dynamodb or not.
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
        #   Save the logs in the DynamoDb database for a particular device.
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
        #   Update the latest logs for the particular device in ElastiCache.
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
        if(self.elasticache):
            res = self.elasticache.hgetall(
                "device_log:%s" % (device_id + '#' + object_id))
            if res:
                return (super().convert(res)['value'])
        res = self.table.query(
            KeyConditionExpression=Key('id').eq(device_id + '#' + object_id)
        )
        if res['Items']:
            return(res['Items'][0])

    def parse_log_data(self, data):
        parse_data = {}
        response = []
        for item in data['Items']:
            object_id = (item['id'].split('#')[1])
            parse_data[object_id] = item
        parse_res = parse(parse_data)
        for key, val in parse_res.items():
            if 'error' in val:
                logging.warning(
                    "Exception generated from parser in models:device_log for "
                    "id {} having value {} and error {}".format(
                        val['id'], val['value'], val['error'])
                )
                res = helper.create_feature_format(
                    INTERNAL_SERVER_ERROR, key, val['value'], '', message=val['error']
                )
                response.append(res)
            else:
                for feat, feat_data in val['value'].items():
                    res = helper.create_feature_format(
                        SUCCESS, feat, feat_data, val['timestamp']
                    )
                    response.append(res)
        return(response)

    # Retrieve latest value for a particular device_id and object_id
    # in a particular time interval
    def get_history_logs_query(self, table_id, db_query_params):
        return self.table.query(
            KeyConditionExpression=Key('id').eq(table_id) &
            Key('timestamp').between(
                db_query_params['from_time'], db_query_params['to_time']),
            ScanIndexForward=False, Limit=1)

    # Retrieve all values for a particular device_id and object_id in a
    # particular time interval
    def get_history_logs_scan(self, table_id, db_query_params):
        response = self.table.query(
            KeyConditionExpression=Key('id').eq(table_id) &
            Key('timestamp').between(
                db_query_params['from_time'], db_query_params['to_time']))
        result = []

        if not response['Items']:
            return
        else:
            result.extend(response['Items'])

        while 'LastEvaluatedKey' in response:
            response = self.table.query(
                KeyConditionExpression=Key('id').eq(table_id) &
                Key('timestamp').between(
                    db_query_params['from_time'], db_query_params['to_time']),
                ExclusiveStartKey=response['LastEvaluatedKey'])
            if response['Items']:
                result.extend(response['Items'])

        return result

    # Breaks the time period into smaller intervals based on the time_unit
    def break_time_period(self, from_time, to_time, time_unit):
        time_periods = []

        # Initialize the first time period
        start_time = self.parse_time(from_time)
        end_time = self.get_end_time(start_time, time_unit)

        to_time_parsed = self.parse_time(to_time)

        first_item = {
            'start_time': start_time, 'end_time': end_time}
        time_periods.append(first_item)
        # Break the time period into successive smaller periods
        while (start_time <= to_time_parsed):
            start_time = end_time + timedelta(seconds=1)
            if start_time > to_time_parsed:
                continue
            end_time = self.get_end_time(
                start_time, time_unit)
            if end_time > to_time_parsed:
                end_time = to_time_parsed
            item = {
                'start_time': start_time, 'end_time': end_time}
            time_periods.append(item)

        return time_periods

    # Returns the end time for time periods
    # For example: If date_time = 2017-01-01 22:10:45 and time_unit = HOURLY,
    # end_time = 2017-01-01 22:59:59
    # Similarly: If date_time = 2017-01-01 22:10:45 and time_unit = DAILY,
    # end_time = 2017-01-01 23:59:59
    # Also: If date_time = 2017-01-01 22:10:45 and time_unit = MONTHLY,
    # end_time = 2017-01-31 23:59:59
    def get_end_time(self, date_time, time_unit):
        if time_unit == helper.HOURLY:
            response = datetime(date_time.year, date_time.month,
                                date_time.day, date_time.hour, 59, 59)
        elif time_unit == helper.DAILY:
            response = datetime(
                date_time.year, date_time.month, date_time.day, 23, 59, 59)
        elif time_unit == helper.MONTHLY:
            last_day = monthrange(date_time.year, date_time.month)[1]
            response = datetime(
                date_time.year, date_time.month, last_day, 23, 59, 59)
        else:  # Time Unit = Threshold value
            response = datetime(
                date_time.year, date_time.month,
                date_time.day, date_time.hour, 59, 59) + timedelta(
                    hours=(time_unit - 1))
        return response

    def parse_time(self, unparsed_time):
        return datetime.strptime(unparsed_time, "%Y-%m-%dT%H:%M:%S+00:00")

    def unparse_time(self, parsed_time):
        return datetime.strftime(parsed_time, "%Y-%m-%dT%H:%M:%S")

    # Parsing value without timezone consideration
    def parse_time_wo_tz(self, unparsed_time):
        return datetime.strptime(unparsed_time, "%Y-%m-%dT%H:%M:%S")
