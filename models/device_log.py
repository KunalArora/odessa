from boto3.dynamodb.conditions import Key
import concurrent.futures
from constants.odessa_response_codes import *
from constants.oids import CHARSET_OID
from datetime import timedelta
from functions import helper
from helpers import time_functions
import logging
from models.base import Base
from os import environ
from pymib.parse import parse


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
                        'timestamp': time_functions.time_convert(data['timestamp']),
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

    def parse_log_data(self, data, ignore_features=None):
        if ignore_features is None:
            ignore_features = []
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
                    if feat not in ignore_features:
                        res = helper.create_feature_format(
                            SUCCESS, feat, feat_data, val['timestamp']
                        )
                        response.append(res)
        return(response)

    # Retrieve latest value for a particular device_id and object_id
    # in a particular time interval
    def get_latest_log_in_interval(self, table_id, db_query_params):
        return self.table.query(
            KeyConditionExpression=Key('id').eq(table_id) &
            Key('timestamp').between(
                db_query_params['from_time'], db_query_params['to_time']),
            ScanIndexForward=False, Limit=1)

    # Retrieve all values for a particular device_id and object_id in a
    # particular time interval
    def get_all_logs_in_interval(self, table_id, db_query_params):
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

    def get_log_history(self, params, object_id_list, original_feature_list):
        feature_response = []

        device_id = params['device_id']
        from_time = params['from_time_unit']
        to_time = params['to_time_unit']
        time_unit = params['time_unit']

        parsed_from_time = time_functions.parse_time(from_time)
        parsed_to_time = time_functions.parse_time(to_time)

        # Get the charset record of the device from the Database
        charset = self.get_charset(device_id)

        if (  # For reducing response time in case of Hourly data
            time_unit == time_functions.HOURLY and
                (parsed_to_time - parsed_from_time) > timedelta(days=7)):
            # Break the time period into smaller periods based on threshold value
            time_periods = time_functions.break_time_period(
                from_time, to_time, int(environ['THRESHOLD_TIME_UNIT_BOC']))

            for period in time_periods:
                db_query_params = {
                    'from_time': time_functions.unparse_time(period['start_time']),
                    'to_time': time_functions.unparse_time(period['end_time'])
                }

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    futures = {
                        executor.submit(
                            self.get_all_logs_in_interval, device_id + '#' + key, db_query_params): key for key in object_id_list.keys()}
                    for future in concurrent.futures.as_completed(futures):
                        if future.result():
                            records = future.result()
                            start_unit = period['start_time']
                            end_unit = start_unit + timedelta(hours=1)
                            child_list = []
                            while (start_unit <= period['end_time']):
                                db_res = {}
                                for item in records:
                                    if start_unit <= time_functions.parse_time(
                                            item['timestamp']) < end_unit:
                                        child_list.append(item)

                                if child_list:
                                    required_item = child_list[-1]
                                    child_list = []
                                    object_id = required_item['id'].split('#')[
                                        1]
                                    db_res.update({object_id: required_item})

                                start_unit = end_unit
                                end_unit = start_unit + timedelta(hours=1)

                                # Parse the retrieved data
                                if db_res:
                                    if charset:
                                        db_res.update(
                                            {CHARSET_OID: charset})
                                    feature_response.extend(
                                        self.parse_oid_value_for_history(
                                            object_id_list, original_feature_list, db_res))

        else:  # Normal Approach for BOC devices
            # Break the time period into smaller periods
            time_periods = time_functions.break_time_period(
                from_time, to_time, time_unit)

            for period in time_periods:
                db_res = {}
                db_query_params = {
                    'from_time': time_functions.unparse_time(period['start_time']),
                    'to_time': time_functions.unparse_time(period['end_time'])
                }

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    futures = {
                        executor.submit(
                            self.get_latest_log_in_interval, device_id + '#' + key, db_query_params): key for key in object_id_list.keys()}
                    for future in concurrent.futures.as_completed(futures):
                        if future.result()['Items']:
                            record = future.result()['Items'][0]
                            object_id = record['id'].split('#')[1]
                            item = {object_id: record}
                            db_res.update(item)

                # Parse the retrieved data
                if db_res:
                    if charset:
                        db_res.update({CHARSET_OID: charset})
                    feature_response.extend(
                        self.parse_oid_value_for_history(
                            object_id_list, original_feature_list, db_res))

        return feature_response

    # Parse oid values and send back required features' values
    def parse_oid_value_for_history(
            self, object_id_list, original_feature_list, log_data):
        response = []
        parse_res = parse(log_data)

        for key, val in parse_res.items():
            if 'error' in val:
                logger.warning(
                    "Exception generated from parser in "
                    "handler:get_history_logs for "
                    f"id {val['id']} having value {val['value']} "
                    f"and error {val['error']}"
                )
                object_id = val['id'].split('#')[1]
                val['features'] = {}
                for feature in object_id_list[object_id]:
                    val['features'].update({feature: None})
                response.append(val)
            else:
                # Rename key for naming convenience
                val['features'] = val.pop('value')
                response.append(val)

        # Filter the features which are in the original list
        for item in response:
            item['features'] = {
                key: val for key, val in item[
                    'features'].items() if key in original_feature_list}

        return response
