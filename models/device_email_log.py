from boto3.dynamodb.conditions import Key
from datetime import timedelta
from helpers import time_functions
from models.base import Base
from os import environ


class DeviceEmailLog(Base):
    def __init__(self):
        super().__init__()
        self.table = self.dynamodb.Table('device_email_logs')

    def create(self, mail_data):
        self.table.put_item(Item=mail_data)

    def get_latest_log_in_interval(
            self, db_query_params, original_feature_list):
        expression_attribute_names = {"#ts": "timestamp"}
        projection_expression = ['#ts']
        for i, feature in enumerate(original_feature_list):
            expression_attribute_names[f"#{i}"] = feature
            projection_expression.append(f"#{i}")
        db_res = self.table.query(
            KeyConditionExpression=Key('serial_number').eq(
                db_query_params['serial_number']) &
            Key('timestamp').between(
                db_query_params['from_time'], db_query_params['to_time']
            ),
            ProjectionExpression=', '.join(projection_expression),
            ExpressionAttributeNames=expression_attribute_names,
            ScanIndexForward=False, Limit=1
        )
        if db_res['Items']:
            return db_res['Items'][0]

    def get_all_logs_in_interval(
            self, db_query_params, original_feature_list):
        expression_attribute_names = {"#ts": "timestamp"}
        projection_expression = ['#ts']
        for i, feature in enumerate(original_feature_list):
            expression_attribute_names[f"#{i}"] = feature
            projection_expression.append(f"#{i}")
        response = self.table.query(
            KeyConditionExpression=Key('serial_number').eq(
                db_query_params['serial_number']) &
            Key('timestamp').between(
                db_query_params['from_time'], db_query_params['to_time']),
            ProjectionExpression=', '.join(projection_expression),
            ExpressionAttributeNames=expression_attribute_names
        )
        result = []

        if not response['Items']:
            return
        else:
            result.extend(response['Items'])

        while 'LastEvaluatedKey' in response:
            response = self.table.query(
                KeyConditionExpression=Key('serial_number').eq(
                    db_query_params['serial_number']) &
                Key('timestamp').between(
                    db_query_params['from_time'], db_query_params['to_time']),
                ProjectionExpression=f"#ts, {', '.join(original_feature_list)}",
                ExpressionAttributeNames={"#ts": "timestamp"},
                ExclusiveStartKey=response['LastEvaluatedKey'])
            if response['Items']:
                result.extend(response['Items'])

        return result

    def get_log_history(self, params, original_feature_list):
        feature_response = []
        db_res_pre = {}
        serial_number = params['serial_number']

        from_time = params['from_time_unit']
        to_time = params['to_time_unit']
        time_unit = params['time_unit']

        parsed_from_time = time_functions.parse_time(from_time)
        parsed_to_time = time_functions.parse_time(to_time)

        if (  # For reducing response time in case of Hourly data
            time_unit == time_functions.HOURLY and
                (parsed_to_time - parsed_from_time) > timedelta(days=7)):
            # Break time period into smaller periods based on threshold value
            time_periods = time_functions.break_time_period(
                from_time, to_time, int(environ['THRESHOLD_TIME_UNIT_EMAIL']))
            for period in time_periods:
                db_query_params = {
                    'serial_number': serial_number,
                    'from_time': time_functions.unparse_time(period['start_time']),
                    'to_time': time_functions.unparse_time(period['end_time'])
                }
                records = self.get_all_logs_in_interval(
                    db_query_params, original_feature_list)
                if records:
                    start_unit = period['start_time']
                    end_unit = start_unit + timedelta(hours=1)
                    child_list = []
                    # Loop for every hour
                    while (start_unit <= period['end_time']):
                        if end_unit >= time_functions.parse_time(
                                records[0]['timestamp']):
                            for item in records:
                                if start_unit <= time_functions.parse_time(
                                        item['timestamp']) < end_unit:
                                    child_list.append(item)

                        if child_list:
                            db_res = child_list[-1]
                            child_list = []
                            feature_response.append(db_res)

                        # Incremental condition of the loop
                        start_unit = end_unit
                        end_unit = start_unit + timedelta(hours=1)

        else:  # Normal Case
            time_periods = time_functions.break_time_period(
                from_time, to_time, time_unit)
            for period in time_periods:
                db_query_params = {
                    'serial_number': serial_number,
                    'from_time': time_functions.unparse_time(period['start_time']),
                    'to_time': time_functions.unparse_time(period['end_time']),
                }
                db_res = self.get_latest_log_in_interval(
                    db_query_params, original_feature_list)
                if db_res:
                    feature_response.append(db_res)

        # Optional functionality
        # Get the latest log before from_time only in the following case:
        # History Logs API is called for reporting_id (offcourse because its email logs) & the following condition is satisfied:
        #       rid_activation_timestamp           from_time          to_time
        # -----------------|---------------------------|------------------|----------> time coordinate
        if 'rid_activation_timestamp' in params:
            rid_activation_timestamp = params['rid_activation_timestamp']

        if 'log_pre_from' in params:
            if (not 'rid_activation_timestamp' in params) or (rid_activation_timestamp < from_time):
                if not feature_response or feature_response[0]['timestamp'] != from_time:
                    expression_attribute_names = {"#ts": "timestamp"}
                    projection_expression = ['#ts']
                    for i, feature in enumerate(original_feature_list):
                        expression_attribute_names[f"#{i}"] = feature
                        projection_expression.append(f"#{i}")
                        db_res_pre = self.table.query(
                        KeyConditionExpression=Key('serial_number').eq(serial_number) &
                            Key('timestamp').lte(from_time),
                        ProjectionExpression=', '.join(projection_expression),
                        ExpressionAttributeNames=expression_attribute_names,
                        ScanIndexForward=False, Limit=1
                        )
                    if db_res_pre['Items']:
                        # Note: Do not return the latest log before from_time if its timestamp is less than the reporting_id activation timestamp, i.e., the following case
                        #    log_pre_from       rid_activation_timestamp           from_time          to_time
                        # ----------|---------------------|---------------------------|------------------|----------> time coordinate
                        if rid_activation_timestamp and db_res_pre['Items'][0]['timestamp'] >= rid_activation_timestamp:
                            db_res_pre['Items'][0]['timestamp'] = from_time
                            feature_response.insert(0, db_res_pre['Items'][0])

        final_response = []
        for item in feature_response:
            result = {}
            timestamp = item.pop('timestamp')
            result['features'] = item
            result['timestamp'] = timestamp
            final_response.append(result)

        return final_response
