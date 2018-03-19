from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr
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
        store_data['log_service_id'] = data['log_service_id']
        if data['communication_type'] == 'cloud':
            store_data['device_id'] = data['device_id']
        else:
            store_data['serial_number'] = data['serial_number']
        self.table.put_item(
            Item=store_data)


    def read(self, reporting_id, log_service_id):
        result = self.table.query(
            KeyConditionExpression=Key('reporting_id').eq(reporting_id),
            FilterExpression=Attr('log_service_id').eq(log_service_id)
            )
        if result['Items']:
            return result['Items']


    # Get reporting records in a particular time interval
    # Also return the adjusted time intervals for accurate searching
    def get_reporting_records(self, reporting_id, log_service_id, from_time, to_time):
        records = self.read(reporting_id, log_service_id)

        if not records:  # No records = Reporting Id not found
            return None
        else:  # One or more records
            for i, record in enumerate(records):
                if record['timestamp'] > to_time:
                    break

                if i < len(records) - 1:  # If next item exists in 'records' list
                    # Condition for calculating 'from_time_unit'
                    if from_time <= record['timestamp'] < records[i + 1]['timestamp']:
                        record['from_time_unit'] = record['timestamp']
                    elif record['timestamp'] < from_time < records[i + 1]['timestamp']:
                        record['from_time_unit'] = from_time
                    elif record['timestamp'] < records[i + 1]['timestamp'] <= from_time:
                        continue

                    # Condition for calculating 'to_time_unit'
                    if record['from_time_unit'] < records[i + 1]['timestamp'] < to_time:
                        record['to_time_unit'] = time_functions.subtract_seconds(
                            records[i + 1]['timestamp'], 1)
                        # Condition for next loop
                        from_time = records[i + 1]['timestamp']
                    elif to_time <= records[i + 1]['timestamp']:
                        record['to_time_unit'] = to_time
                        # Condition for next loop
                        from_time = records[i + 1]['timestamp']

                elif i == len(records) - 1:  # If this is the last item in 'records' list
                    # Condition for calculating 'from_time_unit'
                    if record['timestamp'] <= from_time:
                        record['from_time_unit'] = from_time
                    elif from_time < record['timestamp'] <= to_time:
                        record['from_time_unit'] = record['timestamp']

                    record['to_time_unit'] = to_time

                record['rid_activation_timestamp'] = record.pop('timestamp') # Rename the timestamp to rid_activation_timestamp for convenience (rid = reporting_id)
                
            return records
