import datetime
from models.base import Base

class ReportingRegistration(Base):
    def __init__(self):
        super().__init__()

    def create(self, data):
        table = self.dynamodb.Table('reporting_registrations')
        store_data = {}
        store_data['reporting_id'] = data['reporting_id']
        store_data['timestamp'] = current_utc_time()
        store_data['communication_type'] = data['communication_type']
        if data['communication_type'] == 'cloud':
            store_data['device_id'] = data['device_id']
        else:
            store_data['serial_number'] = data['serial_number']
        table.put_item(
                Item=store_data)

def current_utc_time():
    return (datetime.datetime.
            utcnow().strftime('%Y-%m-%dT%H:%M:%S'))
