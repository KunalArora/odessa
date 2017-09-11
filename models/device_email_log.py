from models.base import Base

class DeviceEmailLog(Base):
	def __init__(self):
		super().__init__()


	def create(self, mail_data):
		table = self.dynamodb.Table('device_email_logs')
		table.put_item(
			Item=mail_data)
