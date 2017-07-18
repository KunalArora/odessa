from models.base import Base


class ServiceOid(Base):
    def __init__(self):
        super().__init__()

    def read(self, log_service_id):
        table = self.dynamodb.Table('service_oids')
        result =  table.get_item(
            Key={
                'id': log_service_id
            }
        )
        if 'Item' in result:
            return result['Item']
        else:
            return False
