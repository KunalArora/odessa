from models.base import Base


class ServiceOid(Base):
    def __init__(self):
        super().__init__()

    @classmethod
    def ids(cls):
        t = ServiceOid().dynamodb.Table('service_oids')
        result = t.scan(AttributesToGet=['id'])
        if 'Items' in result:
            return [item['id'] for item in result['Items']]
        else:
            return False

    def read(self, log_service_id):
        table = self.dynamodb.Table('service_oids')
        result = table.get_item(
            Key={
                'id': log_service_id
            }
        )
        if 'Item' in result:
            return result['Item']
        else:
            return False
