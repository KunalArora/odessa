from models.base import Base


class PushNotificationSubscription(Base):
    def __init__(self):
        super().__init__()
        self.table = self.dynamodb.Table('push_notification_subscriptions')

    def read(self, log_service_id, object_id):
        result = self.table.get_item(Key={
                'log_service_id': log_service_id,
                'object_id': object_id
            })
        if result['Item']:
            self.log_service_id = result['Item']['log_service_id']
            self.object_id = result['Item']['object_id']
            self.notify_url = result['Item']['notify_url']

        return self

    def is_subscribed(self):
        return hasattr(self, 'notify_url')
