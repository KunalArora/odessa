import uuid
import json
import datetime


def get_one_time_token(event, context):
    token = {
        'session_token': uuid.uuid4().hex,
        'secret_access_key': uuid.uuid4().hex,
        'access_key_id': uuid.uuid4().hex,
        'expiration': datetime.datetime.now().isoformat()
    }
    return {
        'statusCode': 200,
        'body': json.dumps(token)
        }
