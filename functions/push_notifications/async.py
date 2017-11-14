import json
import logging
import sys
from urllib import parse
from urllib import request
from urllib.error import HTTPError
from urllib.error import URLError

logger = logging.getLogger('send_push_notification:async')
logger.setLevel(logging.INFO)


def send_push_notification(event, context):
    logger.info(f'async:send_push_notification, request: {json.dumps(event)}')
    if ('reporting_id' not in event or not isinstance(event['reporting_id'], str)):
        logger.warning('BadRequest on async:send_push_notification (reporting_id "{event["reporting_id"]}" invalid.)')
        return
    if ('object_id' not in event or not isinstance(event['object_id'], str)):
        logger.warning('BadRequest on async:send_push_notification (object_id "{event["object_id"]}" invalid.)')
        return
    if ('data' not in event or not isinstance(event['data'], list)):
        logger.warning('BadRequest on async:send_push_notification (data "{event["data"]}" invalid.)')
        return
    if ('timestamp' not in event or not isinstance(event['timestamp'], str)):
        logger.warning('BadRequest on async:send_push_notification (timestamp "{event["timestamp"]}" invalid.)')
        return
    if ('notify_url' not in event or not isinstance(event['notify_url'], str)):
        logger.warning('BadRequest on async:send_push_notification (notify_url "{event["notify_url"]}" invalid.)')
        return
    notification_payload = {
        'reporting_id': event['reporting_id'],
        'object_id': event['object_id'],
        'timestamp': event['timestamp'],
        'data': event['data']
    }
    try:
        data = parse.urlencode(notification_payload).encode('ascii')
        request.urlopen(event['notify_url'], data)
        logger.info(f'async:send_push_notification successfully sent notification to {event["notify_url"]}.')
    except HTTPError as e:
        if e.code not in range(400, 499):
            raise
        else:
            logger.error(f'Error {e.code} on async:send_push_notification to {event["notify_url"]}.')
            logger.error(e.read())
    except URLError as e:
        logger.error(f'Error on async:send_push_notification to {event["notify_url"]}.')
        logger.error(e.read())
    except:  # pragma: no cover
        logger.error(sys.exc_info())
