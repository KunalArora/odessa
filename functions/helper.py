import json


def create_response(device_id, boc_response):
    body = {
        'code': boc_response['code'],
        'device_id': device_id
    }

    if 'get' in boc_response:
        body['data'] = boc_response['get']
    elif 'set' in boc_response:
        body['data'] = boc_response['set']

    body['message'] = boc_response['message']
    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }
    return response


def error_response(device_id, code, message):
    body = {
        'code': code,
        'device_id': device_id,
        'message': message
    }
    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }
    return response


def code_to_message(code, reason=None):
    error = {
        503: reason,
        560: 'Error',
        563: 'Failed to call BOC API'
    }
    return error[code]
