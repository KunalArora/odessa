import json
import logging
from botocore.exceptions import ClientError
from botocore.exceptions import ConnectionError
from constants.odessa_response_codes import *
from functions import helper
from models.device_log import DeviceLog
from models.device_network_status import DeviceNetworkStatus
from models.device_subscription import DeviceSubscription
from models.service_oid import ServiceOid
from redis import RedisError

logger = logging.getLogger('device_logs')
logger.setLevel(logging.INFO)


def get_latest_logs(event, context):
    #    Retrieve latest logs from the database
    device_log = DeviceLog()
    device_subscription = DeviceSubscription()
    device_network_status = DeviceNetworkStatus()
    service_oid = ServiceOid()
    devices = []
    logger.info("Request parameter {}".format(event))
    try:
        if isinstance(event['body'], (str, bytes)):
            request = json.loads(event['body'])
        device_ids = request['device_id'] if 'device_id' in request else ''
        service_id = request['log_service_id'] if 'log_service_id' in request and request['log_service_id'] else '0'
        if not device_ids:
            logger.warning(
                "Device Id request parameter is either null or not present "
                "for event {}".format(event))
            raise helper.DeviceIdParameterError(event)

        #   Retrieve object Id list from ObjectIdList table
        #   depending on the log_service_id
        oid = service_oid.read(service_id)
        if not oid:
            logger.warning(
                "Request log service Id doesn't exist in the database "
                "for event {}".format(event))
            raise helper.ServiceIdError(event)

        #   Make sure that multiple instance of same device_id
        #   is not present in the request
        device_ids = list(set(device_ids))

        for device_id in device_ids:
            #   Retrieve object_ids (whose status is Subscribed) for a particular
            #   device_id and service_id from DeviceSubscription table.
            status_res = device_subscription.get_device_status(
                device_id, service_id)

            #   Retrieve Online_Offline feature value from
            #   DeviceNetworkStatus table
            network_res = device_network_status.get_latest_status(
                device_id)

            #   If no object_id is Subscribed yet or the device_id
            #   is not found in the DeviceSubscription table and device_id
            #   is not found in the DeviceNetworkStatus table then,
            #   'Device Not Found' error is returned as response.
            if not status_res['Items'] and not network_res:
                devices.append(
                    helper.create_devices_layer([], device_id, code=DEVICE_NOT_FOUND
                                                ))
            else:
                #   Retrieve latest logs from either ElastiCache or Dynamodb
                log_res = device_log.get_latest_logs(status_res)

                #   Verify if the above extracted data from DeviceLog table is
                #   latest or not by checking with DeviceSubscription table data
                verified_res = device_subscription.verify_updated_date(
                    log_res, service_id)
                network_res = network_res[0] if type(
                    network_res) is list else network_res

                parsed_res = device_log.parse_data(verified_res)
                parsed_res.append(
                    helper.create_feature_format(SUCCESS, 'Online_Offline',
                                                 network_res['status'],
                                                 network_res['timestamp'])
                )
                features = helper.create_features_layer(parsed_res)
                devices.append(
                    helper.create_devices_layer(features, device_id))
        response = {
            "statusCode": 200,
            "body": json.dumps(helper.create_odessa_layer(devices))
        }
        return response
    except (helper.DeviceIdParameterError, helper.ServiceIdError) as e:
        logger.error(e)
        response = {
            "statusCode": 200,
            "body": json.dumps(helper.create_odessa_layer([], code=BAD_REQUEST))
        }
        return response
    except ValueError as e:
        logger.error(e)
        logger.warning(
            "JSON format Value error in the request for event {}".format(event))
        response = {
            "statusCode": 200,
            "body": json.dumps(helper.create_odessa_layer([], code=BAD_REQUEST))
        }
        return response
    except TypeError as e:
        logger.error(e)
        logger.warning(
            "Format Type error in the request for event {}".format(event))
        response = {
            "statusCode": 200,
            "body": json.dumps(helper.create_odessa_layer([], code=BAD_REQUEST))
        }
        return response
    except ConnectionError as e:
        logger.error(e)
        logger.warning(
            "Dynamodb Connection Error on GetDeviceLog "
            "for event {}".format(event))
        response = {
            "statusCode": 200,
            "body": json.dumps(
                helper.create_odessa_layer([], code=BOC_DB_CONNECTION_ERROR))
        }
        return response
    except ClientError as e:
        logger.error(e)
        logger.warning(
            "Dynamodb Client Error on GetDeviceLog for event {}".format(event))
        response = {
            "statusCode": 200,
            "body": json.dumps(
                helper.create_odessa_layer([], code=BOC_DB_CONNECTION_ERROR))
        }
        return response
    except RedisError as e:
        logger.error(e)
        logger.warning(
            "Redis Error on GetDeviceLog for event {}".format(event))
        response = {
            "statusCode": 200,
            "body": json.dumps(
                helper.create_odessa_layer([], code=BOC_DB_CONNECTION_ERROR))
        }
        return response
