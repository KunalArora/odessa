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
        device_ids = request['device_id'] if 'device_id' in request else None
        service_id = str(request['log_service_id']) if 'log_service_id' in request and request['log_service_id'] else '0'
        if isinstance(device_ids, str):
            device_ids = [device_ids]
        if not device_ids:
            logger.warning(
                "handler:device_logs Device Id request parameter is either "
                "null or not present for event {}".format(event))
            raise helper.DeviceIdParameterError(event)

        #   Retrieve object Id list from ObjectIdList table
        #   depending on the log_service_id
        oid = service_oid.read(service_id)
        if not oid:
            logger.warning(
                "handler:device_logs Request log service Id doesn't exist "
                "in the database for event {}".format(event))
            raise helper.ServiceIdError(event)

        #   Make sure that multiple instance of same device_id
        #   is not present in the request
        device_ids = list(set(device_ids))

        for device_id in device_ids:
            device_id = device_id.lower()
            #   Retrieve object_ids (whose status is Subscribed) for a particular
            #   device_id and service_id from DeviceSubscription table.
            status_res = device_subscription.get_device_status(
                device_id, service_id)
            subscribed_offline = False
            if (status_res and status_res[0]['status'] == 1201):
                subscribed_offline = True
                subscribed_offline_timestamp = status_res[0]['updated_at']

            #   Retrieve Online_Offline feature value from
            #   DeviceNetworkStatus table
            network_res = device_network_status.get_latest_status(
                device_id)

            #   Verify if the above extracted data from DeviceLog table is
            #   latest or not by checking with DeviceSubscription table data
            if network_res:
                network_res = network_res[0] if type(
                    network_res) is list else network_res

            #   If no object_id is Subscribed yet or the device_id
            #   is not found in the DeviceSubscription table and device_id
            #   is not found in the DeviceNetworkStatus table then,
            #   'Device Not Found' error is returned as response.
            if not status_res and not network_res:
                devices.append(
                    helper.create_devices_layer([], device_id, code=DEVICE_NOT_FOUND))
            elif subscribed_offline:
                feature = helper.create_feature_format(SUCCESS, 'Online_Offline',
                                                       'offline', subscribed_offline_timestamp)
                features = helper.create_features_layer([feature])
                devices.append(
                    helper.create_devices_layer(features, device_id))
            else:
                #   Retrieve latest logs from either ElastiCache or Dynamodb
                log_res = device_log.get_latest_logs(status_res)
                if not log_res['Items'] and not network_res:
                    logger.warning(
                        "handler:device_logs No records found for device {}".format(device_id)
                    )
                parsed_res = device_log.parse_log_data(log_res)
                if network_res:
                    parsed_res.append(
                        helper.create_feature_format(SUCCESS, 'Online_Offline',
                                                    network_res['status'],
                                                    network_res['timestamp'])
                        )
                elif not network_res and status_res and status_res[0]['status'] == 1200:
                    parsed_res.append(
                        helper.create_feature_format(SUCCESS, 'Online_Offline',
                                                    'online', status_res[0]['updated_at']))
                features = helper.create_features_layer(parsed_res)
                devices.append(
                    helper.create_devices_layer(features, device_id))
        error_code = helper.odessa_error_code(devices)
        return helper.latest_logs_response(error_code, devices)
    except (helper.DeviceIdParameterError, helper.ServiceIdError) as e:
        return helper.latest_logs_response(BAD_REQUEST)
    except ValueError as e:
        logger.warning(
            "handler:device_logs JSON format Value error in the request for event {}".format(event))
        return helper.latest_logs_response(BAD_REQUEST)
    except TypeError as e:
        logger.warning(
            "handler:device_logs Format Type error in the request for event {}".format(event))
        return helper.latest_logs_response(BAD_REQUEST)
    except ConnectionError as e:
        logger.error(e)
        logger.warning(
            "handler:device_logs Dynamodb Connection Error "
            "on GetDeviceLog for event {}".format(event))
        return helper.latest_logs_response(BOC_DB_CONNECTION_ERROR)
    except ClientError as e:
        logger.error(e)
        logger.warning(
            "handler:device_logs Dynamodb Client Error on "
            "GetDeviceLog for event {}".format(event))
        return helper.latest_logs_response(BOC_DB_CONNECTION_ERROR)
    except RedisError as e:
        logger.error(e)
        logger.warning(
            "handler:device_logs Redis Error on GetDeviceLog for event {}".format(event))
        return helper.latest_logs_response(BOC_DB_CONNECTION_ERROR)
