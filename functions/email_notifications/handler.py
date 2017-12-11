import boto3
import csv
import email
import io
import logging
import json
from datetime import datetime
from config import PrintCountFieldMap
from models.device_email_log import DeviceEmailLog
import xml.etree.ElementTree as ET
from botocore.exceptions import ClientError
from botocore.exceptions import ConnectionError
from os import environ
from botocore.client import Config
import yaml
import os
import traceback

logger = logging.getLogger('email_notifications')
logger.setLevel(logging.INFO)

def save_mail_report(event, context):
    device_email_log = DeviceEmailLog()
    mail_log_data = {}
    xml_parsed_field_map_dict = {}
    csv_parsed_field_map_dict = {}
    logger.info(f"Request parameter: {event}")

    try:
        request = json.loads(event["Records"][0]['Sns']['Message'])

        email_from = request['mail']['commonHeaders']['from'][0]
        country_code = email_from[-2:]
        subject_present = False
        env = os.environ['STAGE']

        with open(f"config/country_config/{env}.yml") as data:
            yaml_data = yaml.load(data)

        if (
            country_code in yaml_data['CountryConfig']
                and yaml_data['CountryConfig'][country_code]['email_enabled']):
            email_subject = request['mail']['commonHeaders']['subject']
            country_email_subject = yaml_data['CountryConfig'][country_code]['subject_list'].split('|')

            for subject in country_email_subject:
                if subject in email_subject:
                    subject_present = True
                    break
            if not subject_present:
                logger.warning(
                    f'handler:email_notifications this email does not belong '
                    f'to print counts as the email subject {email_subject} '
                    f'does not match country {country_code} email '
                    f'subject {country_email_subject}.'
                )
                return

            bucket_name = request['receipt']['action']['bucketName']
            bucket_object_key = request['receipt']['action']['objectKey']
            bucket_region_name = request['receipt']['action']['topicArn'].split(':')[3]
            mail_timestamp = request['mail']['timestamp']


            raw_email = retrieve_mail(bucket_name, bucket_object_key, bucket_region_name)
            msg = email.message_from_string(raw_email)
            attachments = find_attachments(msg)
            for cdisp, part in attachments:
                extension = cdisp['filename'].split('.')[1]
                mail_charset = part.get_content_charset()
                encoding= mail_charset if mail_charset else 'utf-8'
                attach_data = str(part.get_payload(decode=True), encoding)

            if extension == 'xml':
                root = ET.fromstring(attach_data)
                xml_data = parse_xml(root)

                for item in range(0, len(xml_data), 2):
                    xml_parsed_field_map_dict[xml_data[item]
                                              ] = xml_data[item + 1]
                for k, v in xml_parsed_field_map_dict.items():
                    if k in PrintCountFieldMap.PrintCountFieldMapXML:
                        mail_log_data[PrintCountFieldMap.PrintCountFieldMapXML[k]
                                      ] = v if v else ' '
            elif extension == 'csv':
                raw_data = io.StringIO(attach_data)
                csv_data = csv.DictReader(raw_data, delimiter=',')

                for item in csv_data:
                    csv_parsed_field_map_dict = dict(item)
                for k, v in csv_parsed_field_map_dict.items():
                    if k in PrintCountFieldMap.PrintCountFieldMapCSV:
                        mail_log_data[PrintCountFieldMap.PrintCountFieldMapCSV[k]
                                      ] = v if v else ' '
            else:
                logger.warning(
                    f'handler:email_notifications {extension} '
                    f'file extension not yet supported')
                return

            mail_timestamp = datetime.strptime(mail_timestamp, '%Y-%m-%dT%H:%M:%S.%fZ')
            mail_timestamp = mail_timestamp.replace(microsecond=0)
            mail_log_data['timestamp'] = mail_timestamp.isoformat()
            device_email_log.create(mail_log_data)
        else:
            logger.warning(
                f'handler:email_notifications email notifications functionality '
                f'is not enabled for the specified country {country_code}')
    except ValueError as e:
        logger.warning(traceback.format_exc())
    except TypeError as e:
        logger.warning(traceback.format_exc())
    except ConnectionError as e:
        logger.error(e)
        logger.warning(
            f'handler:email_notifications dynamodb connection error '
            f'on SaveMailReport for bucket {bucket_name}, object_key '
            f'{bucket_object_key} and xml_parsed_data {mail_log_data}')
    except ClientError as e:
        logger.error(e)
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchBucket':
            logger.warning(
                f'handler:email_notifications Amazon S3 invalid bucket name '
                f'error on SaveMailReport for bucket {bucket_name} and '
                f'object_key {bucket_object_key}')
        elif error_code == 'NoSuchKey':
            logger.warning(
                f'handler:email_notifications Amazon S3 invalid object key name '
                f'error on SaveMailReport for bucket {bucket_name} and '
                f'object_key {bucket_object_key}')
        else:
            logger.warning(
                f'handler:email_notifications dynamodb client error '
                f'on SaveMailReport for bucket {bucket_name}, object_key '
                f'{bucket_object_key} and xml_parsed_data {mail_log_data}')

def parse_xml(root):
    res = []
    for child in root:
        res = res + read_rec(child, child.tag.split('}')[1])
    return res


def read_rec(node, name, parent=None):
    if len(node.getchildren()) > 0:
        res = []
        for child in node:
            child_name_prefix = child.tag.split('}')[1]
            if child.attrib:
                attr = child.attrib
                key = next(iter(attr.keys()))
                valu = next(iter(attr.values()))
                child_name_prefix = child_name_prefix + '_' + key + '_' + valu
            res = res + read_rec(child, child_name_prefix, name)
        return res
    else:
        return [parent + '_' + name, node.text]


def retrieve_mail(bucket_name, bucket_object_key, bucket_region_name):
    if environ['S3_ENDPOINT_URL']:
        s3client = boto3.resource('s3', endpoint_url=environ['S3_ENDPOINT_URL'],
                                aws_access_key_id=environ['S3_ACCESS_KEY'],
                                aws_secret_access_key=environ['S3_SECRET_KEY'],
                                config=Config(signature_version='s3v4'),
                                )
    else:
        s3client = boto3.resource('s3', region_name=bucket_region_name)
    obj = s3client.Object(bucket_name, bucket_object_key)
    res = obj.get()
    return (res['Body'].read().decode('utf-8'))

def find_attachments(message):
    found = []
    for part in message.walk():
        if 'content-disposition' not in part:
            continue
        cdisp = part['content-disposition'].split(';')
        cdisp = [x.strip() for x in cdisp]
        if cdisp[0].lower() != 'attachment':
            continue
        parsed = {}
        for kv in cdisp[1:]:
            key, val = kv.split('=')
            if val.startswith('"'):
                val = val.strip('"')
            elif val.startswith("'"):
                val = val.strip("'")
            parsed[key] = val
        found.append((parsed, part))
    return found
