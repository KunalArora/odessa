import boto3
import csv
import email
import io
import logging
from config import CountryConfig
from config import PrintCountFieldMap
from models.device_email_log import DeviceEmailLog
import xml.etree.ElementTree as ET
from botocore.exceptions import ClientError
from botocore.exceptions import ConnectionError
from os import environ
from botocore.client import Config

logger = logging.getLogger('email_notifications')
logger.setLevel(logging.INFO)


def save_mail_report(event, context):
    device_email_log = DeviceEmailLog()
    mail_log_data = {}
    xml_parsed_field_map_dict = {}
    csv_parsed_field_map_dict = {}

    try:
        email_from = event['mail']['commonHeaders']['from'][0]
        country_code = (email_from.split('@')[1].split('.')[1])

        if CountryConfig.EmailEnableConfigPerCountry[country_code]:
            email_subject = event['mail']['commonHeaders']['subject']
            if email_subject not in CountryConfig.SubjectListConfigPerCountry[country_code]:
                return

            bucket_name = event['receipt']['action']['bucketName']
            bucket_object_key = event['receipt']['action']['objectKey']
            mail_timestamp = event['mail']['timestamp']


            raw_email = retrieve_mail(bucket_name, bucket_object_key)
            msg = email.message_from_string(raw_email)
            attachments = find_attachments(msg)
            for cdisp, part in attachments:
                extension = cdisp['filename'].split('.')[1]
                attach_data = str(part.get_payload(decode=True),
                    part.get_content_charset())

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
                    "{} file extension not yet supported.".format(extension))
                return

            mail_log_data['timestamp'] = mail_timestamp
            device_email_log.create(mail_log_data)
        else:
            logger.warning("Email Notifications functionality is not enabled "
                           "for the specified country {}".format(country_code))
    except ValueError as e:
        logger.warning(
            "handler:email_notifications JSON format Value error in the "
            "request for event {}".format(event))
    except TypeError as e:
        logger.warning(
            "handler:email_notifications Format Type error in the "
            "request for event {}".format(event))
    except ConnectionError as e:
        logger.error(e)
        logger.warning(
            "handler:email_notifications Dynamodb Connection Error "
            "on SaveMailReport for bucket {}, object_key {} "
            "and xml_parsed_data {}".format(bucket_name, bucket_object_key, mail_log_data))
    except ClientError as e:
        logger.error(e)
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchBucket':
            logger.warning(
                "handler:email_notifications Amazon S3 Invalid Bucket Name Error on "
                "SaveMailReport for bucket {} and object_key {}".format(bucket_name, bucket_object_key))
        else:
            logger.warning(
                "handler:email_notifications Dynamodb Client Error "
                "on SaveMailReport for bucket {}, object_key {} "
                "and xml_parsed_data {}".format(bucket_name, bucket_object_key, mail_log_data))

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


def retrieve_mail(bucket_name, bucket_object_key):
    if environ['S3_ENDPOINT_URL']:
        s3client = boto3.resource('s3', endpoint_url=environ['S3_ENDPOINT_URL'],
                                aws_access_key_id=environ['S3_ACCESS_KEY'],
                                aws_secret_access_key=environ['S3_SECRET_KEY'],
                                config=Config(signature_version='s3v4'),
                                )
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
