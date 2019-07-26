"""
    Listen to CloudTrail S3 events, and publishes them to the DataStream
"""
import json
import logging
import os
from uuid import uuid4
from datetime import datetime

import boto3
import backoff
import botocore
from aws_xray_sdk.core import patch_all, xray_recorder

from lib.decorators import kinesis_handler
from lib.records import KinesisRecord

# pylint: disable=invalid-name, line-too-long, unused-argument

patch_all()  # for xray tracing of boto libs
dynamodb = boto3.resource('dynamodb')
log = logging.getLogger()

json.JSONEncoder.default = lambda self, obj: (obj.isoformat() if isinstance(obj, type(datetime)) else str(obj))

DATA_STREAM = os.environ.get('DATA_STREAM')
INVENTORY_ARN = os.environ.get('INVENTORY_ARN')
kinesis = boto3.client('kinesis')

@xray_recorder.capture()
@kinesis_handler(event_types=[
    KinesisRecord.S3_SOURCE_EVENT
])
def handler(kinesis_records, context):
    """
    Listen to CloudTrail S3 events, and publishes them to the DataStream
    """
    data = kinesis_records[0].parse()
    detail = data.get('detail')
    return publish({
        "eventSource": data['source'],
        "awsRegion": data['region'],
        "eventTime": data['time'],
        "eventName": detail['eventName'],
        "userIdentity": {
            "principalId": detail['userIdentity']['principalId']
        },
        "requestParameters": {
            "sourceIPAddress": detail['sourceIPAddress']
        },
        "s3": {
            "s3SchemaVersion": "1.0",
            "bucket": {
                "name": detail['requestParameters']['bucketName'],
                "arn": detail['resources'][1]['ARN']
            },
            "object": {
                "key": detail['requestParameters']['key'],
                "size": detail['additionalEventData']['bytesTransferredIn']
            }
        }
    })

@xray_recorder.capture()
@backoff.on_exception(backoff.expo, botocore.exceptions.ClientError, max_time=10)
def publish(event: dict):
    """
    Publish event to Kinesis
    """
    return kinesis.put_record(
        StreamName=DATA_STREAM,
        Data=json.dumps(event).encode('utf-8'),
        PartitionKey=randomize_arn(INVENTORY_ARN)
    )

def randomize_arn(arn: str) -> str:
    return f'{arn}:{str(uuid4())}'
