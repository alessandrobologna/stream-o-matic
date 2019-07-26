"""
Listen to the DynamoDB stream for partition events and dispatch them as CloudWatch custom events
"""
import json
import logging
from datetime import datetime

import boto3
from boto3.dynamodb.types import TypeDeserializer

from aws_xray_sdk.core import patch_all, xray_recorder

from lib.decorators import DynamoRecord, dynamo_handler

# pylint: disable=invalid-name, line-too-long, unused-argument

patch_all()  # for xray tracing of boto libs
cwe = boto3.client('events')

log = logging.getLogger()

json.JSONEncoder.default = lambda self, obj: (obj.isoformat() if isinstance(obj, type(datetime)) else str(obj))

@dynamo_handler(event_types=[
    DynamoRecord.INSERT,
    DynamoRecord.MODIFY
])
@xray_recorder.capture()
def handler(dynamo_records, context):
    """
    Listen to the DynamoDB stream for partition events and dispatch them as CloudWatch custom events
    """
    deserializer = TypeDeserializer()
    record = dynamo_records[0]
    item = {
        key: deserializer.deserialize(value) for d in [record['dynamodb']['NewImage'], record['dynamodb']['Keys']] for key, value in d.items()
    }

    return cwe.put_events(
        Entries=[
            {
                'Source': 'custom.partition.event',
                'DetailType': 'Partition State Change',
                'Detail': json.dumps(
                    {
                        'Event': f'custom.event.partition.{item["state"]}',
                        'Record': item
                    }
                )
            }
        ]
    )
