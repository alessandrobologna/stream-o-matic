""""
Listen to CloudTrail Glue events in the tmp database, and create a symlink to the compacted partition
"""
import json
import logging
import os
from datetime import datetime

import boto3
import botocore
import backoff
from aws_xray_sdk.core import patch_all, xray_recorder

from lib.decorators import kinesis_handler, KinesisRecord
from lib.glue import glue
from lib.s3 import upload_file, check_prefix

# pylint: disable=invalid-name, line-too-long, unused-argument, logging-fstring-interpolation

patch_all()  # for xray tracing of boto libs

dynamodb = boto3.resource('dynamodb')
logs = boto3.client('logs')
log = logging.getLogger()
TMP_DATABASE = os.environ['TMP_DATABASE']
COMPACTED_PARTITIONS = os.environ['COMPACTED_PARTITIONS']

json.JSONEncoder.default = lambda self, obj: (obj.isoformat() if isinstance(obj, type(datetime)) else str(obj))

@xray_recorder.capture()
@kinesis_handler(event_types=[
    KinesisRecord.GLUE_SOURCE_EVENT
])
def handler(kinesis_records, context):
    """
    Listen to CloudTrail Glue events in the tmp database, and create a symlink to the compacted partition
    """
    detail = kinesis_records[0].parse().get('detail')
    if detail.get('typeOfChange') == 'CreateTable':
        tables = detail.get('changedTables')
        database_name = detail.get('databaseName')
        if database_name == TMP_DATABASE:
            return [update_simlink(database_name, table_name) for table_name in tables]

@xray_recorder.capture()
@backoff.on_exception(backoff.expo, botocore.exceptions.ClientError, max_time=10)
def update_simlink(database_name: str, table_name: dict):
    try:
        # attempt to remove the tmp table from glue, ignore if not found
        glue.delete_table(
            DatabaseName=database_name,
            Name=table_name
        )
    except glue.exceptions.EntityNotFoundException:
        pass
    item = dynamodb.Table(COMPACTED_PARTITIONS).get_item(
        Key={
            'tmptable': f'{database_name}.{table_name}'
        }
    ).get('Item')
    if item:
        # make sure target exists
        if check_prefix(item.get('target')):
            log.info(f"updating {item.get('location')} with {item.get('target')}")
            return upload_file(
                item.get('location'),
                item.get('target')        
            )
    return None
