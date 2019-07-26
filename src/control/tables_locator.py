""""
Listen to CloudTrail Glue events, and map in dynamodb the location of the Glue Table
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
from lib.glue import get_glue_table

# pylint: disable=invalid-name, line-too-long, unused-argument

patch_all()  # for xray tracing of boto libs

dynamodb = boto3.resource('dynamodb')
logs = boto3.client('logs')
log = logging.getLogger()
TMP_DATABASE = os.environ['TMP_DATABASE']
GLUE_TABLES_LOCATOR = os.environ['GLUE_TABLES_LOCATOR']
DATA_BUCKET = os.environ['DATA_BUCKET']

json.JSONEncoder.default = lambda self, obj: (obj.isoformat() if isinstance(obj, type(datetime)) else str(obj))

@xray_recorder.capture()
@kinesis_handler(event_types=[
    KinesisRecord.GLUE_SOURCE_EVENT
])
def handler(kinesis_records, context):
    """
    Listen to CloudTrail Glue events, and map in dynamodb the location of the Glue Table
    """
    detail = kinesis_records[0].parse().get('detail')
    if detail.get('typeOfChange') == 'CreateTable':
        tables = detail.get('changedTables')
        database_name = detail.get('databaseName')
        if database_name != TMP_DATABASE:
            return [insert_location(database_name, table_name) for table_name in tables]
    return None

@xray_recorder.capture()
@backoff.on_exception(backoff.expo, botocore.exceptions.ClientError, max_time=10)
def insert_location(database_name: str, table_name: dict):
    """
    Insert location of Glue Table
    """
    table = get_glue_table(database_name, table_name)
    if not table:
        return
    log.info(table.dump())
    bucket = table.get_table_bucket()
    if bucket != DATA_BUCKET:
        return
    table_prefix = table.get_table_prefix()
    batch_items = [{
        'location': f's3://{bucket}/{table_prefix}',
        'database_name': database_name,
        'table_name': table_name,
        'partition_keys': table.get_partition_keys(),
        'is_simlinked': table.is_symlinked()
    }]
    firehose_target = table.get_firehose_target()
    if firehose_target != table_prefix:
        batch_items.append({
            'location': f's3://{bucket}/{firehose_target}',
            'symlink': f's3://{bucket}/{table_prefix}'
        })
    log.info(batch_items)
    if batch_items:
        with dynamodb.Table(GLUE_TABLES_LOCATOR).batch_writer() as batch:
            for batch_item in batch_items:
                batch.put_item(Item=batch_item)
    return batch_items
