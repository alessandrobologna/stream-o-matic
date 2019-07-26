"""
Listen to the DynamoDB stream for partition events and dispatch them as CloudWatch custom events
"""
import os
import json
import logging
from datetime import datetime

import backoff
import botocore
from aws_xray_sdk.core import patch_all, xray_recorder
from lib.decorators import KinesisRecord, kinesis_handler
from lib.glue import get_glue_table, glue
from lib.s3 import upload_file

# pylint: disable=invalid-name, line-too-long, unused-argument

patch_all()  # for xray tracing of boto libs

log = logging.getLogger()
DATA_BUCKET = os.environ['DATA_BUCKET']

json.JSONEncoder.default = lambda self, obj: (obj.isoformat() if isinstance(obj, type(datetime)) else str(obj))

@kinesis_handler(event_types=[
    KinesisRecord.OPENED_PARTITION_EVENT
])
@xray_recorder.capture()
def handler(kinesis_records, context):
    """
    Listen to Kinesis events and add a new partition to a table
    """
    detail = kinesis_records[0].parse().get('detail')
    record = detail.get('Record')
    if record.get('leaf'):
        return add_glue_partition(record)

@xray_recorder.capture()
@backoff.on_exception(backoff.expo, botocore.exceptions.ClientError, max_time=10)
def add_glue_partition(record: dict):
    """
    Add the partition to Glue
    """
    location = record['location']
    symlink = record.get('symlink')
    if symlink:
        # event is for a firehose location, just create a symlink
        partition = record['partition']
        return upload_file(
            f'{symlink}/{partition}/symlink.txt',
            f'{location}/'
        )
    # event is for a glue location add the current partition
    db_table = record['db_table']
    database_name, table_name = db_table.split('.')

    values = record['values']
    table = get_glue_table(database_name, table_name)
    if not table:
        return f"{db_table} not found"
    storage = table.get_storage_descriptor()
    try:
        return glue.create_partition(
            DatabaseName=database_name,
            TableName=table_name,
            PartitionInput={
                'Values': values,
                'StorageDescriptor': {
                    'Columns': storage['Columns'],
                    'Location': location,
                    'InputFormat': storage['InputFormat'],
                    'OutputFormat': storage['OutputFormat'],
                    'Compressed': storage['Compressed'],
                    'SerdeInfo': storage['SerdeInfo'],
                    'Parameters': storage.get('Parameters', {}),
                    'StoredAsSubDirectories': False
                },
                'Parameters': table.get('Parameters', {})
            }
        )
    except glue.exceptions.AlreadyExistsException as ex:
        # there's already a partition
        log.info(ex)
