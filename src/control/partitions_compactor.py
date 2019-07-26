"""
Listen to the DynamoDB stream for partition events update Glue partitions metastore
"""
import os
import json
import logging
from datetime import datetime, timedelta
from uuid import uuid4

import backoff
import boto3

import botocore
from aws_xray_sdk.core import patch_all, xray_recorder

from lib.decorators import KinesisRecord, kinesis_handler
from lib.glue import get_glue_table
from lib.athena import execute_query

# pylint: disable=invalid-name, line-too-long, unused-argument

patch_all()  # for xray tracing of boto libs
dynamodb = boto3.resource('dynamodb')
log = logging.getLogger()
DATA_BUCKET = os.environ['DATA_BUCKET']
TMP_DATABASE = os.environ['TMP_DATABASE']
COMPACTED_PARTITIONS = os.environ['COMPACTED_PARTITIONS']
EPOCH = datetime.utcfromtimestamp(0)

json.JSONEncoder.default = lambda self, obj: (obj.isoformat() if isinstance(obj, type(datetime)) else str(obj))

@kinesis_handler(event_types=[
    KinesisRecord.CLOSED_PARTITION_EVENT
])
@xray_recorder.capture()
def handler(kinesis_records, context):
    """
    Listen to Kinesis events and add a new partition to a table
    """
    detail = kinesis_records[0].parse().get('detail')
    record = detail.get('Record')
    if record.get('leaf') and record.get('symlink'):
        return compact_glue_partition(record)


@xray_recorder.capture()
@backoff.on_exception(backoff.expo, botocore.exceptions.ClientError, max_time=10)
def compact_glue_partition(record: dict):
    """
    Listen to Kinesis events and compact a partition for a simlinked table
    """
    db_table = record['db_table']
    database_name, table_name = db_table.split('.')
    table = get_glue_table(database_name, table_name)
    if not table:
        return f"{db_table} not found"
    symlink = record.get('symlink')
    location = record['location']
    partition = record['partition']
    compacted = record.get('compacted',0)
    target = location.replace(partition, '').rsplit('/', 2)[0] + f'/compacted={compacted}/' + partition
    storage = table.get_storage_descriptor()
    columns = ', '.join([
        f'\"{column["Name"]}\"' for column in storage.get('Columns', [])
    ])
    values = record['values']
    clause = ' AND '.join([
        f"\"{key['Name']}\"='{values[idx]}'" for idx, key in enumerate(table.get_partition_keys())
    ])
    tmp_table = f'{TMP_DATABASE}.tmp{str(uuid4()).replace("-","")}'
    with_parts = [
        f"external_location = '{target}'",
        "format = 'Parquet'",
        table.get_compaction_bucketing(),
    ]
    sorting = table.get_compaction_sorting()
    if sorting:
        with_parts.append(sorting)
    query = f"""
        CREATE TABLE {tmp_table}
        WITH ({', '.join(with_parts)}) 
        AS SELECT {columns}
        FROM {db_table} 
        WHERE {clause}
        {table.get_compaction_sorting()}
    """
    query = ' '.join(query.replace('\n', ' ').split())
    response = log_transaction(tmp_table, target, f'{symlink}/{partition}/symlink.txt', query)
    if response:
        return execute_query(query, DATA_BUCKET)


@xray_recorder.capture()
@backoff.on_exception(backoff.expo, botocore.exceptions.ClientError, max_time=10)
def log_transaction(tmptable, target, location, query):
    return dynamodb.Table(COMPACTED_PARTITIONS).put_item(
        Item={
            'tmptable': tmptable,
            'target': target,
            'location': location,
            'query': query,
            'expiry': int((datetime.now() + timedelta(days=1) - EPOCH).total_seconds())
        }
    )
