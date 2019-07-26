"""
Listen to CloudTrail S3 events, and updates the partition map for the
corresponding Glue table in DynamoDB
"""
import json
import logging
import os
import re
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import  Key, Attr
import backoff
import botocore
from aws_xray_sdk.core import patch_all, xray_recorder
from cachetools import cached, TTLCache

from lib.decorators import KinesisRecord, kinesis_handler

# pylint: disable=invalid-name, line-too-long, unused-argument

patch_all()  # for xray tracing of boto libs
dynamodb = boto3.resource('dynamodb')
log = logging.getLogger()
dynamo_table_cache = TTLCache(maxsize=1000, ttl=3600)

json.JSONEncoder.default = lambda self, obj: (obj.isoformat() if isinstance(obj, type(datetime)) else str(obj))
S3_ARN_TO_PARTS = re.compile(r'arn:aws:s3:([^:]*):(\d*):([^/]+)/(.*)')
OPENED = "opened"
CLOSED = "closed"
GLUE_TABLES_LOCATOR = os.environ['GLUE_TABLES_LOCATOR']
GLUE_PARTITIONS_MAPPER = os.environ['GLUE_PARTITIONS_MAPPER']

@xray_recorder.capture()
@kinesis_handler(event_types=[
    KinesisRecord.S3_SOURCE_EVENT
])
def handler(kinesis_records, context):
    """
    Listen to CloudTrail S3 events, and updates the partition map for the corresponding Glue table in DynamoDB
    """
    detail = kinesis_records[0].parse().get('detail')
    for resource in detail.get('resources', []):
        if resource.get('type') == 'AWS::S3::Object':
            match = S3_ARN_TO_PARTS.match(resource.get('ARN'))
            log.info(resource.get('ARN'))
            if match:
                bucket = match[3]
                prefix = match[4].rsplit('/', 1)[0]
                return append_partition(get_table(bucket, prefix), bucket, prefix)
    return None

@xray_recorder.capture()
@cached(dynamo_table_cache)
@backoff.on_exception(backoff.expo, botocore.exceptions.ClientError, max_time=10)
def get_table(bucket: str, prefix: str):
    """
    Find Glue Table based on location
    """
    dynamo = dynamodb.Table(GLUE_TABLES_LOCATOR)
    while prefix:
        location = f"s3://{bucket}/{prefix}"
        item = dynamo.get_item(
            Key={
                'location': location
            }
        ).get('Item')
        if item:
            symlink = item.get('symlink')
            if symlink:
                symlinked = dynamo.get_item(
                    Key={
                        'location': symlink
                    }
                ).get('Item')
                # merge the two records
                if symlinked:
                    symlinked.update(item)
                    return symlinked
            return item
        if not '/' in prefix:
            return
        prefix = prefix.rsplit('/', 1)[0]

@xray_recorder.capture()
@backoff.on_exception(backoff.expo, botocore.exceptions.ClientError, max_time=10)
def append_partition(item: dict, bucket: str, prefix: str):
    """
    Append the new partition to dynamodb table of partitions
    """
    log.info(item)
    if not item:
        return f"No table at {bucket}/{prefix}"
    partition_keys = item.get('partition_keys')
    if not partition_keys:
        return f"Table at {bucket}/{prefix} is not partitioned"

    dynamo = dynamodb.Table(os.environ['GLUE_PARTITIONS_MAPPER'])
    location = item['location']
    partition = prefix.replace(location.split('/', 3)[3], "")[1:]
    symlink = item.get('symlink')
    partition_segments = partition.split('/')
    db_table = f'{item["database_name"]}.{item["table_name"]}'

    batch_items: list = []
    values: list = []
    timestamp = datetime.utcnow().isoformat()
    for idx, partition_key in enumerate(partition_keys):
        if idx >= len(partition_segments):
            break
        segment = partition_segments[idx]
        if not segment:
            continue
        values.append(segment.split('=')[-1])
        locator = f'{location}:{idx:02}:{partition_key["Name"]}'
        partition = '/'.join([partition_segments[i] for i in range(0, idx+1)])
        partition_entry = dynamo.get_item(
            Key={
                'locator': locator,
                'partition': partition
            }
        ).get('Item')
        if partition_entry:
            if partition_entry['state'] == CLOSED:
                partition_entry['state'] = OPENED
                partition_entry['updated'] = timestamp
                log.info(f'object added to closed partition at {partition_entry["location"]}, reopening')
                batch_items.append(partition_entry)
        else:
            partition_entry = {
                'locator': locator,
                'partition': partition,
                'location': f's3://{bucket}/{prefix}',
                'db_table': db_table,
                'state': OPENED,
                'values': values,
                'leaf': idx == len(partition_keys) - 1,
                'compacted': -1,
                'created': timestamp,
                'updated': timestamp
            }
            if symlink:
                partition_entry['symlink'] = symlink
            log.info(f'object added to new partition at {partition_entry["location"]}')
            batch_items.append(partition_entry)
        # find and close all other sibling partitions that are still open
        open_partitions = dynamo.query(
            IndexName='state-index',
            KeyConditionExpression=Key('locator').eq(locator) & Key('state').eq(OPENED),
            FilterExpression=Attr('partition').ne(partition)
        ).get('Items')
        for open_partition in open_partitions:
            open_partition['state'] = CLOSED
            open_partition['compacted'] = open_partition.get('compacted', -1) + 1
            open_partition['updated'] = timestamp
            log.info(f'closing partition at {open_partition["locator"]}, {open_partition["partition"]}')
            batch_items.append(open_partition)
    if batch_items:
        with dynamo.batch_writer(overwrite_by_pkeys=['locator', 'partition']) as batch:
            for batch_item in batch_items:
                batch.put_item(Item=batch_item)
        return batch_items
