"""
Listen to CloudTrail Glue events, and create a Kinesis Firehose for each new Glue table created
"""
import os
import json
import logging
from datetime import datetime

import backoff
import boto3
import botocore
from aws_xray_sdk.core import patch_all, xray_recorder

from lib.decorators import kinesis_handler
from lib.records import KinesisRecord
from lib.glue import get_glue_table, glue

# pylint: disable=invalid-name, line-too-long, unused-argument

patch_all()  # for xray tracing of boto libs
firehose = boto3.client('firehose')
dynamodb = boto3.resource('dynamodb')
logs = boto3.client('logs')
log = logging.getLogger()

TMP_DATABASE = os.environ['TMP_DATABASE']

json.JSONEncoder.default = lambda self, obj: (obj.isoformat() if isinstance(obj, type(datetime)) else str(obj))


@xray_recorder.capture()
@kinesis_handler(event_types=[
    KinesisRecord.GLUE_SOURCE_EVENT
])
def handler(kinesis_records, context):
    """
    Listen to CloudTrail Glue events, and create a Kinesis Firehose for each new Glue table created
    """
    detail = kinesis_records[0].parse().get('detail')
    database_name = detail.get('databaseName')
    if database_name != TMP_DATABASE:
        return [
            create_stream(create_config(database_name, table_name)) for table_name in detail.get('changedTables') 
            if detail.get('typeOfChange') == 'CreateTable'
        ] 

@xray_recorder.capture()
@backoff.on_exception(backoff.expo, botocore.exceptions.ClientError, max_time=10)
def create_config(database_name: str, table_name: dict):
    """
    Create a Firehose configuration based on the Glue Table
    """
    table = get_glue_table(database_name, table_name)
    if not table:
        return f"Table {database_name}.{table_name} not found"
    log.info(table.dump())
    params = table.get_params()
    # only create a firehose if explicitely requested
    if not params.is_automation():
        return None

    prefix = table.get_firehose_target()
    bucket_arn = f'arn:aws:s3:::{table.get_table_bucket()}'
    partition_schema = params.get_partition_schema()

    if not partition_schema:
        # generate a partition schema based on the table
        partition_schema_elements: list = []
        for partition_key in [key.get('Name') for key in table.get_partition_keys()]:
            if partition_key.lower() in ('year', 'yyyy'):
                partition_schema_elements.append(f'{partition_key}=' + '!{timestamp:yyyy}')
            elif partition_key.lower() in ('month', 'mm'):
                partition_schema_elements.append(f'{partition_key}' + '=!{timestamp:MM}')
            elif partition_key.lower() in ('day', 'dd'):
                partition_schema_elements.append(f'{partition_key}' + '=!{timestamp:dd}')
            elif partition_key.lower() in ('hour', 'hh'):
                partition_schema_elements.append(f'{partition_key}' + '=!{timestamp:HH}')
            elif partition_key.lower() in ('minute', 'min'):
                partition_schema_elements.append(f'{partition_key}' + '=!{timestamp:mm}')
            elif partition_key.lower() in ('date', 'day', 'dt'):
                partition_schema_elements.append(f'{partition_key}' + '=!{timestamp:yyyyMMdd}')
            elif partition_key.lower() in ('time', 'tm'):
                partition_schema_elements.append(f'{partition_key}' + '=!{timestamp:HHmm}')
        partition_schema = '/'.join(partition_schema_elements)

    configuration = {
        'DatabaseName': database_name,
        'TableName': table_name,
        'RoleARN': params.get_role_arn(),
        'BucketARN': bucket_arn,
        'Prefix': f'{prefix}/{partition_schema}/',
        'ErrorOutputPrefix': f'errors/{prefix}/' + '!{firehose:error-output-type}/' + f'{partition_schema}/',
        'CloudWatchLoggingOptions': {
            'Enabled': True,
            'LogGroupName': params.get_log_group(),
            'LogStreamName': f'{database_name}.{table_name}'
        },
        'DataFormatConversionConfiguration': {
            'SchemaConfiguration': {
                'RoleARN': params.get_role_arn(),
                'DatabaseName': database_name,
                'TableName': table_name
            },
            'InputFormatConfiguration': {
                'Deserializer': {
                    'OpenXJsonSerDe': {
                        'ConvertDotsInJsonKeysToUnderscores': True,
                        'CaseInsensitive': True
                    }
                }
            },
            'OutputFormatConfiguration': {
                'Serializer': {
                    'ParquetSerDe': {
                        'Compression': 'GZIP',
                        'EnableDictionaryCompression': True
                    }
                }
            },
            'Enabled': True
        }
    }

    # optionally set custom BufferingHints
    buffering_hints: dict = {}
    buffering_seconds = params.get_buffering_seconds()
    if buffering_seconds:
        buffering_hints['IntervalInSeconds'] = buffering_seconds

    buffering_mb = params.get_buffering_mb()
    if buffering_mb:
        buffering_hints['SizeInMBs'] = buffering_mb

    if buffering_hints:
        configuration['BufferingHints'] = buffering_hints

    if params.get_lambda_arn():
        configuration['ProcessingConfiguration'] = {
            'Enabled': True,
            'Processors': [
                {
                    'Type': 'Lambda',
                    'Parameters': [
                        {
                            'ParameterName': 'LambdaArn',
                            'ParameterValue': params.get_lambda_arn() + ':$LATEST'
                        },
                        {
                            'ParameterName': 'NumberOfRetries',
                            'ParameterValue': params.get_processors_lambda_retries()
                        },
                        {
                            'ParameterName': 'RoleArn',
                            'ParameterValue': params.get_role_arn()
                        },
                        {
                            'ParameterName': 'BufferSizeInMBs',
                            'ParameterValue': params.get_processors_lambda_buffer_mb()
                        },
                        {
                            'ParameterName': 'BufferIntervalInSeconds',
                            'ParameterValue': params.get_processors_lambda_buffer_seconds()
                        },
                    ]
                },
            ]
        }
    return configuration

@xray_recorder.capture()
@backoff.on_exception(backoff.expo, botocore.exceptions.ClientError, max_time=10)
def create_stream(config: dict) -> dict:
    """
    Send payload to target firehose
    """
    if not config:
        return
    log.info(config)
    database_name = config.pop('DatabaseName')
    table_name = config.pop('TableName')
    stream_name = f'{database_name}.{table_name}'
    try:
        return firehose.create_delivery_stream(
            DeliveryStreamName=stream_name,
            DeliveryStreamType='DirectPut',
            ExtendedS3DestinationConfiguration=config
        )
    except firehose.exceptions.ResourceInUseException:
        # it's already there, just log
        log.warning({
            'Code' : 409,
            'Message': f'firehose {stream_name} is already present'
        })
