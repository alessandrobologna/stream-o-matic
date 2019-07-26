"""
Creates a CloudWatch Log Stream for each Firehose
"""
import json
import logging
from datetime import datetime

import backoff
import boto3
import botocore
from aws_xray_sdk.core import patch_all, xray_recorder

from lib.decorators import kinesis_handler, KinesisRecord

# pylint: disable=invalid-name, line-too-long, unused-argument

patch_all()  # for xray tracing of boto libs
logs = boto3.client('logs')
log = logging.getLogger()

json.JSONEncoder.default = lambda self, obj: (obj.isoformat() if isinstance(obj, type(datetime)) else str(obj))

@xray_recorder.capture()
@kinesis_handler(event_types=[
    KinesisRecord.FIREHOSE_SOURCE_EVENT
])
def handler(kinesis_records, context):
    """
    Listen to CloudTrail Firehose events, and create a log stream for the firehose if it doesn't exists
    """
    detail = kinesis_records[0].parse().get('detail')
    if detail:
        log.info(detail)
        try:
            logging_options = detail['requestParameters']['extendedS3DestinationConfiguration']['cloudWatchLoggingOptions']
            if logging_options.get('enabled'):
                return create_logstream(logging_options)
        except (KeyError, TypeError):
            pass
    return None

@xray_recorder.capture()
@backoff.on_exception(backoff.expo, botocore.exceptions.ClientError, max_time=10)
def create_logstream(logging_options: dict):
    """
    Create a log stream given the logging options
    """
    try:
        return logs.create_log_stream(
            logGroupName=logging_options.get('logGroupName'),
            logStreamName=logging_options.get('logStreamName')
        )
    except logs.exceptions.ResourceAlreadyExistsException:
        # there's already a log stream
        pass
    except logs.exceptions.ResourceNotFoundException:
        # log group is missing
        logs.create_log_group(
            logGroupName=logging_options.get('logGroupName')
        )
        return create_logstream(logging_options)
    except logs.exceptions.InvalidParameterException as ex:
        # not a valid name
        log.error(ex)
