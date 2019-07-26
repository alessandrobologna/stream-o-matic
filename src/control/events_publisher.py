""""
Listen to CloudTrail Events and publishes them to Kinesis
"""
import json
import logging
import os
from datetime import datetime

import boto3
import botocore
import backoff
from aws_xray_sdk.core import patch_all, xray_recorder
import aws_lambda_logging
 
from jsonpath_ng.ext import parse

# pylint: disable=invalid-name, line-too-long, unused-argument

patch_all()  # for xray tracing of boto libs
kinesis = boto3.client('kinesis')
log = logging.getLogger()

source_path_map: dict = {
    'aws.s3' : parse('$[detail][resources][?(@.type=="AWS::S3::Object")].ARN'),
    'aws.firehose' : parse('$.detail.responseElements.deliveryStreamARN'),
    'aws.glue' : parse('$.resources[0]')
}


@xray_recorder.capture()
def handler(event: dict, context):
    """
    Listen to CloudTrail Events and publishes them to Kinesis
    """
    aws_lambda_logging.setup(
        level=os.environ.get('LOGLEVEL', 'INFO'),
        aws_request_id=context.aws_request_id,
        boto_level='CRITICAL'
    )
    log.info(event)
    source = event.get('source')
    if source:
        expr = source_path_map.get(source)
        if expr:
            values = [match.value for match in expr.find(event)]
            if values:
                log.debug(values[0])

