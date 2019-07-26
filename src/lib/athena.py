import json
import os
import logging
import backoff

import boto3
import botocore
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all

# pylint: disable=invalid-name, line-too-long, unused-argument

log = logging.getLogger()

patch_all()  # for xray tracing of boto libs

athena = boto3.client('athena')

@xray_recorder.capture()
@backoff.on_exception(backoff.expo, botocore.exceptions.ClientError, max_time=10)
def execute_query(query, bucket):
    # normalize query string
    query = ' '.join(query.replace('\n', ' ').split())
    xray_recorder.current_subsegment().put_metadata(
        "statement", query, "athena"
    )
    try:
        return athena.start_query_execution(
            QueryString=query,
            ResultConfiguration={
                'OutputLocation': f's3://{bucket}/athena',
                'EncryptionConfiguration': {
                    'EncryptionOption': "SSE_S3"
                }
            }
        )
    except athena.exceptions.InvalidRequestException as ex:
        log.error(ex)

