"""
Logs every record in the stream
"""
import base64
import json
import logging
import os
from datetime import datetime

import aws_lambda_logging
from aws_kinesis_agg.deaggregator import iter_deaggregate_records
from aws_xray_sdk.core import patch_all, xray_recorder

# pylint: disable=invalid-name, line-too-long, unused-argument

patch_all()  # for xray tracing of boto libs
log = logging.getLogger()

json.JSONEncoder.default = lambda self, obj: (obj.isoformat() if isinstance(obj, type(datetime)) else str(obj))

@xray_recorder.capture()
def handler(event, context):
    """
    Logs every record in the stream
    """
    aws_lambda_logging.setup(
        level=os.environ.get('LOGLEVEL', 'INFO'),
        aws_request_id=context.aws_request_id,
        boto_level='CRITICAL'
    )
    raw_kinesis_records = event['Records']
    for kinesis_record in iter_deaggregate_records(raw_kinesis_records):
        try:
            kinesis_record['kinesis']['data'] = json.loads(base64.b64decode(kinesis_record['kinesis']['data']))
        except json.JSONDecodeError:
            pass
        log.info(kinesis_record)
