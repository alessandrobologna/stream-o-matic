"""
Minimalistic almost-passthrough Firehose record processor
"""

import os
import logging
import base64
import json

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all
import aws_lambda_logging

# pylint: disable=invalid-name, line-too-long, unused-argument, broad-except, logging-fstring-interpolation

patch_all()  # for xray tracing of boto libs
log = logging.getLogger()

@xray_recorder.capture()
def handler(event, context):
    """
    Minimalistic almost-passthrough Firehose record processor
    """
    aws_lambda_logging.setup(
        level=os.environ.get('LOGLEVEL', 'INFO'),
        aws_request_id=context.aws_request_id,
        boto_level='CRITICAL'
    )
    received_raw_firehose_records = event['records']
    log.info(f'Received {len(received_raw_firehose_records)} events')
   
    return {
        'records' : [
            transform(firehose_record)
            for firehose_record in received_raw_firehose_records
        ]
    }

@xray_recorder.capture()
def transform(firehose_record: dict):
    """
    Add firehose metadata to the source record
    """
    transformed = {
        'recordId': firehose_record['recordId'],
        'result': 'ProcessingFailed',
        'data' : firehose_record['data']
    }
    try:
        data = json.loads(base64.b64decode(firehose_record['data']))
        data['firehose'] = {
            'record_id': firehose_record['recordId'],
            'timestamp': firehose_record['approximateArrivalTimestamp']
        }
        transformed['data'] = base64.b64encode(json.dumps(data).encode("utf-8")).decode('utf-8')
        transformed['result'] = 'Ok'
    except Exception as ex:
        log.error(ex)
    return transformed
