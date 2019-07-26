"""
Demultiplex a Kinesis Firehose Event sent to this stream to the corresponding firehose
"""
import os
import logging
from collections import defaultdict

import backoff
import boto3
import botocore
from aws_xray_sdk.core import patch_all, xray_recorder

from lib.decorators import kinesis_handler, KinesisRecord

# pylint: disable=invalid-name, line-too-long, unused-argument

patch_all()  # for xray tracing of boto libs
kinesis = boto3.client('kinesis')
firehose = boto3.client('firehose')
log = logging.getLogger()
DATA_STREAM = os.environ.get('DATA_STREAM')

@xray_recorder.capture()
@kinesis_handler(event_types=[
    KinesisRecord.FIREHOSE_TARGET_EVENT
], batch_size=500)
def handler(kinesis_records, context):
    """
    Fan out records in batches to the firehose based on the partition key prefix
    Failed records are requed back into the stream for later reprocessing.
    """
    res = defaultdict(list)
    for record in kinesis_records:
        res[record.get_evaluated_match()[3]].append(record)

    batches = [
        {'firehose': key, 'records': value} for key, value in res.items()
    ]
    results = [
        publish_batch(entry['firehose'], entry['records']) for entry in batches
    ]

    for failed_batch in results:
        for failed_record in failed_batch:
            log.warning({
                "Message" : "Requeing record",
                "Record" : failed_record.dump()
            })
            reque(failed_record)
    return "Success"

@xray_recorder.capture()
@backoff.on_exception(backoff.expo, botocore.exceptions.ClientError, max_time=10)
def publish_batch(target: str, records: list) -> dict:
    """
    Send payload to target firehose
    """
    try:
        result = firehose.put_record_batch(
            DeliveryStreamName=target,
            Records=[{
                'Data': record.decode()
            } for record in records]
        )
        if result.get('FailedPutCount'):
            return [
                records[i] for i, record in enumerate(result['RequestResponses'])
                if "ErrorCode" in record
            ]
    except firehose.exceptions.ResourceNotFoundException as ex:
        # it wasn't meant for a firehose after all
        log.error({
            'Code' : 404,
            'Message': f'firehose {target} is not available',
            "Exception": ex
        })
    except firehose.exceptions.InvalidArgumentException as ex:
        # it wasn't meant for a firehose after all
        log.error({
            'Code' : 500,
            'Message': f'Error reply from endpoint',
            "Exception": ex
        })
    return []


@xray_recorder.capture()
@backoff.on_exception(backoff.expo, botocore.exceptions.ClientError, max_time=10)
def reque(record):
    kinesis.put_record(
        StreamName=DATA_STREAM,
        Data=record.decode(),
        PartitionKey=record['kinesis']['partitionKey']
    )
    
