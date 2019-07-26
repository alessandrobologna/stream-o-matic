import logging
import os
from functools import wraps
from itertools import chain, islice

import aws_lambda_logging
from aws_kinesis_agg.deaggregator import iter_deaggregate_records

from lib.records import KinesisRecord, DynamoRecord

# pylint: disable=invalid-name, line-too-long, unused-argument

log = logging.getLogger()

def kinesis_handler(event_types, batch_size=1):
    def handler_decorator(func):
        @wraps(func)
        def lambda_handler(*args, **kwargs):
            event = args[0]
            context = args[1]
            aws_lambda_logging.setup(
                level=os.environ.get('LOGLEVEL', 'INFO'),
                aws_request_id=context.aws_request_id,
                boto_level='CRITICAL'
            )
            received_raw_kinesis_records = event['Records']
            for raw_kinesis_records in chunks(iter_deaggregate_records(received_raw_kinesis_records), batch_size):
                kinesis_records: list = []
                for raw_kinesis_record in raw_kinesis_records:
                    kinesis_record = KinesisRecord(raw_kinesis_record)
                    if kinesis_record.is_any_of(event_types):
                        kinesis_records.append(kinesis_record)
                if kinesis_records:
                    log.info({
                        "Action": "Processing",
                        "Events": [kinesis_record.get_type() for kinesis_record in kinesis_records]
                    })
                    results = func(kinesis_records, context)
                    if results:
                        log.info({
                            "Results" : results
                        })
        return lambda_handler
    return handler_decorator

def dynamo_handler(event_types, batch_size=1):
    def handler_decorator(func):
        @wraps(func)
        def lambda_handler(*args, **kwargs):
            event = args[0]
            context = args[1]
            aws_lambda_logging.setup(
                level=os.environ.get('LOGLEVEL', 'INFO'),
                aws_request_id=context.aws_request_id,
                boto_level='CRITICAL'
            )
            received_raw_dynamo_records = event['Records']
            for raw_dynamo_records in chunks(received_raw_dynamo_records, batch_size):
                dynamo_records: list = []
                for raw_dynamo_record in raw_dynamo_records:
                    dynamo_record = DynamoRecord(raw_dynamo_record)
                    if dynamo_record.is_any_of(event_types):
                        dynamo_records.append(dynamo_record)
                if dynamo_records:
                    log.info({
                        "Action": "Processing",
                        "Event": [dynamo_record.get_type() for dynamo_record in dynamo_records]
                    })
                    result = func(dynamo_records, context)
                    if result:
                        log.info({
                            "Result" : result
                        })
        return lambda_handler
    return handler_decorator

def chunks(iterable, size):
    """
    Chunks an iterable in a sequence of iterables with the given size 
    """
    iterator = iter(iterable)
    for first in iterator:
        yield chain([first], islice(iterator, size - 1))
