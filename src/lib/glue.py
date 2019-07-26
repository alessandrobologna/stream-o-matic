import logging

import backoff
import boto3
import botocore

from aws_xray_sdk.core import patch_all, xray_recorder
from cachetools import cached, TTLCache
from lib.tables import GlueTable

# pylint: disable=invalid-name, line-too-long
patch_all()  # for xray tracing of boto libs

glue_table_cache = TTLCache(maxsize=1000, ttl=300)
glue = boto3.client('glue')
log = logging.getLogger()

@xray_recorder.capture()
@cached(glue_table_cache)
@backoff.on_exception(backoff.expo, botocore.exceptions.ClientError, max_time=10)
def get_glue_table(database_name: str, table_name: str) -> dict:
    """
    Get Glue Table
    """
    try:
        return GlueTable(glue.get_table(
            DatabaseName=database_name,
            Name=table_name
        ).get('Table'))
    except glue.exceptions.EntityNotFoundException:
        log.error({
            'Code': 404,
            'Message': f'table {database_name}.{table_name} does not exists'
        })
