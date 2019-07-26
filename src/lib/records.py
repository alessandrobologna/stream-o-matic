"""
Defines Kinesis and DynamoDB streams base record structures
"""
import re
import json
from base64 import b64decode

from collections.abc import Mapping

# pylint: disable=invalid-name, line-too-long, unused-argument

class KinesisRecord(Mapping):
    """
    Wraps a Kinesis record
    """
    # GLUE_EVENT = re.compile(r'arn:aws:glue:([^:]*):(\d*):table/([^/]+)/([^/]+)')
    # S3_EVENT = re.compile(r'arn:aws:s3:([^:]*):(\d*):([^/]+)/(.*)')
    GLUE_SOURCE_EVENT = re.compile(r'(aws.glue):?(.*)')
    S3_SOURCE_EVENT = re.compile(r'(aws.s3):?(.*)')
    FIREHOSE_SOURCE_EVENT = re.compile(r'(aws.firehose):?(.*)')
    FIREHOSE_TARGET_EVENT = re.compile(r'arn:aws:firehose:([^:]*):(\d*):deliverystream/([^:]+)(:.*)?')
    OPENED_PARTITION_EVENT = re.compile(r'(custom.event.partition.opened):?(.*)')
    CLOSED_PARTITION_EVENT = re.compile(r'(custom.event.partition.closed):?(.*)')

    match = None
    def __init__(self, *args, **kw):
        self._storage = dict(*args, **kw)
    def __getitem__(self, key):
        return self._storage[key]
    def __iter__(self):
        return iter(self._storage)
    def __len__(self):
        return len(self._storage)
    def dump(self):
        return self._storage
    
    def get_type(self):
        return self._storage['kinesis']['partitionKey'].replace('"', '')

    def is_any_of(self, regexes):
        self.match = None
        for expression in regexes:
            self.match = re.match(expression, self.get_type())
            if self.match:
                return True
        return False

    def get_evaluated_match(self):
        return self.match

    def decode(self) -> dict:
        """
        returns the base64 decoded record data
        """
        return b64decode(self._storage['kinesis']['data'])

    def parse(self) -> dict:
        return json.loads(self.decode())
    


class DynamoRecord(Mapping):
    """
    Wraps a DynamoDB record
    """
    INSERT = "INSERT"
    MODIFY = "MODIFY"
    REMOVE = "REMOVE"
    def __init__(self, *args, **kw):
        self._storage = dict(*args, **kw)
    def __getitem__(self, key):
        return self._storage[key]
    def __iter__(self):
        return iter(self._storage)
    def __len__(self):
        return len(self._storage)
    def dump(self):
        return self._storage
    
    def get_type(self):
        return self._storage['eventName']

    def is_any_of(self, types):
        for entry in types:
            if self.get_type() == entry:
                return True
        return False

    def requeue(self):
        pass

