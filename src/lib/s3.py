import re
import boto3
import botocore
from aws_xray_sdk.core import xray_recorder
import backoff

# pylint: disable=invalid-name

s3 = boto3.client('s3')

S3_LOCATION_REGEX = r's3://([^/]+)/(.*)'
@xray_recorder.capture()
@backoff.on_exception(backoff.expo, botocore.exceptions.ClientError, max_time=10)
def upload_file(location, body):
    """
    Upload an object in a given s3 location
    """
    match = re.match(S3_LOCATION_REGEX, location)
    if match:
        return s3.put_object(
            Body=body,
            Bucket=match[1],
            Key=match[2]
        )

@backoff.on_exception(backoff.expo, botocore.exceptions.ClientError, max_time=10)
def list_generator(location: str):
    """
    Iterates the bucket listings at the given prefix, ensuring that '/' is appended to it if not present
    """
    match = re.match(S3_LOCATION_REGEX, location)
    if match:
        bucket = match[1]
        prefix = match[2]
        kwargs = {
            'Bucket': bucket,
            'Prefix': prefix if prefix[-1:]=='/' else prefix + '/'
        }
        while True:
            response = s3.list_objects_v2(**kwargs)
            contents = response.get('Contents')
            if contents:
                for obj in contents:
                    yield obj['Key']
            token = response.get('NextContinuationToken')
            if not token:
                break
            kwargs['ContinuationToken'] = token

@backoff.on_exception(backoff.expo, botocore.exceptions.ClientError, max_time=10)
def check_prefix(location):
    """
    Return true if there's at least a file at prefix
    """
    match = re.match(S3_LOCATION_REGEX, location)
    if match:
        bucket = match[1]
        prefix = match[2]
        kwargs = {
            'Bucket': bucket,
            'Prefix': prefix if prefix[-1:]=='/' else prefix + '/'
        }
        return s3.list_objects_v2(**kwargs).get('Contents') is not None
    return False