
import os
import re
from collections.abc import Mapping

# pylint: disable=invalid-name, line-too-long

class GlueTable(Mapping):
    """
    Simply wraps the glue table params
    """
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

    def get_params(self):
        """
        Returns a GlueTableParams object wrapping this table parameters
        """
        return GlueTableParams(self._storage.get('Parameters', {}))

    def get_table_location(self):
        """
        Returns the table location without a trailing slash
        """
        location = self.get_storage_descriptor().get('Location')
        return location[:-1] if location[-1:] == '/' else location

    def get_table_bucket(self):
        """
        Returns the table bucket
        """
        location = self.get_storage_descriptor().get('Location')
        return re.match(r's3://([^/]+)/.*', location)[1]

    def get_table_prefix(self):
        """
        Returns the table prefix without a trailing slash
        """
        location = self.get_storage_descriptor().get('Location')
        prefix = re.match(r's3://[^/]+/(.*)', location)[1]
        return prefix[:-1] if prefix[-1:] == '/' else prefix

    def get_storage_descriptor(self):
        """
        Returns the storage descriptor
        """
        return self._storage.get('StorageDescriptor')

    def get_partition_keys(self):
        """
        Returns the storage descriptor
        """
        return self._storage.get('PartitionKeys', [])

    def is_symlinked(self):
        """
        Returns True if this table is using a simlink input format
        """
        return self.get_storage_descriptor().get('InputFormat') == 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'

    def get_firehose_target(self):
        """"
        Returns the table location if the InputFormat is not a SymlinkTextInputFormat,
        otherwise returns the the table location prefixed with the firehose prefix
        """
        prefix = self.get_table_prefix()
        if self.is_symlinked():
            # prepend the firehose prefix
            prefix = self.get_params().get_firehose_prefix() + prefix + self.get_params().get_firehose_suffix()
        return prefix[:-1] if prefix[-1:] == '/' else prefix

    def get_compaction_bucketing(self):
        """
        Returns a SQL statement to use when compacting the partition in buckets
        If no parameter is specified, it defaults to one bucket by the first column in the schema
        """
        columns = self.get_params().get_compaction_bucketing_columns()
        if not columns:
            columns = [self.get_storage_descriptor().get('Columns')[0]['Name']]
        return 'bucketed_by = ARRAY[' + ','.join([f"'{column}'" for column in columns]) +'], bucket_count = ' + self.get_params().get_compaction_bucketing_count()

    def get_compaction_sorting(self):
        """
        """
        columns = self.get_params().get_compaction_sorting_columns()
        if columns:
            return 'order by = ' + ','.join([f"'{column}'" for column in columns])
        return ""

class GlueTableParams(Mapping):
    """
    Simply wraps the glue table params
    """
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

    def get(self, key: str, alt: str = None) -> str:
        """
        Returns either a table parameter matching the key, or an env variable, or a fallback value
        """
        return self._storage.get(key, os.environ.get(key, alt))

    def get_int(self, key: str, alt: int = 0) -> int:
        """
        Returns either a table parameter matching the key, or an env variable, or a fallback value
        """
        return int(self.get(key, alt))

    def is_automation(self):
        return self.get('firehose_automation', "").lower() in ('true', 'yes')
    
    def get_firehose_prefix(self):
        prefix = self.get('firehose_prefix', 'firehose/')
        return prefix if prefix[-1:] == '/' else prefix + '/'

    def get_firehose_suffix(self):
        suffix = self.get('firehose_prefix', '/stage')
        return suffix if suffix[0] == '/' else '/' + suffix

    def get_partition_schema(self):
        return self.get('firehose_partition_schema')

    def get_role_arn(self):
        return self.get('firehose_role_arn')

    def get_log_group(self):
        return self.get('firehose_log_group')

    def get_buffering_seconds(self):
        return self.get_int('firehose_buffering_seconds')

    def get_buffering_mb(self):
        return self.get_int('firehose_buffering_mb')

    def get_lambda_arn(self):
        return self.get('firehose_processors_lambda_arn')

    def get_processors_lambda_retries(self):
        return self.get('firehose_processors_lambda_retries', '3')

    def get_processors_lambda_buffer_mb(self):
        return self.get('firehose_processors_lambda_buffer_mb', '1')

    def get_processors_lambda_buffer_seconds(self):
        return self.get('firehose_processors_lambda_buffer_seconds', '60')

    def get_compaction_bucketing_count(self):
        return self.get('firehose_compaction_bucketing_count', '1')

    def get_compaction_bucketing_columns(self): 
        columns = self.get('firehose_compaction_bucketing_columns')
        if columns:
            return [
                column.strip() for column in columns.split(',')
            ]
        return []

    def get_compaction_sorting_columns(self):
        columns = self.get('firehose_compaction_sorting_columns')
        if columns:
            return [
                column.strip() for column in columns.split(',')
            ]
        return []
