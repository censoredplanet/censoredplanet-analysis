# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Beam pipeline for converting json scan files into bigquery tables."""

from __future__ import absolute_import

import datetime
import json
import logging
import os
import re
from pprint import pprint
from typing import Optional, Tuple, Dict, List, Any, Iterator, Iterable
import uuid

import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery as beam_bigquery
from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from google.cloud import bigquery as cloud_bigquery

# Custom Types
#
# All or part of a scan row to be written to bigquery
# ex (only scan data): {'domain': 'test.com', 'ip': '1.2.3.4', 'success' true}
# ex (only ip metadata): {'asn': 13335, 'as_name': 'CLOUDFLAREINC'}
# ex (both): {'domain': 'test.com', 'ip': '1.2.3.4', 'asn': 13335}
Row = Dict[str, Any]
#
# A key containing a date and IP
# ex: ("2020-01-01", '1.2.3.4')
DateIpKey = Tuple[str, str]

# Tables have names like 'echo_scan' and 'http_scan
BASE_TABLE_NAME = 'scan'
# Prod data goes in the `firehook-censoredplanet:base' dataset
PROD_DATASET_NAME = 'base'

SCAN_BIGQUERY_SCHEMA = {
    # Columns from Censored Planet data
    'domain': 'string',
    'ip': 'string',
    'date': 'date',
    'start_time': 'timestamp',
    'end_time': 'timestamp',
    'retries': 'integer',
    'sent': 'string',
    'received': 'string',
    'error': 'string',
    'blocked': 'boolean',
    'success': 'boolean',
    'fail_sanity': 'boolean',
    'stateful_block': 'boolean',
    'measurement_id': 'string',
    'source': 'string',
    # Columns added from CAIDA data
    'netblock': 'string',
    'asn': 'integer',
    'as_name': 'string',
    'as_full_name': 'string',
    'as_class': 'string',
    'country': 'string',
}
# Future fields
"""
    'domain_category': 'string',
    'as_traffic': 'integer',
"""

# Mapping of each scan type to the zone to run its pipeline in.
# This adds more parallelization when running all pipelines.
SCAN_TYPES_TO_ZONES = {
    'https': 'us-east1',  # https has the most data, so it gets the best zone.
    'http': 'us-east4',
    'echo': 'us-west1',
    'discard': 'us-central1',
}

ALL_SCAN_TYPES = SCAN_TYPES_TO_ZONES.keys()

# PCollection key names used internally by the beam pipeline
IP_METADATA_PCOLLECTION_NAME = 'metadata'
ROWS_PCOLLECION_NAME = 'rows'


def get_bigquery_schema(
    field_types: Dict[str, str]) -> beam_bigquery.TableSchema:
  """Return a beam bigquery schema for the output table.

  Args:
    field_types: dict of {'field_name': 'column_type'}

  Returns:
    A bigquery table schema
  """
  table_schema = beam_bigquery.TableSchema()

  for (name, field_type) in field_types.items():

    field_schema = beam_bigquery.TableFieldSchema()
    field_schema.name = name
    field_schema.type = field_type
    field_schema.mode = 'nullable'  # all fields are flat
    table_schema.fields.append(field_schema)

  return table_schema


def source_from_filename(filepath: str) -> str:
  """Get the source string from a scan filename.

  Source represents the .tar.gz container which held this file.

  Args:
    filepath:
    'gs://firehook-scans/echo/CP_Quack-echo-2020-08-23-06-01-02/results.json'

  Returns:
    Just the 'CP_Quack-echo-2020-08-23-06-01-02' source
  """
  path = os.path.split(filepath)[0]
  path_end = os.path.split(path)[1]
  return path_end


def get_existing_datasources(table_name: str) -> List[str]:
  """Given a table return all sources that contributed to the table.

  Args:
    table_name: name of a bigquery table like
      'firehook-censoredplanet:echo_results.scan_test'

  Returns:
    List of data sources. ex ['CP_Quack-echo-2020-08-23-06-01-02']
  """
  # This needs to be created locally
  # because bigquery client objects are unpickable.
  # So passing in a client to the class breaks the pickling beam uses
  # to send state to remote machines.
  client = cloud_bigquery.Client()

  # Bigquery table names are of the format project:dataset.table
  # but this library wants the format project.dataset.table
  fixed_table_name = table_name.replace(':', '.')

  query = 'SELECT DISTINCT(source) AS source FROM `{table}`'.format(
      table=fixed_table_name)
  rows = client.query(query)
  sources = [row.source for row in rows]

  return sources


def make_tuple(line: str, filename: str) -> Tuple[str, str]:
  """Helper method for making a tuple from two args."""
  return (filename, line)


def read_scan_text(
    p: beam.Pipeline,
    filenames: List[str]) -> beam.pvalue.PCollection[Tuple[str, str]]:
  """Read in all individual lines for the given data sources.

  Args:
    p: beam pipeline object
    filenames: List of files to read from

  Returns:
    A PCollection[Tuple[filename, line]] of all the lines in the files keyed
    by
    filename
  """
  # List[PCollection[Tuple[filename,line]]]
  line_pcollections_per_file: List[beam.PCollection[Tuple[str, str]]] = []

  for filename in filenames:
    # PCollection[line]
    lines = p | 'read file ' + filename >> beam.io.ReadFromText(filename)

    step_name = 'annotate filename ' + filename

    # PCollection[Tuple[filename,line]]
    lines_with_filenames = (
        lines | step_name >> beam.Map(make_tuple, filename).with_output_types(
            Tuple[str, str]))

    line_pcollections_per_file.append(lines_with_filenames)

  # PCollection[Tuple[filename,line]]
  lines = (
      tuple(line_pcollections_per_file)
      | 'flatten lines' >> beam.Flatten(pipeline=p).with_output_types(
          Tuple[str, str]))

  return lines


def between_dates(filename: str,
                  start_date: Optional[datetime.date] = None,
                  end_date: Optional[datetime.date] = None) -> bool:
  """Return true if a filename is between (or matches either of) two dates.

  Args:
    filename: string of the format
    "gs://firehook-scans/http/CP_Quack-http-2020-05-11-01-02-08/results.json"
    start_date: date object or None
    end_date: date object or None

  Returns:
    boolean
  """
  date = datetime.date.fromisoformat(
      re.findall(r'\d\d\d\d-\d\d-\d\d', filename)[0])
  if start_date and end_date:
    return start_date <= date <= end_date
  elif start_date:
    return start_date <= date
  elif end_date:
    return date <= end_date
  else:
    return True


def flatten_measurement(filename: str, line: str) -> Iterator[Row]:
  """Flatten a measurement string into several roundtrip rows.

  Args:
    filename: a filepath string
    line: a json string describing a censored planet measurement. example
    {'Keyword': 'test.com,
     'Server': '1.2.3.4',
     'Results': [{'Success': true},
                 {'Success': false}]}

  Yields:
    Dicts containing individual roundtrip information
    {'column_name': field_value}
    examples:
    {'domain': 'test.com', 'ip': '1.2.3.4', 'success': true}
    {'domain': 'test.com', 'ip': '1.2.3.4', 'success': true}
  """

  try:
    scan = json.loads(line)
  except json.decoder.JSONDecodeError as e:
    logging.warning('JSONDecodeError: %s\nFilename: %s\n%s\n', e, filename,
                    line)
    return

  # Add a unique id per-measurement so single retry rows can be reassembled
  random_measurement_id = uuid.uuid4().hex

  for result in scan['Results']:
    received = result.get('Received', '')
    if isinstance(received, str):
      received_flat = received
    else:
      # TODO figure out a better way to deal with the structure in http/https
      received_flat = json.dumps(received)

    date = result['StartTime'][:10]
    row = {
        'domain': scan['Keyword'],
        'ip': scan['Server'],
        'date': date,
        'start_time': result['StartTime'],
        'end_time': result['EndTime'],
        'retries': scan['Retries'],
        'sent': result['Sent'],
        'received': received_flat,
        'error': result.get('Error', ''),
        'blocked': scan['Blocked'],
        'success': result['Success'],
        'fail_sanity': scan['FailSanity'],
        'stateful_block': scan['StatefulBlock'],
        'measurement_id': random_measurement_id,
        'source': source_from_filename(filename),
    }
    yield row


def make_date_ip_key(row: Row) -> DateIpKey:
  """Makes a tuple key of the date and ip from a given row dict."""
  return (row['date'], row['ip'])


def merge_metadata_with_rows(key: DateIpKey,
                             value: Dict[str, List[Row]]) -> Iterator[Row]:
  # pyformat: disable
  """Merge a list of rows with their corresponding metadata information.

  Args:
    key: The DateIpKey tuple that we joined on. This is thrown away.
    value: A two-element dict
      {IP_METADATA_PCOLLECTION_NAME: One element list containing an ipmetadata
               ROWS_PCOLLECION_NAME: Many element list containing row dicts}
      where ipmetadata is a dict of the format {column_name, value}
       {'netblock': '1.0.0.1/24', 'asn': 13335, 'as_name': 'CLOUDFLARENET', ...}
      and row is a dict of the format {column_name, value}
       {'domain': 'test.com', 'ip': '1.1.1.1', 'success': true ...}

  Yields:
    row dict {column_name, value} containing both row and metadata cols/values
  """
  # pyformat: enable
  ip_metadata = value[IP_METADATA_PCOLLECTION_NAME][0]
  rows = value[ROWS_PCOLLECION_NAME]

  for row in rows:
    new_row: Row = {}
    new_row.update(row)
    new_row.update(ip_metadata)
    yield new_row


def get_partition_params() -> Dict[str, Any]:
  """Returns additional partitioning params to pass with the bigquery load.

  Returns: A dict of query params, See:
  https://beam.apache.org/releases/pydoc/2.14.0/apache_beam.io.gcp.bigquery.html#additional-parameters-for-bigquery-tables
  https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#resource:-table
  """
  partition_params = {
      'timePartitioning': {
          'type': 'DAY',
          'field': 'date'
      },
      'clustering': {
          'fields': ['country', 'asn']
      }
  }
  return partition_params


def get_job_name(table_name: str, incremental_load: bool) -> str:
  """Creates the job name for the beam pipeline.

  Pipelines with the same name cannot run simultaneously.

  Args:
    table_name: a dataset.table name like 'base.scan_echo'
    incremental_load: boolean. whether the job is incremental.

  Returns:
    A string like 'write-base-scan-echo'
  """
  # no underscores or periods are allowed in beam job names
  fixed_table_name = table_name.replace('_', '-').replace('.', '-')

  if incremental_load:
    return 'append-' + fixed_table_name
  else:
    return 'write-' + fixed_table_name


def get_table_name(dataset_name: str, scan_type: str, base_table_name: str):
  """Construct a bigquery table name.

  Args:
    dataset_name: dataset name like 'base' or 'laplante'
    scan_type: data type, one of 'echo', 'discard', 'http', 'https'
    base_table_name: table name like 'scan'

  Returns:
    a dataset.table name like 'base.echo_scan'
  """
  return f'{dataset_name}.{scan_type}_{base_table_name}'


class ScanDataBeamPipelineRunner():
  """A runner to collect cloud values and run a corrosponding beam pipeline."""

  def __init__(self, project: str, schema: Dict[str, str], bucket: str,
               staging_location: str, temp_location: str,
               ip_metadata_class: type, ip_metadata_bucket_folder: str):
    """Initialize a pipeline runner.

    Args:
      project: google cluod project name
      schema: bigquery schema
      bucket: gcs bucket name
      staging_location: gcs bucket name, used for staging beam data
      temp_location: gcs bucket name, used for temp beam data
      ip_metadata_class: an IpMetadataInterface subclass (class, not instance)
      ip_metadata_bucket_folder: gcs folder with ip metadata files
    """
    self.project = project
    self.schema = schema
    self.bucket = bucket
    self.staging_location = staging_location
    self.temp_location = temp_location
    # Because an instantiated IpMetadata object is too big for beam's
    # serlalization to pass around we pass in the class to instantiate instead.
    self.ip_metadata_class = ip_metadata_class
    self.ip_metadata_bucket_folder = ip_metadata_bucket_folder

  def get_full_table_name(self, table_name: str):
    """Get a full project:dataset.table name.

    Args:
      table_name: a dataset.table name

    Returns:
      project:dataset.table name
    """
    return self.project + ':' + table_name

  def data_to_load(self,
                   gcs: GCSFileSystem,
                   scan_type: str,
                   incremental_load: bool,
                   table_name: str,
                   start_date: Optional[datetime.date] = None,
                   end_date: Optional[datetime.date] = None) -> List[str]:
    """Select the right files to read.

    Args:
      gcs: GCSFileSystem object
      scan_type: one of 'echo', 'discard', 'http', 'https'
      incremental_load: boolean. If true, only read the latest new data
      table_name: dataset.table name like 'base.scan_echo'
      start_date: date object, only files after or at this date will be read
      end_date: date object, only files at or before this date will be read

    Returns:
      A List of filename strings. ex
       ['gs://firehook-scans/echo/CP_Quack-echo-2020-08-22-06-08-03/results.json',
        'gs://firehook-scans/echo/CP_Quack-echo-2020-08-23-06-01-02/results.json']
    """
    if incremental_load:
      full_table_name = self.get_full_table_name(table_name)
      existing_sources = get_existing_datasources(full_table_name)
    else:
      existing_sources = []

    # Both zipped and unzipped data to be read in
    zipped_regex = self.bucket + scan_type + '/**/results.json.gz'
    unzipped_regex = self.bucket + scan_type + '/**/results.json'

    zipped_metadata = [m.metadata_list for m in gcs.match([zipped_regex])][0]
    unzipped_metadata = [m.metadata_list for m in gcs.match([unzipped_regex])
                        ][0]
    file_metadata = zipped_metadata + unzipped_metadata

    filenames = [metadata.path for metadata in file_metadata]
    file_sizes = [metadata.size_in_bytes for metadata in file_metadata]

    filtered_filenames = [
        filename for (filename, file_size) in zip(filenames, file_sizes)
        if (between_dates(filename, start_date, end_date) and
            source_from_filename(filename) not in existing_sources and
            file_size != 0)
    ]
    return filtered_filenames

  def add_metadata(
      self, rows: beam.pvalue.PCollection[Row]) -> beam.pvalue.PCollection[Row]:
    """Add ip metadata to a collection of roundtrip rows.

    Args:
      rows: beam.PCollection[Row]

    Returns:
      PCollection[Row]
      The same rows as above with with additional metadata columns added.
    """

    # PCollection[Tuple[DateIpKey,Row]]
    rows_keyed_by_ip_and_date = (
        rows
        | 'key by ips and dates' >>
        beam.Map(lambda row: (make_date_ip_key(row), row)).with_output_types(
            Tuple[DateIpKey, Row]))

    # PCollection[DateIpKey]
    ips_and_dates = (
        rows_keyed_by_ip_and_date
        | 'get keys' >> beam.Keys().with_output_types(DateIpKey))

    # PCollection[DateIpKey]
    deduped_ips_and_dates = (
        ips_and_dates | 'dedup' >> beam.Distinct().with_output_types(DateIpKey))

    # PCollection[Tuple[date,List[ip]]]
    grouped_ips_by_dates = (
        deduped_ips_and_dates | 'group by date' >>
        beam.GroupByKey().with_output_types(Tuple[str, Iterable[str]]))

    # PCollection[Tuple[DateIpKey,Row]]
    ips_with_metadata = (
        grouped_ips_by_dates
        | 'get ip metadata' >> beam.FlatMapTuple(
            self.add_ip_metadata).with_output_types(Tuple[DateIpKey, Row]))

    # PCollection[Tuple[Tuple[date,ip],Dict[input_name_key,List[Row]]]]
    grouped_metadata_and_rows = (({
        IP_METADATA_PCOLLECTION_NAME: ips_with_metadata,
        ROWS_PCOLLECION_NAME: rows_keyed_by_ip_and_date
    }) | 'group by keys' >> beam.CoGroupByKey())

    # PCollection[Row]
    rows_with_metadata = (
        grouped_metadata_and_rows
        | 'merge metadata with rows' >>
        beam.FlatMapTuple(merge_metadata_with_rows).with_output_types(Row))

    return rows_with_metadata

  def add_ip_metadata(self, date: str,
                      ips: List[str]) -> Iterator[Tuple[DateIpKey, Row]]:
    """Add Autonymous System metadata for ips in the given rows.

    Args:
      date: a 'YYYY-MM-DD' date key
      ips: a list of ips

    Yields:
      Tuples (DateIpKey, metadata_dict)
      where metadata_dict is a row Dict[column_name, values]
    """
    ip_metadata_db = self.ip_metadata_class(
        datetime.date.fromisoformat(date), self.ip_metadata_bucket_folder, True)
    for ip in ips:
      metadata_key = (date, ip)

      try:
        (netblock, asn, as_name, as_full_name, as_type,
         country) = ip_metadata_db.lookup(ip)
        metadata_values = {
            'netblock': netblock,
            'asn': asn,
            'as_name': as_name,
            'as_full_name': as_full_name,
            'as_class': as_type,
            'country': country,
        }

      except KeyError as e:
        logging.warning('KeyError: %s\n', e)
        metadata_values = {}  # values are missing, but entry should still exist

      yield (metadata_key, metadata_values)

  def write_to_bigquery(self, rows: beam.pvalue.PCollection[Row],
                        table_name: str, incremental_load: bool):
    """Write out row to a bigquery table.

    Args:
      rows: PCollection[Row] of data to write.
      table_name: dataset.table name like 'base.echo_scan' Determines which
        tables to write to.
      incremental_load: boolean. If true, only load the latest new data, if
        false reload all data.

    Raises:
      Exception: if any arguments are invalid.
    """
    if incremental_load:
      write_mode = beam.io.BigQueryDisposition.WRITE_APPEND
    else:
      write_mode = beam.io.BigQueryDisposition.WRITE_TRUNCATE

    (rows | 'Write' >> beam.io.WriteToBigQuery(
        self.get_full_table_name(table_name),
        schema=get_bigquery_schema(self.schema),
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=write_mode,
        additional_bq_parameters=get_partition_params()))

  def get_pipeline_options(self, scan_type: str,
                           job_name: str) -> PipelineOptions:
    """Sets up pipeline options for a beam pipeline.

    Args:
      scan_type: one of 'echo', 'discard', 'http', 'https'
      job_name: a name for the dataflow job

    Returns:
      PipelineOptions
    """
    pipeline_options = PipelineOptions(
        runner='DataflowRunner',
        project=self.project,
        region=SCAN_TYPES_TO_ZONES[scan_type],
        staging_location=self.staging_location,
        temp_location=self.temp_location,
        job_name=job_name,
        runtime_type_check=False,  # slow in prod
        setup_file='./pipeline/setup.py')
    pipeline_options.view_as(SetupOptions).save_main_session = True

    return pipeline_options

  def run_beam_pipeline(self, scan_type: str, incremental_load: bool,
                        job_name: str, table_name: str,
                        start_date: Optional[datetime.date],
                        end_date: Optional[datetime.date]):
    """Run a single apache beam pipeline to load json data into bigquery.

    Args:
      scan_type: one of 'echo', 'discard', 'http', 'https'
      incremental_load: boolean. If true, only load the latest new data, if
        false reload all data.
      job_name: string name for this pipeline job.
      table_name: dataset.table name like 'base.scan_echo'
      start_date: date object, only files after or at this date will be read.
        Mostly only used during development.
      end_date: date object, only files at or before this date will be read.
        Mostly only used during development.

    Raises:
      Exception: if any arguments are invalid or the pipeline fails.
    """
    logging.getLogger().setLevel(logging.INFO)
    pipeline_options = self.get_pipeline_options(scan_type, job_name)
    gcs = GCSFileSystem(pipeline_options)

    new_filenames = self.data_to_load(gcs, scan_type, incremental_load,
                                      table_name, start_date, end_date)
    if not new_filenames:
      logging.info('No new files to load incrementally')
      return

    with beam.Pipeline(options=pipeline_options) as p:
      # PCollection[Tuple[filename,line]]
      lines = read_scan_text(p, new_filenames)

      # PCollection[Row]
      rows = (
          lines | 'flatten json' >>
          beam.FlatMapTuple(flatten_measurement).with_output_types(Row))

      # PCollection[Row]
      rows_with_metadata = self.add_metadata(rows)

      self.write_to_bigquery(rows_with_metadata, table_name, incremental_load)
