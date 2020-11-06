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
"""Beam pipeline for converting json scan files into bigquery tables.

To re-run the full beam pipeline manually (and blow away any old tables) run

python pipeline/main.py --env=prod --incremental=False
"""

from __future__ import absolute_import

import argparse
import concurrent
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

# Project values
CLOUD_PROJECT = 'firehook-censoredplanet'
BEAM_STAGING_LOCATION = 'gs://firehook-dataflow-test/staging'
BEAM_TEMP_LOCATION = 'gs://firehook-dataflow-test/temp'
INPUT_BUCKET = 'gs://firehook-scans/'

# Output table name pieces
# Tables have names like 'scan' and 'scan_test'
SCAN_TABLE_NAME = 'scan'
# Datasets have names like 'echo_results', and 'https_results'
DATASET_SUFFIX = '_results'

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

# PCollection key names used internally by the beam pipeline
IP_METADATA_PCOLLECTION_NAME = 'metadata'
ROWS_PCOLLECION_NAME = 'rows'


class ScanDataBeamPipelineRunner():
  """A runner to collect cloud values and run a corrosponding beam pipeline."""

  def __init__(self, project: str, table_name: str, dataset_suffix: str,
               schema: Dict[str, str], bucket: str, staging_location: str,
               temp_location: str):
    self.project = project
    self.base_table_name = table_name
    self.dataset_suffix = dataset_suffix
    self.schema = schema
    self.bucket = bucket
    self.staging_location = staging_location
    self.temp_location = temp_location

  @staticmethod
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

  def get_table_name(self, scan_type, env):
    """Get a bigquery table name.

    Args:
      scan_type: data type, one of 'echo', 'discard', 'http', 'https'
      env: one of 'prod' or 'dev'.

    Returns:
      a string table name like 'firehook-censoredplanet:echo_results.scan_test'

    Raises:
      Exception: if the env is invalid.
    """
    if env == 'dev':
      table_name = self.base_table_name + '_test'
    elif env == 'prod':
      table_name = self.base_table_name
    else:
      raise Exception('Invalid env: ' + env)

    dataset = scan_type + self.dataset_suffix
    return self.project + ':' + dataset + '.' + table_name

  def get_existing_datasources(self, scan_type: str, env: str) -> List[str]:
    """Given a scan type return all sources that contributed to the current table.

    Args:
      scan_type: one of 'echo', 'discard', 'http', 'https'
      env: one of 'prod' or 'dev'.

    Returns:
      List of data sources. ex ['CP_Quack-echo-2020-08-23-06-01-02']
    """
    # This needs to be created locally
    # because bigquery client objects are unpickable.
    # So passing in a client to the class breaks the pickling beam uses
    # to send state to remote machines.
    client = cloud_bigquery.Client()

    source_table = self.get_table_name(scan_type, env)
    # Bigquery table names are of the format project:dataset.table
    # but this library wants the format project.dataset.table
    fixed_source_table = source_table.replace(':', '.')

    query = 'SELECT DISTINCT(source) AS source FROM `{table}`'.format(
        table=fixed_source_table)
    rows = client.query(query)
    sources = [row.source for row in rows]

    return sources

  @staticmethod
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

  def data_to_load(self,
                   gcs: GCSFileSystem,
                   scan_type: str,
                   incremental_load: bool,
                   env: str,
                   start_date: Optional[datetime.date] = None,
                   end_date: Optional[datetime.date] = None) -> List[str]:
    """Select the right files to read.

    Args:
      gcs: GCSFileSystem object
      scan_type: one of 'echo', 'discard', 'http', 'https'
      incremental_load: boolean. If true, only read the latest new data
      env: one of 'prod' or 'dev'.
      start_date: date object, only files after or at this date will be read
      end_date: date object, only files at or before this date will be read

    Returns:
      A List of filename strings. ex
       ['gs://firehook-scans/echo/CP_Quack-echo-2020-08-22-06-08-03/results.json',
        'gs://firehook-scans/echo/CP_Quack-echo-2020-08-23-06-01-02/results.json']
    """
    if incremental_load:
      existing_sources = self.get_existing_datasources(scan_type, env)
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
        if (self.between_dates(filename, start_date, end_date) and
            self.source_from_filename(filename) not in existing_sources and
            file_size != 0)
    ]
    return filtered_filenames

  @classmethod
  def read_scan_text(
      cls, p: beam.Pipeline,
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
          lines | step_name >> beam.Map(
              cls.make_tuple, filename).with_output_types(Tuple[str, str]))

      line_pcollections_per_file.append(lines_with_filenames)

    # PCollection[Tuple[filename,line]]
    lines = (
        tuple(line_pcollections_per_file)
        | 'flatten lines' >> beam.Flatten(pipeline=p).with_output_types(
            Tuple[str, str]))

    return lines

  @staticmethod
  def make_tuple(line: str, filename: str) -> Tuple[str, str]:
    """Helper method for making a tuple from two args."""
    return (filename, line)

  @staticmethod
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

  @classmethod
  def flatten_measurement(cls, filename: str, line: str) -> Iterator[Row]:
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
          'source': cls.source_from_filename(filename),
      }
      yield row

  @classmethod
  def add_metadata(
      cls, rows: beam.pvalue.PCollection[Row]) -> beam.pvalue.PCollection[Row]:
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
        | 'key by ips and dates' >> beam.Map(lambda row: (cls.make_date_ip_key(
            row), row)).with_output_types(Tuple[DateIpKey, Row]))

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
            cls.add_ip_metadata).with_output_types(Tuple[DateIpKey, Row]))

    # PCollection[Tuple[Tuple[date,ip],Dict[input_name_key,List[Row]]]]
    grouped_metadata_and_rows = (({
        IP_METADATA_PCOLLECTION_NAME: ips_with_metadata,
        ROWS_PCOLLECION_NAME: rows_keyed_by_ip_and_date
    }) | 'group by keys' >> beam.CoGroupByKey())

    # PCollection[Row]
    rows_with_metadata = (
        grouped_metadata_and_rows
        | 'merge metadata with rows' >> beam.FlatMapTuple(
            cls.merge_metadata_with_rows).with_output_types(Row))

    return rows_with_metadata

  @staticmethod
  def make_date_ip_key(row: Row) -> DateIpKey:
    """Makes a tuple key of the date and ip from a given row dict."""
    return (row['date'], row['ip'])

  @staticmethod
  def add_ip_metadata(date: str,
                      ips: List[str]) -> Iterator[Tuple[DateIpKey, Row]]:
    """Add Autonymous System metadata for ips in the given rows.

    Args:
      date: a 'YYYY-MM-DD' date key
      ips: a list of ips

    Yields:
      Tuples (DateIpKey, metadata_dict)
      where metadata_dict is a row Dict[column_name, values]
    """
    # This class needs to be imported here
    # since this function will be called on remote workers.
    try:
      # On workers we import relative to setup.py
      from metadata.ip_metadata import IpMetadata

    except ImportError:
      # For tests we import relative to the top-level test call
      # and we import a fake
      logging.warning('Using a fake IpMetadata for testing')
      from pipeline.metadata.fake_ip_metadata import FakeIpMetadata as IpMetadata

    ip_metadata = IpMetadata(
        datetime.date.fromisoformat(date), allow_previous_day=True)

    for ip in ips:
      metadata_key = (date, ip)

      try:
        (netblock, asn, as_name, as_full_name, as_type,
         country) = ip_metadata.lookup(ip)
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

  @staticmethod
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

  def write_to_bigquery(self, rows: beam.pvalue.PCollection[Row],
                        scan_type: str, incremental_load: bool, env: str):
    """Write out row to a bigquery table.

    Args:
      rows: PCollection[Row] of data to write.
      scan_type: one of 'echo', 'discard', 'http', 'https'
      incremental_load: boolean. If true, only load the latest new data, if
        false reload all data.
      env: one of 'prod' or 'dev. Determines which tables to write to.

    Raises:
      Exception: if any arguments are invalid.
    """
    full_table_name = self.get_table_name(scan_type, env)

    if incremental_load:
      write_mode = beam.io.BigQueryDisposition.WRITE_APPEND
    else:
      write_mode = beam.io.BigQueryDisposition.WRITE_TRUNCATE

    (rows | 'Write' >> beam.io.WriteToBigQuery(
        full_table_name,
        schema=self.get_bigquery_schema(self.schema),
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=write_mode))

  @staticmethod
  def get_job_name(scan_type: str, incremental_load: bool, env: str) -> str:
    """Creates the job name for the beam pipeline.

    Pipelines with the same name cannot run simultaneously.

    Args:
      scan_type: one of 'echo', 'discard', 'http', 'https'
      incremental_load: boolean. whether the job is incremental.
      env: one of 'prod' or 'dev.

    Returns:
      A string like 'echo-flatten-add-metadata-prod-incremental'
    """
    job_name = scan_type + '-flatten-add-metadata-' + env
    if incremental_load:
      job_name = job_name + '-incremental'
    return job_name

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

  def run_beam_pipeline(self, scan_type: str, incremental_load: bool, env: str,
                        start_date: Optional[datetime.date],
                        end_date: Optional[datetime.date]):
    """Run an apache beam pipeline to load json data into bigquery.

    Args:
      scan_type: one of 'echo', 'discard', 'http', 'https'
      incremental_load: boolean. If true, only load the latest new data, if
        false reload all data.
      env: one of 'prod' or 'dev. Determines which tables to write to.
      start_date: date object, only files after or at this date will be read.
        Mostly only used during development.
      end_date: date object, only files at or before this date will be read.
        Mostly only used during development.

    Raises:
      Exception: if any arguments are invalid or the pipeline fails.
    """
    logging.getLogger().setLevel(logging.INFO)
    job_name = self.get_job_name(scan_type, incremental_load, env)
    pipeline_options = self.get_pipeline_options(scan_type, job_name)
    gcs = GCSFileSystem(pipeline_options)

    new_filenames = self.data_to_load(gcs, scan_type, incremental_load, env,
                                      start_date, end_date)
    if not new_filenames:
      logging.info('No new files to load incrementally')
      return

    with beam.Pipeline(options=pipeline_options) as p:
      # PCollection[Tuple[filename,line]]
      lines = self.read_scan_text(p, new_filenames)

      # PCollection[Row]
      rows = (
          lines | 'flatten json' >> beam.FlatMapTuple(
              self.flatten_measurement).with_output_types(Row))

      # PCollection[Row]
      rows_with_metadata = self.add_metadata(rows)

      self.write_to_bigquery(rows_with_metadata, scan_type, incremental_load,
                             env)

  def run_all_scan_types(self,
                         incremental_load: bool,
                         env: str,
                         start_date: Optional[datetime.date] = None,
                         end_date: Optional[datetime.date] = None):
    """Runs the beam pipelines for all scan types in parallel.

    Args:
      incremental_load: boolean. If true, only load the latest new data, if
        false reload all data.
      env: one of 'prod' or 'dev. Determines which tables to write to.
      start_date: date object, only files after or at this date will be read.
        Mostly only used during development.
      end_date: date object, only files at or before this date will be read.
        Mostly only used during development.

    Returns:
      True on success

    Raises:
      Exception: if any of the pipelines fail or don't finish.
    """
    scan_types = SCAN_TYPES_TO_ZONES.keys()

    with concurrent.futures.ThreadPoolExecutor() as pool:
      futures = []
      for scan_type in scan_types:
        future = pool.submit(self.run_beam_pipeline, scan_type,
                             incremental_load, env, start_date, end_date)
        futures.append(future)

      finished, pending = concurrent.futures.wait(
          futures, return_when=concurrent.futures.FIRST_EXCEPTION)

      exceptions = [
          future.exception() for future in finished if future.exception()
      ]
      if exceptions:
        # If there were any exceptions just raise the first one.
        raise exceptions[0]

      if pending:
        raise Exception('Some pipelines failed to finish: ', pending,
                        'finished: ', finished)
      return True

  def run_dev(self, scan_type: str, incremental_load: bool):
    """Run a dev pipeline for testing.

    Dev only needs to load a week of data of one type for testing.
    For incremental loads use the last week of data.
    for full loads use data from two weeks ago.
    (This is to make it easier to test the full and then incremental loads
    together when developing.)

    Args:
      scan_type: one of 'echo', 'discard', 'http', 'https' or 'all'
      incremental_load: boolean. If true, only load the latest new data.
    """
    if incremental_load:
      end_day = datetime.date.today()
    else:
      end_day = datetime.date.today() - datetime.timedelta(days=7)
    start_day = end_day - datetime.timedelta(days=7)

    if scan_type == 'all':
      self.run_all_scan_types(incremental_load, 'dev', start_day, end_day)
    else:
      self.run_beam_pipeline(scan_type, incremental_load, 'dev', start_day,
                             end_day)


def get_firehook_beam_pipeline_runner():
  """Factory function to get a beam pipeline class with firehook values."""

  return ScanDataBeamPipelineRunner(CLOUD_PROJECT, SCAN_TABLE_NAME,
                                    DATASET_SUFFIX, SCAN_BIGQUERY_SCHEMA,
                                    INPUT_BUCKET, BEAM_STAGING_LOCATION,
                                    BEAM_TEMP_LOCATION)


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Run a beam pipeline over scans')
  parser.add_argument(
      '--full',
      action='store_true',
      default=False,
      help='Run over all files and not just the latest (rebuilds tables)')
  parser.add_argument(
      '--env',
      type=str,
      default='dev',
      choices=['dev', 'prod'],
      help='Whether to run over prod or dev data')
  parser.add_argument(
      '--scan_type',
      type=str,
      default='echo',
      choices=['all'] + list(SCAN_TYPES_TO_ZONES.keys()),
      help='Which type of scan to run over')
  args = parser.parse_args()

  incremental = not args.full

  runner = get_firehook_beam_pipeline_runner()

  if args.env == 'dev':
    runner.run_dev(args.scan_type, incremental)
  elif args.env == 'prod':
    runner.run_all_scan_types(incremental, 'prod')
