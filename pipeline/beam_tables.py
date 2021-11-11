# Copyright 2020 Jigsaw Operations LLC
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
import logging
import re
from typing import Optional, Tuple, Dict, List, Any, Iterator, Iterable

import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery as beam_bigquery
from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from google.cloud import bigquery as cloud_bigquery  # type: ignore

from pipeline.metadata.beam_metadata import DateIpKey, IP_METADATA_PCOLLECTION_NAME, ROWS_PCOLLECION_NAME, make_date_ip_key, merge_metadata_with_rows
from pipeline.metadata.flatten import Row
from pipeline.metadata import flatten
from pipeline.metadata import satellite
from pipeline.metadata.ip_metadata_chooser import IpMetadataChooserFactory

# Tables have names like 'echo_scan' and 'http_scan
BASE_TABLE_NAME = 'scan'
# Prod data goes in the `firehook-censoredplanet:base' dataset
PROD_DATASET_NAME = 'base'

# key: (type, mode)
SCAN_BIGQUERY_SCHEMA = {
    # Columns from Censored Planet data
    'domain': ('string', 'nullable'),
    'category': ('string', 'nullable'),
    'ip': ('string', 'nullable'),
    'date': ('date', 'nullable'),
    'start_time': ('timestamp', 'nullable'),
    'end_time': ('timestamp', 'nullable'),
    'error': ('string', 'nullable'),
    'anomaly': ('boolean', 'nullable'),
    'success': ('boolean', 'nullable'),
    'stateful_block': ('boolean', 'nullable'),
    'is_control': ('boolean', 'nullable'),
    'controls_failed': ('boolean', 'nullable'),
    'measurement_id': ('string', 'nullable'),
    'source': ('string', 'nullable'),
    'blockpage': ('boolean', 'nullable'),
    'page_signature': ('string', 'nullable'),

    # Column filled in all tables
    'received_status': ('string', 'nullable'),
    # Columns filled only in HTTP/HTTPS tables
    'received_body': ('string', 'nullable'),
    'received_headers': ('string', 'repeated'),
    # Columns filled only in HTTPS tables
    'received_tls_version': ('integer', 'nullable'),
    'received_tls_cipher_suite': ('integer', 'nullable'),
    'received_tls_cert': ('string', 'nullable'),

    # Columns added from CAIDA data
    'netblock': ('string', 'nullable'),
    'asn': ('integer', 'nullable'),
    'as_name': ('string', 'nullable'),
    'as_full_name': ('string', 'nullable'),
    'as_class': ('string', 'nullable'),
    'country': ('string', 'nullable'),
    # Columns from DBIP
    'organization': ('string', 'nullable'),
}
# Future fields
"""
    'as_traffic': ('integer', 'nullable'),
"""

# Mapping of each scan type to the zone to run its pipeline in.
# This adds more parallelization when running all pipelines.
SCAN_TYPES_TO_ZONES = {
    'https': 'us-east1',  # https has the most data, so it gets the best zone.
    'http': 'us-east4',
    'echo': 'us-west1',
    'discard': 'us-west2',
    'satellite': 'us-central1'  # us-central1 has 160 shuffle slots by default
}

ALL_SCAN_TYPES = SCAN_TYPES_TO_ZONES.keys()

# Data files for the non-Satellite pipelines
SCAN_FILES = ['results.json']


def _get_bigquery_schema(scan_type: str) -> Dict[str, Any]:
  """Get the appropriate schema for the given scan type.

  Args:
    scan_type: str, one of 'echo', 'discard', 'http', 'https',
      'satellite' or 'blockpage'

  Returns:
    A nested Dict with bigquery fields like SCAN_BIGQUERY_SCHEMA.
  """
  if scan_type == satellite.SCAN_TYPE_BLOCKPAGE:
    return satellite.BLOCKPAGE_BIGQUERY_SCHEMA
  if scan_type == satellite.SCAN_TYPE_SATELLITE:
    full_schema: Dict[str, Any] = {}
    full_schema.update(SCAN_BIGQUERY_SCHEMA)
    full_schema.update(satellite.SATELLITE_BIGQUERY_SCHEMA)
    return full_schema
  return SCAN_BIGQUERY_SCHEMA


def _get_beam_bigquery_schema(
    fields: Dict[str, Any]) -> beam_bigquery.TableSchema:
  """Return a beam bigquery schema for the output table.

  Args:
    fields: dict of {'field_name': ['column_type', 'column_mode']}

  Returns:
    A bigquery table schema
  """
  table_schema = beam_bigquery.TableSchema()

  for (name, attributes) in fields.items():
    field_type = attributes[0]
    mode = attributes[1]
    field_schema = beam_bigquery.TableFieldSchema()
    field_schema.name = name
    field_schema.type = field_type
    field_schema.mode = mode
    if len(attributes) > 2:
      field_schema.fields = []
      for (subname, (subtype, submode)) in attributes[2].items():
        subfield_schema = beam_bigquery.TableFieldSchema()
        subfield_schema.name = subname
        subfield_schema.type = subtype
        subfield_schema.mode = submode
        field_schema.fields.append(subfield_schema)
    table_schema.fields.append(field_schema)
  return table_schema


def _get_existing_datasources(table_name: str) -> List[str]:
  """Given a table return all sources that contributed to the table.

  Args:
    table_name: name of a bigquery table like
      'firehook-censoredplanet:echo_results.scan_test'

  Returns:
    List of data sources. ex ['CP_Quack-echo-2020-08-23-06-01-02']
  """
  # This needs to be created locally
  # because bigquery client objects are unpickleable.
  # So passing in a client to the class breaks the pickling beam uses
  # to send state to remote machines.
  client = cloud_bigquery.Client()

  # Bigquery table names are of the format project:dataset.table
  # but this library wants the format project.dataset.table
  fixed_table_name = table_name.replace(':', '.')

  query = f'SELECT DISTINCT(source) AS source FROM `{fixed_table_name}`'
  rows = client.query(query)
  sources = [row.source for row in rows]
  return sources


def _make_tuple(line: str, filename: str) -> Tuple[str, str]:
  """Helper method for making a tuple from two args."""
  return (filename, line)


def _read_scan_text(
    p: beam.Pipeline,
    filenames: List[str]) -> beam.pvalue.PCollection[Tuple[str, str]]:
  """Read in all individual lines for the given data sources.

  Args:
    p: beam pipeline object
    filenames: List of files to read from

  Returns:
    A PCollection[Tuple[filename, line]] of all the lines in the files keyed
    by filename
  """
  # PCollection[filename]
  pfilenames = (p | beam.Create(filenames))

  # PCollection[Tuple(filename, line)]
  lines = (
      pfilenames | "read files" >> beam.io.ReadAllFromText(with_filename=True))

  return lines


def _between_dates(filename: str,
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
  if start_date:
    return start_date <= date
  if end_date:
    return date <= end_date
  return True


def _get_partition_params(scan_type: str = None) -> Dict[str, Any]:
  """Returns additional partitioning params to pass with the bigquery load.

  Args:
    scan_type: data type, one of 'echo', 'discard', 'http', 'https',
      'satellite' or 'blockpage'

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
  if scan_type == satellite.SCAN_TYPE_BLOCKPAGE:
    partition_params.pop('clustering')
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
  return 'write-' + fixed_table_name


def get_table_name(dataset_name: str, scan_type: str,
                   base_table_name: str) -> str:
  """Construct a bigquery table name.

  Args:
    dataset_name: dataset name like 'base' or 'laplante'
    scan_type: data type, one of 'echo', 'discard', 'http', 'https'
    base_table_name: table name like 'scan'

  Returns:
    a dataset.table name like 'base.echo_scan'
  """
  return f'{dataset_name}.{scan_type}_{base_table_name}'


def _raise_exception_if_zero(num: int) -> None:
  if num == 0:
    raise Exception("Zero rows were created even though there were new files.")


def _raise_error_if_collection_empty(
    rows: beam.pvalue.PCollection[Row]) -> beam.pvalue.PCollection:
  count_collection = (
      rows | "Count" >> beam.combiners.Count.Globally() |
      "Error if empty" >> beam.Map(_raise_exception_if_zero))
  return count_collection


class ScanDataBeamPipelineRunner():
  """A runner to collect cloud values and run a corrosponding beam pipeline."""

  def __init__(self, project: str, bucket: str, staging_location: str,
               temp_location: str,
               metadata_chooser_factory: IpMetadataChooserFactory) -> None:
    """Initialize a pipeline runner.

    Args:
      project: google cluod project name
      bucket: gcs bucket name
      staging_location: gcs bucket name, used for staging beam data
      temp_location: gcs bucket name, used for temp beam data
      metadata_chooser: factory to create a metadata chooser
    """
    self.project = project
    self.bucket = bucket
    self.staging_location = staging_location
    self.temp_location = temp_location
    self.metadata_chooser_factory = metadata_chooser_factory

  def _get_full_table_name(self, table_name: str) -> str:
    """Get a full project:dataset.table name.

    Args:
      table_name: a dataset.table name

    Returns:
      project:dataset.table name
    """
    return self.project + ':' + table_name

  def _data_to_load(self,
                    gcs: GCSFileSystem,
                    scan_type: str,
                    incremental_load: bool,
                    table_name: str,
                    start_date: Optional[datetime.date] = None,
                    end_date: Optional[datetime.date] = None) -> List[str]:
    """Select the right files to read.

    Args:
      gcs: GCSFileSystem object
      scan_type: one of 'echo', 'discard', 'http', 'https', 'satellite'
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
      full_table_name = self._get_full_table_name(table_name)
      existing_sources = _get_existing_datasources(full_table_name)
    else:
      existing_sources = []

    if scan_type == satellite.SCAN_TYPE_SATELLITE:
      files_to_load = satellite.SATELLITE_FILES
    else:
      files_to_load = SCAN_FILES

    # Both zipped and unzipped data to be read in
    zipped_regex = self.bucket + scan_type + '/**/{0}.gz'
    unzipped_regex = self.bucket + scan_type + '/**/{0}'

    file_metadata = []
    for file in files_to_load:
      zipped_metadata = [
          m.metadata_list for m in gcs.match([zipped_regex.format(file)])
      ][0]
      unzipped_metadata = [
          m.metadata_list for m in gcs.match([unzipped_regex.format(file)])
      ][0]
      file_metadata += zipped_metadata + unzipped_metadata

    filenames = [metadata.path for metadata in file_metadata]
    file_sizes = [metadata.size_in_bytes for metadata in file_metadata]

    filtered_filenames = [
        filename for (filename, file_size) in zip(filenames, file_sizes)
        if (_between_dates(filename, start_date, end_date) and
            flatten.source_from_filename(filename) not in existing_sources and
            file_size != 0)
    ]
    return filtered_filenames

  def _add_metadata(
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
        rows | 'key by ips and dates' >>
        beam.Map(lambda row: (make_date_ip_key(row), row)).with_output_types(
            Tuple[DateIpKey, Row]))

    # PCollection[DateIpKey]
    # pylint: disable=no-value-for-parameter
    ips_and_dates = (
        rows_keyed_by_ip_and_date | 'get ip and date keys per row' >>
        beam.Keys().with_output_types(DateIpKey))

    # PCollection[DateIpKey]
    deduped_ips_and_dates = (
        # pylint: disable=no-value-for-parameter
        ips_and_dates | 'dedup' >> beam.Distinct().with_output_types(DateIpKey))

    # PCollection[Tuple[date,List[ip]]]
    grouped_ips_by_dates = (
        deduped_ips_and_dates | 'group by date' >>
        beam.GroupByKey().with_output_types(Tuple[str, Iterable[str]]))

    # PCollection[Tuple[DateIpKey,Row]]
    ips_with_metadata = (
        grouped_ips_by_dates | 'get ip metadata' >> beam.FlatMapTuple(
            self._add_ip_metadata).with_output_types(Tuple[DateIpKey, Row]))

    # PCollection[Tuple[Tuple[date,ip],Dict[input_name_key,List[Row]]]]
    grouped_metadata_and_rows = (({
        IP_METADATA_PCOLLECTION_NAME: ips_with_metadata,
        ROWS_PCOLLECION_NAME: rows_keyed_by_ip_and_date
    }) | 'group by keys' >> beam.CoGroupByKey())

    # PCollection[Row]
    rows_with_metadata = (
        grouped_metadata_and_rows | 'merge metadata with rows' >>
        beam.FlatMapTuple(merge_metadata_with_rows).with_output_types(Row))

    return rows_with_metadata

  def _add_ip_metadata(self, date: str,
                       ips: List[str]) -> Iterator[Tuple[DateIpKey, Row]]:
    """Add Autonymous System metadata for ips in the given rows.

    Args:
      date: a 'YYYY-MM-DD' date key
      ips: a list of ips

    Yields:
      Tuples (DateIpKey, metadata_dict)
      where metadata_dict is a row Dict[column_name, values]
    """
    ip_metadata_chooser = self.metadata_chooser_factory.make_chooser(
        datetime.date.fromisoformat(date))

    for ip in ips:
      metadata_key = (date, ip)
      metadata_values = ip_metadata_chooser.get_metadata(ip)

      yield (metadata_key, metadata_values)

  def _write_to_bigquery(self, scan_type: str,
                         rows: beam.pvalue.PCollection[Row], table_name: str,
                         incremental_load: bool) -> None:
    """Write out row to a bigquery table.

    Args:
      scan_type: one of 'echo', 'discard', 'http', 'https',
        'satellite' or 'blockpage'
      rows: PCollection[Row] of data to write.
      table_name: dataset.table name like 'base.echo_scan' Determines which
        tables to write to.
      incremental_load: boolean. If true, only load the latest new data, if
        false reload all data.

    Raises:
      Exception: if any arguments are invalid.
    """
    schema = _get_beam_bigquery_schema(_get_bigquery_schema(scan_type))

    if incremental_load:
      write_mode = beam.io.BigQueryDisposition.WRITE_APPEND
    else:
      write_mode = beam.io.BigQueryDisposition.WRITE_TRUNCATE

    (rows | f'Write {scan_type}' >> beam.io.WriteToBigQuery(  # pylint: disable=expression-not-assigned
        self._get_full_table_name(table_name),
        schema=schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=write_mode,
        additional_bq_parameters=_get_partition_params(scan_type)))

  def _get_pipeline_options(self, scan_type: str,
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
        experiments=[
            'enable_execution_details_collection',
            'use_monitoring_state_manager', 'upload_graph'
        ],
        setup_file='./pipeline/setup.py')
    pipeline_options.view_as(SetupOptions).save_main_session = True
    return pipeline_options

  def run_beam_pipeline(self, scan_type: str, incremental_load: bool,
                        job_name: str, table_name: str,
                        start_date: Optional[datetime.date],
                        end_date: Optional[datetime.date]) -> None:
    """Run a single apache beam pipeline to load json data into bigquery.

    Args:
      scan_type: one of 'echo', 'discard', 'http', 'https' or 'satellite'
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
    pipeline_options = self._get_pipeline_options(scan_type, job_name)
    gcs = GCSFileSystem(pipeline_options)

    new_filenames = self._data_to_load(gcs, scan_type, incremental_load,
                                       table_name, start_date, end_date)
    if not new_filenames:
      logging.info('No new files to load')
      return

    with beam.Pipeline(options=pipeline_options) as p:
      # PCollection[Tuple[filename,line]]
      lines = _read_scan_text(p, new_filenames)

      if scan_type == satellite.SCAN_TYPE_SATELLITE:
        # PCollection[Row], PCollection[Row]
        satellite_rows, blockpage_rows = satellite.process_satellite_lines(
            lines)

        # PCollection[Row]
        rows_with_metadata = self._add_metadata(satellite_rows)

        self._write_to_bigquery(
            satellite.SCAN_TYPE_BLOCKPAGE, blockpage_rows,
            satellite.get_blockpage_table_name(table_name, scan_type),
            incremental_load)
      else:  # Hyperquack scans
        # PCollection[Row]
        rows = (
            lines | 'flatten json' >> beam.ParDo(
                flatten.FlattenMeasurement()).with_output_types(Row))

        # PCollection[Row]
        rows_with_metadata = self._add_metadata(rows)

      _raise_error_if_collection_empty(rows_with_metadata)

      self._write_to_bigquery(scan_type, rows_with_metadata, table_name,
                              incremental_load)
