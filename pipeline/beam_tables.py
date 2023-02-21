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
import pathlib
import re
import json
from typing import Any, Callable, Dict, List, Optional, Tuple

import apache_beam as beam
from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem
from apache_beam.io.fileio import WriteToFiles
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from google.cloud import bigquery as cloud_bigquery  # type: ignore

from pipeline.metadata.schema import BigqueryRow, dict_to_gcs_json_string
from pipeline.metadata import hyperquack
from pipeline.metadata import schema
from pipeline.metadata import flatten_base
from pipeline.metadata import satellite
from pipeline.metadata.add_metadata import MetadataAdder
from pipeline.metadata.ip_metadata_chooser import IpMetadataChooserFactory
from pipeline.metadata import sink

# Tables have names like 'echo_scan' and 'http_scan
BASE_TABLE_NAME = 'scan'
# Prod and dev data goes in the `firehook-censoredplanet:base' dataset
BASE_DATASET_NAME = 'base'

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

# An empty json file is 0 bytes when unzipped, but 33 bytes when zipped
EMPTY_GZIPPED_FILE_SIZE = 33

# We don't include data before this data in satellite
# since it's not very accurate and causes scaling problems
DONT_READ_SATELLITE_DATA_BEFORE = datetime.date(2022, 5, 1)


def _get_existing_bq_datasources(table_name: str, project: str) -> List[str]:
  """Given a BigQuery table return all sources that contributed to the table.
  Args:
    table_name: name of a bigquery table like
      'firehook-censoredplanet:echo_results.scan_test'
    project: project to use for query like 'firehook-censoredplanet'
  Returns:
    List of data sources. ex ['CP_Quack-echo-2020-08-23-06-01-02']
  """
  # This needs to be created locally
  # because bigquery client objects are unpickleable.
  # So passing in a client to the class breaks the pickling beam uses
  # to send state to remote machines.
  client = cloud_bigquery.Client(project=project)

  # Bigquery table names are of the format project:dataset.table
  # but this library wants the format project.dataset.table
  fixed_table_name = table_name.replace(':', '.')

  query = f'SELECT DISTINCT(source) AS source FROM `{fixed_table_name}`'
  rows = client.query(query)
  sources = [row.source for row in rows]
  return sources


def _get_existing_gcs_datasources(gcs_folder: str, project: str) -> List[str]:
  """Given a GCS folder return all contributing sources.

  Args:
    gcs_folder: name of a GCS folder like 'gs://firehook-test/avirkud'
    project: project to use for query like 'firehook-censoredplanet'

  Returns:
    List of data sources. ex ['CP_Quack-echo-2020-08-23-06-01-02']
  """
  # Import storage here to avoid errors on Dataflow workers.
  # pylint: disable=import-outside-toplevel
  from google.cloud import storage  # type: ignore
  # This needs to be created locally
  # because gcs client objects are unpickleable.
  # So passing in a client to the class breaks the pickling beam uses
  # to send state to remote machines.
  client = storage.Client(project=project)

  # The first path component after gs:// is the bucket, the rest is the subdir.
  path_split = gcs_folder[5:].split('/', maxsplit=1)
  bucket_name = path_split[0]
  folder = path_split[1] if len(path_split) > 1 else None

  # We expect filenames in format gs://bucket/.../source={source}/.../file
  bucket = client.get_bucket(bucket_name)
  matches = [
      re.findall(r'source=[^/]*', file.name)
      for file in client.list_blobs(bucket, prefix=folder)
  ]
  sources = [match[0][7:] for match in matches if match]
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


def _filename_matches(filepath: str, allowed_filenames: List[str]) -> bool:
  """Find if a filepath matches a list of filenames.

  Args:
    filepath, zipped or unzipped ex: path/results.js, path/results.json.gz
    allowed_filenames: List of filenames, all unzipped
      ex: [resolvers.json, blockpages.json]

  Returns:
    boolean, whether the filepath matches one of the list.
    Zipped matches to unzipped names count.
  """
  filename = pathlib.PurePosixPath(filepath).name

  if '.gz' in pathlib.PurePosixPath(filename).suffixes:
    filename = pathlib.PurePosixPath(filename).stem

  return filename in allowed_filenames


def _get_partition_params(scan_type: str) -> Dict[str, Any]:
  """Returns additional partitioning params to pass with the bigquery load.

  Args:
    scan_type: data type, one of 'echo', 'discard', 'http', 'https',
      or 'satellite'

  Returns: A dict of query params, See:
  https://beam.apache.org/releases/pydoc/2.14.0/apache_beam.io.gcp.bigquery.html#additional-parameters-for-bigquery-tables
  https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#resource:-table
  """
  partition_params: Dict[str, Any] = {
      'timePartitioning': {
          'type': 'DAY',
          'field': 'date'
      },
  }
  if scan_type == 'satellite':
    partition_params['clustering'] = {
        'fields': ['resolver_country', 'resolver_asn']
    }
  else:
    partition_params['clustering'] = {
        'fields': ['server_country', 'server_asn']
    }
  return partition_params


def get_bq_job_name(table_name: str, incremental_load: bool) -> str:
  """Creates the job name for the beam pipeline (output to Bigquery).

  Pipelines with the same name cannot run simultaneously.

  Args:
    table_name: a dataset.table name like 'base.scan_echo'
    incremental_load: boolean. whether the job is incremental.

  Returns:
    A string like 'write-base-scan-echo'
  """
  # no underscores or periods are allowed in beam job names
  fixed_job_name = table_name.replace('_', '-').replace('.', '-')
  if incremental_load:
    return 'append-' + fixed_job_name
  return 'write-' + fixed_job_name


def get_gcs_job_name(gcs_folder: str, incremental_load: bool) -> str:
  """Creates the job name for the beam pipeline (output to GCS).

  Pipelines with the same name cannot run simultaneously.

  Args:
    gcs_folder: a gs://bucket/folder/... name like 'gs://firehook-test/scans/echo'
    incremental_load: boolean. whether the job is incremental.

  Returns:
    A string like 'write-gs-firehook-test-scans-echo'
  """
  # no underscores, colons, slashes, or periods are allowed in beam job names
  fixed_job_name = gcs_folder.replace('_', '-').replace('.', '-')
  fixed_job_name = fixed_job_name.replace('://', '-').replace('/', '-')
  if incremental_load:
    return 'append-' + fixed_job_name
  return 'write-' + fixed_job_name


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


def get_gcs_folder(dataset_name: str, scan_type: str, bucket_name: str) -> str:
  """Construct a gcs folder name.

  Args:
    dataset_name: dataset name like 'base' or 'laplante'
    scan_type: data type, one of 'echo', 'discard', 'http', 'https', 'satellite'
    bucket_name: bucket name like 'firehook-test'

  Returns:
    a GCS folder like 'gs://firehook-test/avirkud/echo'
  """
  return f'gs://{bucket_name}/{dataset_name}/{scan_type}'


def _raise_exception_if_zero(num: int) -> None:
  if num == 0:
    raise Exception("Zero rows were created even though there were new files.")


def _raise_error_if_collection_empty(
    rows: beam.pvalue.PCollection[BigqueryRow]) -> beam.pvalue.PCollection:
  count_collection = (
      rows | "Count" >> beam.combiners.Count.Globally() |
      "Error if empty" >> beam.Map(_raise_exception_if_zero))
  return count_collection


def _get_destination(record: str) -> str:
  """Returns the hive-format dest folder for a measurement record str."""
  record_dict = json.loads(record)
  if 'server_country' in record_dict:
    return f'source={record_dict["source"]}/country={record_dict["server_country"]}/results'
  return f'source={record_dict["source"]}/country={record_dict["resolver_country"]}/results'


def _custom_file_naming(suffix: Optional[str] = None) -> Callable:
  """Returns custom function to name destination files."""

  # Beam requires the returned function to have the following arguments,
  # see beam.io.filename.destination_prefix_naming.
  def _inner(window: Any, pane: Any, shard_index: Optional[int],
             total_shards: Optional[int], compression: Optional[str],
             destination: Optional[str]) -> str:
    # Get the filename with the destination_prefix_naming format:
    # '{prefix}-{start}-{end}-{pane}-{shard:05d}-of-{total_shards:05d}{suffix}{compression}'
    # where prefix = destination.
    filename = beam.io.fileio.destination_prefix_naming(suffix)(window, pane,
                                                                shard_index,
                                                                total_shards,
                                                                compression,
                                                                destination)
    # Remove shard component from filename if there is only one shard.
    filename = filename.replace('-00000-of-00001', '')
    return filename

  return _inner


class ScanDataBeamPipelineRunner():
  """A runner to collect cloud values and run a corrosponding beam pipeline."""

  def __init__(self, project: str, input_bucket: str, staging_location: str,
               temp_location: str, output_bucket: str,
               metadata_chooser_factory: IpMetadataChooserFactory) -> None:
    """Initialize a pipeline runner.

    Args:
      project: google cluod project name
      input_bucket: gcs bucket name
      staging_location: gcs bucket name, used for staging beam data
      temp_location: gcs bucket name, used for temp beam data
      output_bucket: gcs bucket name, used when writing to GCS (instead of bq)
      metadata_chooser: factory to create a metadata chooser
    """
    self.project = project
    self.input_bucket = input_bucket
    self.staging_location = staging_location
    self.temp_location = temp_location
    self.output_bucket = output_bucket
    self.metadata_adder = MetadataAdder(metadata_chooser_factory)

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
                    table_name: Optional[str] = None,
                    gcs_folder: Optional[str] = None,
                    start_date: Optional[datetime.date] = None,
                    end_date: Optional[datetime.date] = None) -> List[str]:
    """Select the right files to read.

    Args:
      gcs: GCSFileSystem object
      scan_type: one of 'echo', 'discard', 'http', 'https', 'satellite'
      incremental_load: boolean. If true, only read the latest new data
      table_name: dataset.table name like 'base.scan_echo'
      gcs_folder: GCS folder gs://bucket/folder/... like 'gs://firehook-test/avirkud'
      start_date: date object, only files after or at this date will be read
      end_date: date object, only files at or before this date will be read

    Returns:
      A List of filename strings. ex
       ['gs://firehook-scans/echo/CP_Quack-echo-2020-08-22-06-08-03/results.json',
        'gs://firehook-scans/echo/CP_Quack-echo-2020-08-23-06-01-02/results.json']
    """
    if incremental_load:
      if table_name and gcs_folder:
        full_table_name = self._get_full_table_name(table_name)
        table_existing_sources = _get_existing_bq_datasources(full_table_name,
                                                        self.project)
        gcs_existing_sources = _get_existing_gcs_datasources(
            gcs_folder, self.project)
        table_set = set(table_existing_sources)
        gcs_set = set(gcs_existing_sources)
        if table_set != gcs_set:
          raise Exception('Files in the dataset table and the GCS folder differ, aborting')
        existing_sources = table_existing_sources
      elif table_name:
        full_table_name = self._get_full_table_name(table_name)
        existing_sources = _get_existing_bq_datasources(full_table_name,
                                                        self.project)
      elif gcs_folder:
        existing_sources = _get_existing_gcs_datasources(
            gcs_folder, self.project)
      else:
        raise Exception('Either table_name or gcs_folder argument is required.')
    else:
      existing_sources = []

    if scan_type == schema.SCAN_TYPE_SATELLITE:
      files_to_load = satellite.SATELLITE_FILES
    else:
      files_to_load = SCAN_FILES

    # Filepath like `gs://firehook-scans/echo/**/*'
    files_regex = f'{self.input_bucket}{scan_type}/**/*'
    file_metadata = [m.metadata_list for m in gcs.match([files_regex])][0]

    filepaths = [metadata.path for metadata in file_metadata]
    file_sizes = [metadata.size_in_bytes for metadata in file_metadata]

    if scan_type == 'satellite':
      if start_date is None or start_date < DONT_READ_SATELLITE_DATA_BEFORE:
        start_date = DONT_READ_SATELLITE_DATA_BEFORE

    filtered_filenames = [
        filepath for (filepath, file_size) in zip(filepaths, file_sizes)
        if (_between_dates(filepath, start_date, end_date) and
            _filename_matches(filepath, files_to_load) and
            flatten_base.source_from_filename(filepath) not in existing_sources
            and file_size > EMPTY_GZIPPED_FILE_SIZE)
    ]

    return filtered_filenames

  def _write_to_bigquery(self, scan_type: str,
                         rows: beam.pvalue.PCollection[BigqueryRow],
                         table_name: str, incremental_load: bool) -> None:
    """Write out row to a bigquery table.

    Args:
      scan_type: one of 'echo', 'discard', 'http', 'https',
        or 'satellite'
      rows: PCollection[BigqueryRow] of data to write.
      table_name: dataset.table name like 'base.echo_scan' Determines which
        tables to write to.
      incremental_load: boolean. If true, only load the latest new data, if
        false reload all data.

    Raises:
      Exception: if any arguments are invalid.
    """
    bq_schema = schema.get_beam_bigquery_schema(
        schema.get_bigquery_schema(scan_type))

    if incremental_load:
      write_mode = beam.io.BigQueryDisposition.WRITE_APPEND
    else:
      write_mode = beam.io.BigQueryDisposition.WRITE_TRUNCATE

    # Pcollection[Dict[str, Any]]
    dict_rows = (
        rows | f'dataclass to dicts {scan_type}' >> beam.Map(
            schema.flatten_to_dict).with_output_types(Dict[str, Any]))

    (dict_rows | f'Write {scan_type}' >> beam.io.WriteToBigQuery(  # pylint: disable=expression-not-assigned
        self._get_full_table_name(table_name),
        schema=bq_schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=write_mode,
        additional_bq_parameters=_get_partition_params(scan_type)))

  def _write_to_gcs(self, scan_type: str,
                    rows: beam.pvalue.PCollection[BigqueryRow],
                    gcs_folder: str) -> None:
    """Write out rows to GCS folder with hive-partioned format.

    Rows are written to .json.gz files organized by source and country:
    '{gcs_folder}/source={source}/country={country}/results.json.gz'

    Args:
      scan_type: one of 'echo', 'discard', 'http', 'https',
        or 'satellite'
      rows: PCollection[BigqueryRow] of data to write.
      gcs_folder: GCS folder gs://bucket/folder/... like
        'gs://firehook-test/scans/echo' to write output files to.

    Raises:
      Exception: if any arguments are invalid.
    """
    # Pcollection[Dict[str, Any]]
    dict_rows = (
        rows | f'dataclass to dicts {scan_type}' >> beam.Map(
            schema.flatten_to_dict).with_output_types(Dict[str, Any]))

    # PCollection[str]
    json_rows = (
        dict_rows | 'dicts to json' >>
        beam.Map(dict_to_gcs_json_string).with_output_types(str))

    # Set shards=1 and max_writers_per_bundle=0 to avoid sharding output.
    (json_rows | 'Write to GCS files' >> WriteToFiles(  # pylint: disable=expression-not-assigned
        path=gcs_folder,
        destination=_get_destination,
        sink=lambda dest: sink.JsonGzSink(),
        shards=1,
        max_writers_per_bundle=0,
        file_naming=_custom_file_naming(suffix='.json.gz')))

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
            'use_monitoring_state_manager'
        ],
        dataflow_service_options=['enable_prime'],
        setup_file='./pipeline/setup.py')
    pipeline_options.view_as(SetupOptions).save_main_session = True
    return pipeline_options

  def run_beam_pipeline(self, scan_type: str, incremental_load: bool,
                        job_name: str, table_name: Optional[str],
                        gcs_folder: Optional[str],
                        start_date: Optional[datetime.date],
                        end_date: Optional[datetime.date],
                        export_gcs: bool,
                        bq_and_gcs: bool) -> None:
    """Run a single apache beam pipeline to load json data into bigquery.

    Args:
      scan_type: one of 'echo', 'discard', 'http', 'https' or 'satellite'
      incremental_load: boolean. If true, only load the latest new data, if
        false reload all data.
      job_name: string name for this pipeline job.
      table_name: dataset.table name like 'base.scan_echo'
        if writing to Bigquery.
      gcs_folder: gs://bucket/folder/... name like 'gs://firehook-test/scans'
        if writing to GCS.
      start_date: date object, only files after or at this date will be read.
        Mostly only used during development.
      end_date: date object, only files at or before this date will be read.
        Mostly only used during development.
      export_gcs: boolean. If true, write to Google Cloud Storage, if false
        write to BigQuery.

    Raises:
      Exception: if any arguments are invalid or the pipeline fails.
    """
    logging.getLogger().setLevel(logging.INFO)

    if export_gcs and not gcs_folder:
      raise Exception('gcs_folder argument required when writing to GCS.')
    if not export_gcs and not table_name:
      raise Exception('table_name argument required when writing to BigQuery.')
    if bq_and_gcs and not gcs_folder and not table_name:
      raise Exception('table_name and gcs_folder arguments required when writing to BigQuery and GCS.')

    pipeline_options = self._get_pipeline_options(scan_type, job_name)
    gcs = GCSFileSystem(pipeline_options)

    new_filenames = self._data_to_load(gcs, scan_type, incremental_load,
                                       table_name, gcs_folder, start_date,
                                       end_date)
    if not new_filenames:
      logging.info('No new files to load')
      return

    with beam.Pipeline(options=pipeline_options) as p:
      # PCollection[Tuple[filename,line]]
      lines = _read_scan_text(p, new_filenames)

      if scan_type == schema.SCAN_TYPE_SATELLITE:
        # PCollection[SatelliteRow]
        rows = satellite.process_satellite_lines(lines, self.metadata_adder)

      else:  # Hyperquack scans
        # PCollection[HyperquackRow]
        rows = hyperquack.process_hyperquack_lines(lines, self.metadata_adder)

      _raise_error_if_collection_empty(rows)

      # TODO: Eventually we want to be able to run both exports simultaneously
      # so that we can reuse the pipeline output.
      if bq_and_gcs:
        self._write_to_bigquery(scan_type, rows, table_name, incremental_load)
        self._write_to_gcs(scan_type, rows, gcs_folder)
      elif export_gcs and gcs_folder is not None:
        self._write_to_gcs(scan_type, rows, gcs_folder)
      elif table_name is not None:
        self._write_to_bigquery(scan_type, rows, table_name, incremental_load)
