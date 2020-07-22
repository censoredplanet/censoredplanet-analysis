"""Beam pipeline for converting json scan files into bigquery tables."""

from __future__ import absolute_import

import argparse
import json
import logging
from pprint import pprint

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

bigquery_schema = {
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
    'stateful_block': 'boolean'
}
# Future fields
"""
    'row_number', 'integer',
    'domain_category': 'string',
    'netblock': 'string',
    'asn': 'string',
    'as_name': 'string',
    'as_full_name': 'string',
    'as_traffic': 'integer',
    'as_class': 'string',
    'country': 'string',

"""


def get_bigquery_schema():
  """Return a beam bigquery schema for the output table."""
  table_schema = bigquery.TableSchema()

  for (name, field_type) in bigquery_schema.items():

    field_schema = bigquery.TableFieldSchema()
    field_schema.name = name
    field_schema.type = field_type
    field_schema.mode = 'nullable'  # all fields are flat
    table_schema.fields.append(field_schema)

  return table_schema


def flatten_measurement(line):
  """Flattens a measurement string into several roundtrip rows.

  Args:
    line: a json string describing a censored planet measurement. example
    {'Keyword': 'test.com,
     'Server': '1.2.3.4',
     'Results': [{'Success': true},
                 {'Success': false}]}

  Returns:
    an array of dicts containing individual roundtrip information
    [{'column_name': field_value}]
    example
    [{'domain': 'test.com', 'ip': '1.2.3.4', 'success': true}
     {'domain': 'test.com', 'ip': '1.2.3.4', 'success': true}]
  """
  rows = []

  try:
    scan = json.loads(line)
  except json.decoder.JSONDecodeError as e:
    logging.error('%s', e)
    return rows

  for result in scan['Results']:
    rows.append({
        'domain': scan['Keyword'],
        'ip': scan['Server'],
        'date': result['StartTime'][:10],
        'start_time': result['StartTime'],
        'end_time': result['EndTime'],
        'retries': scan['Retries'],
        'sent': result['Sent'],
        'received': result.get('Received', ''),
        'error': result.get('Error', ''),
        'blocked': scan['Blocked'],
        'success': result['Success'],
        'fail_sanity': scan['FailSanity'],
        'stateful_block': scan['StatefulBlock'],
    })
  return rows


def run(argv=None, save_main_session=True):
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default='gs://firehook-scans/echo/**/results.json',
      #default='gs://firehook-scans/echo/CP_Quack-echo-2018-07-27-15-20-11/results.json',
      #default='gs://firehook-dataflow-test/results-short*.json',
      help='Input file to process.')
  parser.add_argument(
      '--output',
      dest='output',
      default='firehook-censoredplanet:echo_results.scan',
      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_args.extend([
      # DataflowRunner or DirectRunner
      '--runner=DataflowRunner',
      '--project=firehook-censoredplanet',
      '--region=us-east1',
      '--staging_location=gs://firehook-dataflow-test/staging',
      '--temp_location=gs://firehook-dataflow-test/temp',
      '--job_name=flatten-json-job',
  ])

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  with beam.Pipeline(options=pipeline_options) as p:

    lines = p | ReadFromText(known_args.input)

    rows = (lines | 'flatten json' >> (beam.FlatMap(flatten_measurement)))

    rows | 'Write' >> beam.io.WriteToBigQuery(
        known_args.output,
        schema=get_bigquery_schema(),
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        # WRITE_TRUNCATE is slow when testing.
        # write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
