"""Unit tests for the schema pipeline."""

import unittest

from apache_beam.io.gcp.internal.clients import bigquery as beam_bigquery

from pipeline.metadata import schema


class PipelineMainTest(unittest.TestCase):
  """Unit tests for schema."""

  def test_get_bigquery_schema_hyperquack(self) -> None:
    """Test getting the right bigquery schema for data types."""
    echo_schema = schema.get_bigquery_schema('echo')
    all_hyperquack_top_level_columns = list(
        schema.HYPERQUACK_BIGQUERY_SCHEMA.keys())
    self.assertListEqual(
        list(echo_schema.keys()), all_hyperquack_top_level_columns)

  def test_get_bigquery_schema_satellite(self) -> None:
    satellite_schema = schema.get_bigquery_schema('satellite')
    all_satellite_top_level_columns = list(
        schema.SATELLITE_BIGQUERY_SCHEMA.keys())
    self.assertListEqual(
        list(satellite_schema.keys()), all_satellite_top_level_columns)

  def test_get_beam_bigquery_schema(self) -> None:
    """Test making a bigquery schema for beam's table writing."""
    test_field = {
        'string_field': ('string', 'nullable'),
        'int_field': ('integer', 'repeated'),
    }

    table_schema = schema.get_beam_bigquery_schema(test_field)

    expected_field_schema_1 = beam_bigquery.TableFieldSchema()
    expected_field_schema_1.name = 'string_field'
    expected_field_schema_1.type = 'string'
    expected_field_schema_1.mode = 'nullable'

    expected_field_schema_2 = beam_bigquery.TableFieldSchema()
    expected_field_schema_2.name = 'int_field'
    expected_field_schema_2.type = 'integer'
    expected_field_schema_2.mode = 'repeated'

    expected_table_schema = beam_bigquery.TableSchema()
    expected_table_schema.fields.append(expected_field_schema_1)
    expected_table_schema.fields.append(expected_field_schema_2)

    self.assertEqual(table_schema, expected_table_schema)
