"""Unit tests for the schema pipeline."""

import unittest
from typing import Dict, Any

from apache_beam.io.gcp.internal.clients import bigquery as beam_bigquery

from pipeline.metadata import schema


class SchemaTest(unittest.TestCase):
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

  def test_hyperquack_flatten_matches_bq_schema(self) -> None:
    """Test that the hyperquack flat row schema matches the bq schema."""
    bq_table_schema = schema.get_beam_bigquery_schema(
        schema.get_bigquery_schema('https'))

    # yapf: disable
    row = schema.HyperquackRow(
        domain = 'www.arabhra.org',
        category = 'Intergovernmental Organizations',
        ip = '213.175.166.157',
        date = '2020-11-06',
        start_time = '2020-11-06T15:24:21.124508839-05:00',
        end_time = '2020-11-06T15:24:21.812075476-05:00',
        retry = 0,
        error = 'Incorrect web response: status lines don\'t match',
        anomaly = False,
        success = False,
        is_control = False,
        controls_failed = False,
        measurement_id = '81e2a76dafe04131bc38fc6ec7bbddca',
        source = 'CP_Quack-https-2020-11-06-15-15-31',
        stateful_block = False,
        ip_metadata = schema.IpMetadata(
            netblock = '213.175.166.157/24',
            asn = 13335,
            as_name = 'CLOUDFLARENET',
            as_full_name = 'Cloudflare Inc.',
            as_class = 'Content',
            country = 'US',
            organization = 'Fake Organization'
        ),
        received = schema.HttpsResponse(
            is_known_blockpage = False,
            page_signature = 'x_example_signature',
            status = '302 Found',
            body = 'example body',
            headers = [
                'Content-Language: en',
                'X-Frame-Options: SAMEORIGIN',
            ],
            tls_version = 771,
            tls_cipher_suite = 49199,
            tls_cert = 'MIIH...',
            tls_cert_matches_domain=True,
            tls_cert_common_name = 'example.com',
            tls_cert_issuer = 'Verisign',
            tls_cert_alternative_names = ['www.example.com']
        ),
        outcome='content/status_mismatch:302'
    )
    # yapf: disable
    flat_row = schema.flatten_to_dict(row)

    self.assert_flat_row_matches_bq_schema(flat_row, bq_table_schema)

  def test_satellite_flatten_matches_bq_schema(self) -> None:
    """Test that the satellite flat row schema matches the bq schema."""
    bq_table_schema = schema.get_beam_bigquery_schema(schema.get_bigquery_schema('satellite'))

    # yapf: disable
    row = schema.SatelliteRow(
        domain = 'a.root-servers.net',
        is_control =  True,
        category = 'Control',
        ip = '62.80.182.26',
        is_control_ip = False,
        date = '2021-10-20',
        start_time = '2021-10-20T14:51:45.361175691-04:00',
        end_time = '2021-10-20T14:51:46.261234037-04:00',
        retry = 0,
        error = 'read udp 141.212.123.185:30437->62.80.182.26:53: read: connection refused',
        anomaly = False,
        success = False,
        measurement_id = '47de079652ca53f7bdb57ca956e1c70e',
        source = 'CP_Satellite-2021-10-20-12-00-01',
        controls_failed = True,
        average_confidence = 100,
        untagged_controls = False,
        untagged_response = False,
        excluded = False,
        exclude_reason = '',
        rcode = -1,
        has_type_a = True,
        ip_metadata = schema.IpMetadata(
          country = 'UA',
          name = 'mx4.orlantrans.com.',
          netblock = '213.175.166.157/24',
          asn = 13335,
          as_name = 'CLOUDFLARENET',
          as_full_name = 'Cloudflare Inc.',
          as_class = 'Content',
          organization = 'Fake Organization',
          non_zero_rcode_rate = 0.129,
          private_ip_rate = 0,
          zero_ip_rate = 0,
          connect_error_rate = 0,
          invalid_cert_rate = 0
        ),
        received = [
          schema.SatelliteAnswer(
            ip = '13.249.134.38',
            cert = 'MII...',
            http = 'c5ba7f2da503045170f1d66c3e9f84576d8f3a606bb246db589a8f62c65921af',
            matches_control = schema.MatchesControl(
              ip=True,
              http=True,
              cert=False,
              asnum=True,
              asname=True
            ),
            match_confidence = 100,
            ip_metadata = schema.IpMetadata(
                asn=16509,
                as_name='AMAZON-02',
                country = 'US',
                netblock = '213.175.166.157/24',
                organization = 'VX GROUP - AWS'
            ),
            http_error = 'Get \"http://31.13.77.33:80/\": dial tcp 31.13.77.33:80: connect: connection refused',
            http_response = schema.HttpsResponse(
              is_known_blockpage = False,
              page_signature = 'x_example_fp',
              status = '200',
              body = 'example body',
              headers = [
                'Content-Language: en',
                'X-Frame-Options: SAMEORIGIN',
              ]
            ),
            https_error = 'Get \"https://31.13.77.33:443/\": net/http: request canceled (Client.Timeout exceeded while awaiting headers)',
            https_response = schema.HttpsResponse(
              is_known_blockpage = False,
              page_signature = 'x_example_fp',
              status = '200',
              body = 'example body',
              tls_version = 123,
              tls_cipher_suite = 3,
              tls_cert = 'MII...',
              tls_cert_common_name = 'Common Name',
              tls_cert_issuer = 'Issuer',
              tls_cert_start_date = '2018-04-06T12:00:00',
              tls_cert_end_date = '2012-04-06T12:00:00',
              tls_cert_alternative_names = ['Common Name', 'Alt Name'],
              tls_cert_has_trusted_ca = True,
              tls_cert_matches_domain = True,
              headers = [
                'Content-Language: en',
                'X-Frame-Options: SAMEORIGIN',
              ]
            )
          )
        ]
    )
    # yapf: enable
    flat_row = schema.flatten_to_dict(row)

    self.assert_flat_row_matches_bq_schema(flat_row, bq_table_schema)

  def test_hyperquack_flatten_matches_gcs_dict(self) -> None:
    """Test that the hyperquack gcs dict matches the expected fields."""
    bq_table_schema = schema.get_beam_bigquery_schema(
        schema.get_bigquery_schema('https'))

    # yapf: disable
    row = schema.HyperquackRow(
        domain = 'www.arabhra.org',
        category = 'Intergovernmental Organizations',
        ip = '213.175.166.157',
        date = '2020-11-06',
        start_time = '2020-11-06T15:24:21.124508839-05:00',
        end_time = '2020-11-06T15:24:21.812075476-05:00',
        retry=1,
        error = 'Incorrect web response: status lines don\'t match',
        anomaly = False,
        success = False,
        is_control = False,
        controls_failed = False,
        measurement_id = '81e2a76dafe04131bc38fc6ec7bbddca',
        source = 'CP_Quack-https-2020-11-06-15-15-31',
        stateful_block = False,
        ip_metadata = schema.IpMetadata(
            netblock = '213.175.166.157/24',
            asn = 13335,
            as_name = 'CLOUDFLARENET',
            as_full_name = 'Cloudflare Inc.',
            as_class = 'Content',
            country = 'US',
            organization = 'Fake Organization'
        ),
        received = schema.HttpsResponse(
            is_known_blockpage = False,
            page_signature = 'x_example_signature',
            status = '302 Found',
            body = 'example body',
            headers = [
                'Content-Language: en',
                'X-Frame-Options: SAMEORIGIN',
            ],
            tls_version = 771,
            tls_cipher_suite = 49199,
            tls_cert = 'MIIH...',
            tls_cert_matches_domain=True,
            tls_cert_common_name = 'example.com',
            tls_cert_issuer = 'Verisign',
            tls_cert_alternative_names = ['www.example.com']
        ),
        outcome='content/status_mismatch:302'
    )
    expected_dict = {
      'domain': 'www.arabhra.org',
      'domain_is_control': False,
      'date': '2020-11-06',
      'start_time': '2020-11-06T15:24:21.124508839-05:00',
      'end_time': '2020-11-06T15:24:21.812075476-05:00',
      'retry': 1,
      'server_ip': '213.175.166.157',
      'server_netblock': '213.175.166.157/24',
      'server_asn': 13335,
      'server_as_name': 'CLOUDFLARENET',
      'server_as_full_name': 'Cloudflare Inc.',
      'server_as_class': 'Content',
      'server_country': 'US',
      'server_organization': 'Fake Organization',
      'received_error': "Incorrect web response: status lines don't match",
      'received_tls_version': 771,
      'received_tls_cipher_suite': 49199,
      'received_tls_cert': 'MIIH...',
      'received_tls_cert_common_name': 'example.com',
      'received_tls_cert_issuer': 'Verisign',
      'received_tls_cert_alternative_names': ['www.example.com'],
      'received_status': '302 Found',
      'received_headers': ['Content-Language: en', 'X-Frame-Options: SAMEORIGIN'],
      'received_body': 'example body',
      'matches_template': False,
      'no_response_in_measurement_matches_template': False,
      'controls_failed': False,
      'stateful_block': False,
      'measurement_id': '81e2a76dafe04131bc38fc6ec7bbddca',
      'source': 'CP_Quack-https-2020-11-06-15-15-31'
    }
    # yapf: disable
    gcs_dict = schema.dict_to_gcs_dict_hyperquack(schema.flatten_to_dict(row))
    self.assertDictEqual(gcs_dict, expected_dict)

  def test_satellite_flatten_matches_gcs_dict(self) -> None:
    """Test that the satellite gcs dict matches the expected fields."""
    # yapf: disable
    row = schema.SatelliteRow(
        domain = 'a.root-servers.net',
        is_control =  True,
        category = 'Control',
        ip = '62.80.182.26',
        is_control_ip = False,
        date = '2021-10-20',
        start_time = '2021-10-20T14:51:45.361175691-04:00',
        end_time = '2021-10-20T14:51:46.261234037-04:00',
        retry = None,
        error = 'read udp 141.212.123.185:30437->62.80.182.26:53: read: connection refused',
        anomaly = False,
        success = False,
        measurement_id = '47de079652ca53f7bdb57ca956e1c70e',
        source = 'CP_Satellite-2021-10-20-12-00-01',
        controls_failed = True,
        average_confidence = 100,
        untagged_controls = False,
        untagged_response = False,
        excluded = False,
        exclude_reason = '',
        rcode = -1,
        has_type_a = True,
        ip_metadata = schema.IpMetadata(
          country = 'UA',
          name = 'mx4.orlantrans.com.',
          netblock = '213.175.166.157/24',
          asn = 13335,
          as_name = 'CLOUDFLARENET',
          as_full_name = 'Cloudflare Inc.',
          as_class = 'Content',
          organization = 'Fake Organization',
          non_zero_rcode_rate = 0.129,
          private_ip_rate = 0,
          zero_ip_rate = 0,
          connect_error_rate = 0,
          invalid_cert_rate = 0
        ),
        received = [
          schema.SatelliteAnswer(
            ip = '13.249.134.38',
            cert = 'MII...',
            http = 'c5ba7f2da503045170f1d66c3e9f84576d8f3a606bb246db589a8f62c65921af',
            matches_control = schema.MatchesControl(
              ip=True,
              http=True,
              cert=False,
              asnum=True,
              asname=True
            ),
            match_confidence = 100,
            ip_metadata = schema.IpMetadata(
                asn=16509,
                as_name='AMAZON-02',
                country = 'US',
                netblock = '213.175.166.157/24',
                organization = 'VX GROUP - AWS'
            ),
            http_error = 'Get \"http://31.13.77.33:80/\": dial tcp 31.13.77.33:80: connect: connection refused',
            http_response = schema.HttpsResponse(
              is_known_blockpage = False,
              page_signature = 'x_example_fp',
              status = '200',
              body = 'example body',
              headers = [
                'Content-Language: en',
                'X-Frame-Options: SAMEORIGIN',
              ]
            ),
            https_error = 'Get \"https://31.13.77.33:443/\": net/http: request canceled (Client.Timeout exceeded while awaiting headers)',
            https_response = schema.HttpsResponse(
              is_known_blockpage = False,
              page_signature = 'x_example_fp',
              status = '200',
              body = 'example body',
              tls_version = 123,
              tls_cipher_suite = 3,
              tls_cert = 'MII...',
              tls_cert_matches_domain=True,
              tls_cert_common_name = 'Common Name',
              tls_cert_issuer = 'Issuer',
              tls_cert_start_date = '2018-04-06T12:00:00',
              tls_cert_end_date = '2012-04-06T12:00:00',
              tls_cert_alternative_names = ['Common Name', 'Alt Name'],
              headers = [
                'Content-Language: en',
                'X-Frame-Options: SAMEORIGIN',
              ]
            )
          )
        ]
    )
    expected_dict = {
      'domain': 'a.root-servers.net',
      'domain_is_control': True,
      'date': '2021-10-20',
      'start_time': '2021-10-20T14:51:45.361175691-04:00',
      'end_time': '2021-10-20T14:51:46.261234037-04:00',
      'retry': None,
      'resolver_ip': '62.80.182.26',
      'resolver_name': 'mx4.orlantrans.com.',
      'resolver_is_trusted': False,
      'resolver_netblock': '213.175.166.157/24',
      'resolver_asn': 13335,
      'resolver_as_name': 'CLOUDFLARENET',
      'resolver_as_full_name': 'Cloudflare Inc.',
      'resolver_as_class': 'Content',
      'resolver_country': 'UA',
      'resolver_organization': 'Fake Organization',
      'received_error': 'read udp 141.212.123.185:30437->62.80.182.26:53: read: connection refused',
      'received_rcode': -1,
      'answers': [{
        'ip': '13.249.134.38',
        'asn': 16509,
        'as_name': 'AMAZON-02',
        'ip_organization': 'VX GROUP - AWS',
        'censys_http_body_hash': 'c5ba7f2da503045170f1d66c3e9f84576d8f3a606bb246db589a8f62c65921af',
        'censys_ip_cert': 'MII...',
        'http_error': 'Get "http://31.13.77.33:80/": dial tcp 31.13.77.33:80: connect: connection refused',
        'http_response_status': '200',
        'http_response_headers': ['Content-Language: en', 'X-Frame-Options: SAMEORIGIN'],
        'http_response_body': 'example body',
        'https_error': 'Get "https://31.13.77.33:443/": net/http: request canceled (Client.Timeout exceeded while awaiting headers)',
        'https_tls_version': 123,
        'https_tls_cipher_suite': 3,
        'https_tls_cert': 'MII...',
        'https_tls_cert_common_name': 'Common Name',
        'https_tls_cert_issuer': 'Issuer',
        'https_tls_cert_start_date': '2018-04-06T12:00:00',
        'https_tls_cert_end_date': '2012-04-06T12:00:00',
        'https_tls_cert_alternative_names': ['Common Name', 'Alt Name'],
        'https_response_status': '200',
        'https_response_headers': ['Content-Language: en', 'X-Frame-Options: SAMEORIGIN'],
        'https_response_body': 'example body'
      }],
      'measurement_id': '47de079652ca53f7bdb57ca956e1c70e',
      'source': 'CP_Satellite-2021-10-20-12-00-01'
    }
    # yapf: enable
    gcs_dict = schema.dict_to_gcs_dict_satellite(schema.flatten_to_dict(row))
    self.assertDictEqual(gcs_dict, expected_dict)

  def assert_flat_row_matches_bq_schema(
      self, flat_row: Dict[str,
                           Any], bq_schema: beam_bigquery.TableSchema) -> None:
    """Helper for testing schema matches

    Raises:
      Exception if types don't match or the value is incompletly filled in.
    """
    schema_names = [field.name for field in bq_schema.fields]

    for (key, value) in flat_row.items():
      self.assertIn(key, schema_names)
      i = schema_names.index(key)
      bq_field = bq_schema.fields[i]
      self.assert_compatible_bq_field_type(bq_field, value, key)

  # pylint: disable=multiple-statements
  # pylint: disable=too-many-return-statements
  # pylint: disable=too-many-branches
  def assert_compatible_bq_field_type(self,
                                      field: beam_bigquery.TableFieldSchema,
                                      value: Any, field_name: str) -> None:
    """Helper for testing bq row type matches.

    Some kinds of type coercion are allowed in bq writes.
    This doesn't aim to be comprehensive,
    just a sanity check for the fields we have.

    Raises:
      Exception if types don't match or the value is incompletly filled in.
    """
    value_type = type(value)
    field_type = field.type

    if value is None:
      raise Exception(
          f'Please enumerate all row fields in the test. Missing field: {field_name}'
      )
    if isinstance(value, str):
      if field_type == 'string':
        return None
      if field_type == 'date':
        return None
      if field_type == 'timestamp':
        return None
      if field_type == 'bytes':
        return None
      raise Exception(
          f'Row value type {value_type} doesn\'t match {field_type}')
    if isinstance(value, bool):
      if field_type == 'boolean':
        return None
      raise Exception(
          f'Row value type {value_type} doesn\'t match {field_type}')
    if isinstance(value, float):
      if field_type == 'float':
        return None
      raise Exception(
          f'Row value type {value_type} doesn\'t match {field_type}')
    if isinstance(value, int):
      if field_type == 'integer':
        return None
      if field_type == 'float':
        return None
      raise Exception(
          f'Row value type {value_type} doesn\'t match {field_type}')
    if isinstance(value, list):
      if len(value) == 0:
        raise Exception(
            f'Please add content to lists in the test. Empty list: {field_name}'
        )
      if not field.mode == 'repeated':
        raise Exception(f'List {value} in non repeated field: {field_name}')
      return self.assert_compatible_bq_field_type(field, value[0], field_name)
    if isinstance(value, dict):
      return self.assert_flat_row_matches_bq_schema(value, field)
    raise Exception(f'Unknown row value type {value_type}, value: {value}')
