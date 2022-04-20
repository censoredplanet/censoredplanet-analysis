"""Unit tests for adding ip metadata"""

from __future__ import absolute_import

import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
import apache_beam.testing.util as beam_test_util

from pipeline.metadata.schema import BigqueryRow, IpMetadata, IpMetadataWithKeys
from pipeline.metadata.ip_metadata_chooser import FakeIpMetadataChooserFactory
from pipeline.metadata import satellite
from pipeline.metadata.add_metadata import MetadataAdder


class MetadataAdderTest(unittest.TestCase):
  """Unit tests for adding ip metadata."""

  def test_annotate_row_ip(self) -> None:  # pylint: disable=no-self-use
    """Test adding IP metadata to mesurements."""
    rows = [
        BigqueryRow(
            domain='www.example.com',
            ip='8.8.8.8',
            date='2020-01-01',
            success=True,
        ),
        BigqueryRow(
            domain='www.example.com',
            ip='1.1.1.1',
            date='2020-01-01',
            success=False,
        ),
        BigqueryRow(
            domain='www.example.com',
            ip='8.8.8.8',
            date='2020-01-02',
            success=False,
        ),
        BigqueryRow(
            domain='www.example.com',
            ip='1.1.1.1',
            date='2020-01-02',
            success=True,
        )
    ]

    p = TestPipeline()
    rows = (p | beam.Create(rows))

    adder = MetadataAdder(FakeIpMetadataChooserFactory())
    rows_with_metadata = adder.annotate_row_ip(rows)

    expected = [
        BigqueryRow(
            domain='www.example.com',
            ip='8.8.8.8',
            date='2020-01-01',
            success=True,
            ip_metadata=IpMetadata(
                netblock='8.8.8.0/24',
                asn=15169,
                as_name='GOOGLE',
                as_full_name='Google LLC',
                as_class='Content',
                country='US',
            )),
        BigqueryRow(
            domain='www.example.com',
            ip='1.1.1.1',
            date='2020-01-01',
            success=False,
            ip_metadata=IpMetadata(
                netblock='1.0.0.1/24',
                asn=13335,
                as_name='CLOUDFLARENET',
                as_full_name='Cloudflare Inc.',
                as_class='Content',
                country='US',
            )),
        BigqueryRow(
            domain='www.example.com',
            ip='8.8.8.8',
            date='2020-01-02',
            success=False,
            ip_metadata=IpMetadata(
                netblock='8.8.8.0/24',
                asn=15169,
                as_name='GOOGLE',
                as_full_name='Google LLC',
                as_class='Content',
                country='US',
            )),
        BigqueryRow(
            domain='www.example.com',
            ip='1.1.1.1',
            date='2020-01-02',
            success=True,
            ip_metadata=IpMetadata(
                netblock='1.0.0.1/24',
                asn=13335,
                as_name='CLOUDFLARENET',
                as_full_name='Cloudflare Inc.',
                as_class='Content',
                country='US',
            ))
    ]

    beam_test_util.assert_that(rows_with_metadata,
                               beam_test_util.equal_to(expected))

  # pylint: disable=protected-access

  def test_annotate_ips_caida(self) -> None:
    """Test merging given IP metadata with given measurements."""
    adder = MetadataAdder(FakeIpMetadataChooserFactory())
    metadatas = list(adder._annotate_ips('2020-01-01', ['1.1.1.1', '8.8.8.8']))

    expected_key_1: satellite.DateIpKey = ('2020-01-01', '1.1.1.1')
    expected_value_1 = IpMetadataWithKeys(
        ip='1.1.1.1',
        date='2020-01-01',
        netblock='1.0.0.1/24',
        asn=13335,
        as_name='CLOUDFLARENET',
        as_full_name='Cloudflare Inc.',
        as_class='Content',
        country='US',
        organization='Fake Cloudflare Sub-Org',
    )

    expected_key_2: satellite.DateIpKey = ('2020-01-01', '8.8.8.8')
    expected_value_2 = IpMetadataWithKeys(
        ip='8.8.8.8',
        date='2020-01-01',
        netblock='8.8.8.0/24',
        asn=15169,
        as_name='GOOGLE',
        as_full_name='Google LLC',
        as_class='Content',
        country='US',
        # No organization data is added since the ASN doesn't match dbip
    )

    self.assertListEqual(metadatas, [(expected_key_1, expected_value_1),
                                     (expected_key_2, expected_value_2)])

  def disabled_test_annotate_ips_maxmind(self) -> None:
    """Test merging given IP metadata with given measurements."""
    # TODO turn back on once maxmind is reenabled.

    adder = MetadataAdder(FakeIpMetadataChooserFactory())
    metadatas = list(adder._annotate_ips('2020-01-01', ['1.1.1.3']))

    # Test Maxmind lookup when country data is missing
    # Cloudflare IPs return Australia
    expected_key_1 = ('2020-01-01', '1.1.1.3')
    expected_value_1 = IpMetadataWithKeys(
        ip='1.1.1.3',
        date='2020-01-01',
        netblock='1.0.0.1/24',
        asn=13335,
        as_name='CLOUDFLARENET',
        as_full_name='Cloudflare Inc.',
        as_class='Content',
        organization='Fake Cloudflare Sub-Org',
        country='AU')

    self.assertListEqual(metadatas, [(expected_key_1, expected_value_1)])

  # pylint: enable=protected-access


if __name__ == '__main__':
  unittest.main()
