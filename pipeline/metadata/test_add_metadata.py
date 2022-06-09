"""Unit tests for adding ip metadata"""

from __future__ import absolute_import

import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
import apache_beam.testing.util as beam_test_util

from pipeline.metadata.schema import BigqueryRow, SatelliteRow, SatelliteAnswer, IpMetadata, IpMetadataWithDateKey
from pipeline.metadata.ip_metadata_chooser import FakeIpMetadataChooserFactory
from pipeline.metadata import beam_metadata
from pipeline.metadata.add_metadata import MetadataAdder


class MetadataAdderTest(unittest.TestCase):
  """Unit tests for adding ip metadata."""

  def test_annotate_row_ip(self) -> None:
    """Test adding IP metadata to measurement ips."""
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
                organization='Fake Cloudflare Sub-Org')),
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
                organization='Fake Cloudflare Sub-Org'))
    ]
    adder = MetadataAdder(FakeIpMetadataChooserFactory())

    with TestPipeline() as p:
      rows = p | beam.Create(rows)
      rows_with_metadata = adder.annotate_row_ip(rows)
      beam_test_util.assert_that(rows_with_metadata,
                                 beam_test_util.equal_to(expected))

  def test_annotate_answer_ips(self) -> None:
    """Test adding IP metadata to measurement answers."""
    rows = [
        SatelliteRow(
            domain='dns.google.com',
            date='2020-01-01',
            success=True,
            received=[SatelliteAnswer(ip='8.8.8.8')]),
        SatelliteRow(
            domain='dns.google.com',
            date='2020-01-01',
            success=False,
            received=[
                SatelliteAnswer(
                    ip='8.8.8.8',
                    cert='MII...',  # non IpMetadata tags are not overwritten
                )
            ]),
        SatelliteRow(
            domain='one.one.one.one',
            date='2020-01-02',
            success=False,
            received=[
                SatelliteAnswer(
                    ip='1.1.1.1',
                    ip_metadata=IpMetadata(
                        # tags we also have in routeviews are overwritten
                        country='AU'))
            ]),
        SatelliteRow(
            domain='www.example.com',
            date='2020-01-02',
            success=False,
            received=[
                SatelliteAnswer(
                    ip='1.2.3.4',
                    ip_metadata=IpMetadata(
                        asn=1234,
                        # tags we don't have in routeviews are not overwritten
                        as_name='1234 Name Not Overwritten',
                    ))
            ])
    ]

    expected = [
        SatelliteRow(
            domain='dns.google.com',
            date='2020-01-01',
            success=True,
            received=[
                SatelliteAnswer(
                    ip='8.8.8.8',
                    ip_metadata=IpMetadata(
                        netblock='8.8.8.0/24',
                        asn=15169,
                        as_name='GOOGLE',
                        as_full_name='Google LLC',
                        as_class='Content',
                        country='US',
                    ))
            ]),
        SatelliteRow(
            domain='dns.google.com',
            date='2020-01-01',
            success=False,
            received=[
                SatelliteAnswer(
                    ip='8.8.8.8',
                    cert='MII...',
                    ip_metadata=IpMetadata(
                        netblock='8.8.8.0/24',
                        asn=15169,
                        as_name='GOOGLE',
                        as_full_name='Google LLC',
                        as_class='Content',
                        country='US',
                    ))
            ]),
        SatelliteRow(
            domain='one.one.one.one',
            date='2020-01-02',
            success=False,
            received=[
                SatelliteAnswer(
                    ip='1.1.1.1',
                    ip_metadata=IpMetadata(
                        netblock='1.0.0.1/24',
                        asn=13335,
                        as_name='CLOUDFLARENET',
                        as_full_name='Cloudflare Inc.',
                        as_class='Content',
                        country='US',
                        organization='Fake Cloudflare Sub-Org'))
            ]),
        SatelliteRow(
            domain='www.example.com',
            date='2020-01-02',
            success=False,
            received=[
                SatelliteAnswer(
                    ip='1.2.3.4',
                    ip_metadata=IpMetadata(
                        asn=1234,
                        as_name='1234 Name Not Overwritten',
                    ))
            ])
    ]

    adder = MetadataAdder(FakeIpMetadataChooserFactory())

    with TestPipeline() as p:
      rows = p | beam.Create(rows)
      rows_with_answer_metadata = adder.annotate_answer_ips(rows)
      beam_test_util.assert_that(rows_with_answer_metadata,
                                 beam_test_util.equal_to(expected))

  # pylint: disable=protected-access

  def test_annotate_ips_caida(self) -> None:
    """Test merging given IP metadata with given measurements."""
    adder = MetadataAdder(FakeIpMetadataChooserFactory())
    metadatas = list(adder._annotate_ips('2020-01-01', ['1.1.1.1', '8.8.8.8']))

    expected_key_1: beam_metadata.DateIpKey = ('2020-01-01', '1.1.1.1')
    expected_value_1 = IpMetadataWithDateKey(
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

    expected_key_2: beam_metadata.DateIpKey = ('2020-01-01', '8.8.8.8')
    expected_value_2 = IpMetadataWithDateKey(
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
    expected_value_1 = IpMetadataWithDateKey(
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
