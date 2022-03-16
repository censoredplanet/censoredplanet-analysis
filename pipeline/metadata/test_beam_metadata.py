"""Unit tests for merging metadata."""

from typing import Dict, List, Union
import unittest

from pipeline.metadata import beam_metadata
from pipeline.metadata.flatten_base import Row, IpMetadata


class BeamMetadataTest(unittest.TestCase):
  """Unit tests for merging metadata"""

  def test_merge_metadata_with_rows(self) -> None:
    """Test merging IP metadata pcollection with rows pcollection."""
    key: beam_metadata.DateIpKey = ('2020-01-01', '1.1.1.1')
    ip_metadata = IpMetadata(
        ip='1.1.1.1',
        date='2020-01-01',
        netblock='1.0.0.1/24',
        asn=13335,
        as_name='CLOUDFLARENET',
        as_full_name='Cloudflare Inc.',
        as_class='Content',
        country='US',
    )
    rows = [
        Row(
            domain='www.example.com',
            ip='1.1.1.1',
            date='2020-01-01',
        ),
        Row(
            domain='www.example2.com',
            ip='1.1.1.1',
            date='2020-01-01',
        )
    ]
    value: Dict[str, Union[List[Row], List[IpMetadata]]] = {
        beam_metadata.IP_METADATA_PCOLLECTION_NAME: [ip_metadata],
        beam_metadata.ROWS_PCOLLECION_NAME: rows
    }

    expected_rows = [
        Row(
            domain='www.example.com',
            ip='1.1.1.1',
            date='2020-01-01',
            netblock='1.0.0.1/24',
            asn=13335,
            as_name='CLOUDFLARENET',
            as_full_name='Cloudflare Inc.',
            as_class='Content',
            country='US',
        ),
        Row(
            domain='www.example2.com',
            ip='1.1.1.1',
            date='2020-01-01',
            netblock='1.0.0.1/24',
            asn=13335,
            as_name='CLOUDFLARENET',
            as_full_name='Cloudflare Inc.',
            as_class='Content',
            country='US',
        )
    ]

    rows_with_metadata = list(
        beam_metadata.merge_metadata_with_rows(key, value))
    self.assertListEqual(rows_with_metadata, expected_rows)


if __name__ == '__main__':
  unittest.main()
