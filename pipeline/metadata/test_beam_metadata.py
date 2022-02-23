"""Unit tests for merging metadata."""

from typing import Dict, List
import unittest

from pipeline.metadata import beam_metadata
from pipeline.metadata.flatten import Row


class BeamMetadataTest(unittest.TestCase):
  """Unit tests for merging metadata"""

  def test_merge_metadata_with_rows(self) -> None:
    """Test merging IP metadata pcollection with rows pcollection."""
    key: beam_metadata.DateIpKey = ('2020-01-01', '1.1.1.1')
    ip_metadata: Row = {
        'netblock': '1.0.0.1/24',
        'asn': 13335,
        'as_name': 'CLOUDFLARENET',
        'as_full_name': 'Cloudflare Inc.',
        'as_class': 'Content',
        'country': 'US',
    }
    rows: List[Row] = [{
        'domain': 'www.example.com',
        'ip': '1.1.1.1',
        'date': '2020-01-01',
    }, {
        'domain': 'www.example2.com',
        'ip': '1.1.1.1',
        'date': '2020-01-01',
    }]
    value: Dict[str, List[Row]] = {
        beam_metadata.BEAM_COGROUP_A_SIDE: [ip_metadata],
        beam_metadata.BEAM_COGROUP_B_SIDE: rows
    }

    expected_rows = [{
        'domain': 'www.example.com',
        'ip': '1.1.1.1',
        'date': '2020-01-01',
        'netblock': '1.0.0.1/24',
        'asn': 13335,
        'as_name': 'CLOUDFLARENET',
        'as_full_name': 'Cloudflare Inc.',
        'as_class': 'Content',
        'country': 'US',
    }, {
        'domain': 'www.example2.com',
        'ip': '1.1.1.1',
        'date': '2020-01-01',
        'netblock': '1.0.0.1/24',
        'asn': 13335,
        'as_name': 'CLOUDFLARENET',
        'as_full_name': 'Cloudflare Inc.',
        'as_class': 'Content',
        'country': 'US',
    }]

    rows_with_metadata = list(
        beam_metadata.merge_metadata_with_rows(key, value))
    self.assertListEqual(rows_with_metadata, expected_rows)


if __name__ == '__main__':
  unittest.main()
