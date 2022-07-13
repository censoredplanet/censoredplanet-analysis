"""Unit tests for satellite."""

import json
from typing import Tuple
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
import apache_beam.testing.util as beam_test_util

from pipeline.metadata.schema import SatelliteRow, PageFetchRow, HttpsResponse, SatelliteAnswer, SatelliteAnswerWithSourceKey, IpMetadata, IpMetadataWithSourceKey, MatchesControl
from pipeline.metadata import satellite

# pylint: disable=too-many-lines


def process_satellite_with_tags(
    row_lines: beam.pvalue.PCollection[Tuple[str, str]],
    answer_lines: beam.pvalue.PCollection[Tuple[str, str]],
    resolver_lines: beam.pvalue.PCollection[Tuple[str, str]]
) -> beam.pvalue.PCollection[SatelliteRow]:
  """Helper method to test processing satellite tag information."""
  rows = satellite.parse_and_flatten_satellite_rows(row_lines)
  resolver_tags = satellite.parse_satellite_resolver_tags(resolver_lines)
  answer_tags = satellite.parse_satellite_answer_tags(answer_lines)

  resolver_tagged_satellite = satellite.add_vantage_point_tags(
      rows, resolver_tags)
  answer_tagged_satellite = satellite.add_satellite_answer_tags(
      resolver_tagged_satellite, answer_tags)

  return answer_tagged_satellite


class SatelliteTest(unittest.TestCase):
  """Unit tests for satellite steps."""

  # pylint: disable=protected-access

  def test_make_source_ip_key(self) -> None:
    row = IpMetadataWithSourceKey(
        source='CP_Satellite-2020-12-17-12-00-01',
        ip='1.2.3.4',
    )
    self.assertEqual(
        satellite.make_source_ip_key(row),
        ('CP_Satellite-2020-12-17-12-00-01', '1.2.3.4'))

  def test_read_satellite_resolver_tags(self) -> None:
    """Test reading rows from Satellite resolver tag files."""
    tagged_resolver1 = {'resolver': '1.1.1.1', 'country': 'United States'}
    tagged_resolver2 = {'resolver': '1.1.1.3', 'country': 'Australia'}
    # yapf: disable

    lines = [
        json.dumps(tagged_resolver1),
        json.dumps(tagged_resolver2),
    ]

    filenames = [
      "CP_Satellite-2020-12-17-12-00-01/resolvers.json",
      "CP_Satellite-2020-12-17-12-00-01/resolvers.json.gz",
    ]

    data = zip(filenames, lines)

    tag1 = IpMetadataWithSourceKey(
        ip='1.1.1.1',
        source='CP_Satellite-2020-12-17-12-00-01',
        country='US'
    )
    tag2 = IpMetadataWithSourceKey(
        ip='1.1.1.3',
        source='CP_Satellite-2020-12-17-12-00-01',
        country='AU'
    )
    # yapf: enable

    expected_resolver_tags = [tag1, tag2]

    with TestPipeline() as p:
      lines = p | 'create tags' >> beam.Create(data)

      resolver_tags = lines | 'read resolver tags' >> beam.FlatMapTuple(
          satellite._read_satellite_resolver_tags)

      beam_test_util.assert_that(
          resolver_tags, beam_test_util.equal_to(expected_resolver_tags))

  def test_read_satellite_answer_tags(self) -> None:
    """Test reading rows from Satellite answer tag files."""
    # yapf: disable
    tagged_answer1 = {
      'ip': '60.210.17.137',
      'asname': 'CHINA169-BACKBONE CHINA UNICOM China169 Backbone',
      'asnum': 4837,
      'cert': 'a2fed117238c94a04ba787cfe69e93de36cc8571bab44d5481df9becb9beec75',
      'http': 'e3c1d34ca489928190b45f0535624b872717d1edd881c8ab4b2c62f898fcd4a5'
    }

    lines = [
        json.dumps(tagged_answer1)
    ]

    filenames = [
      "CP_Satellite-2020-12-17-12-00-01/tagged_responses.json",
    ]

    data = zip(filenames, lines)

    tag1 = SatelliteAnswerWithSourceKey(
        ip='60.210.17.137',
        source='CP_Satellite-2020-12-17-12-00-01',
        cert='a2fed117238c94a04ba787cfe69e93de36cc8571bab44d5481df9becb9beec75',
        http='e3c1d34ca489928190b45f0535624b872717d1edd881c8ab4b2c62f898fcd4a5',
        ip_metadata=IpMetadata(
            asn=4837,
            as_name='CHINA169-BACKBONE CHINA UNICOM China169 Backbone'
        )
    )
    # yapf: enable
    expected_answer_tags = [tag1]

    with TestPipeline() as p:
      lines = p | 'create tags' >> beam.Create(data)

      answer_tags = lines | 'read answer tags' >> beam.FlatMapTuple(
          satellite._read_satellite_answer_tags)

      beam_test_util.assert_that(answer_tags,
                                 beam_test_util.equal_to(expected_answer_tags))

  def test_process_satellite_v1(self) -> None:
    """Test processing of Satellite v1 interference and tag files."""

    data_filenames = [
        "CP_Satellite-2020-09-02-12-00-01/interference.json",
        "CP_Satellite-2020-09-02-12-00-01/interference.json"
    ]

    _data = [
        {
            'resolver': '1.1.1.3',
            'query': 'signal.org',
            'answers': {
                '13.249.134.38': ['ip', 'http', 'asnum', 'asname'],
                '13.249.134.44': ['ip', 'http', 'asnum', 'asname'],
                '13.249.134.74': ['ip', 'http', 'asnum', 'asname'],
                '13.249.134.89': ['ip', 'http', 'asnum', 'asname']
            },
            'passed': True
        },
        {
            'resolver': '1.1.1.3',
            'query': 'adl.org',
            'answers': {
                '192.124.249.107': ['ip', 'no_tags']
            },
            'passed': True
        },
    ]

    data = zip(data_filenames, [json.dumps(d) for d in _data])

    resolver_tag_filenames = [
        "CP_Satellite-2020-09-02-12-00-01/resolvers.json",
        "CP_Satellite-2020-09-02-12-00-01/tagged_resolvers.json"
    ]

    answer_tag_filenames = [
        "CP_Satellite-2020-09-02-12-00-01/tagged_answers.json",
        "CP_Satellite-2020-09-02-12-00-01/tagged_answers.json",
        "CP_Satellite-2020-09-02-12-00-01/tagged_answers.json",
        "CP_Satellite-2020-09-02-12-00-01/tagged_answers.json"
    ]

    # yapf: disable
    _resolver_tags = [{
        'name': 'special',
        'resolver': '1.1.1.3'
    }, {
        'resolver': '1.1.1.3',
        'country': 'United States'
    }]

    _answer_tags = [{
        'asname': 'AMAZON-02',
        'asnum': 16509,
        'cert': None,
        'http': 'c5ba7f2da503045170f1d66c3e9f84576d8f3a606bb246db589a8f62c65921af',
        'ip': '13.249.134.38'
    }, {
        'asname': 'AMAZON-02',
        'asnum': 16509,
        'cert':  None,
        'http': '256e35b8bace0e9fe95f308deb35f82117cd7317f90a08f181516c31abe95b71',
        'ip': '13.249.134.44'
    }, {
        'asname': 'AMAZON-02',
        'asnum': 16509,
        'cert': None,
        'http': '2054d0fd3887e0ded023879770d6cde57633b7881f609f1042d90fedf41685fe',
        'ip': '13.249.134.74'
    }, {
        'asname': 'AMAZON-02',
        'asnum': 16509,
        'cert': None,
        'http': '0509322329cdae79475531a019a3628aa52598caa0135c5534905f0c4b4f1bac',
        'ip': '13.249.134.89'
    }]

    resolver_tags = zip(resolver_tag_filenames, [json.dumps(t) for t in _resolver_tags])
    answer_tags = zip(answer_tag_filenames, [json.dumps(t) for t in _answer_tags])

    expected = [SatelliteRow(
        ip = '1.1.1.3',
        is_control_ip = False,
        domain = 'signal.org',
        is_control = False,
        controls_failed = False,
        category = 'Communication Tools',
        error = None,
        anomaly = False,
        success = True,
        rcode = 0,
        received = [SatelliteAnswer(
            ip = '13.249.134.38',
            cert = None,
            http = 'c5ba7f2da503045170f1d66c3e9f84576d8f3a606bb246db589a8f62c65921af',
            matches_control=MatchesControl(
                ip=True,
                http=True,
                asnum=True,
                asname=True,
            ),
            ip_metadata=IpMetadata(
                asn=16509,
                as_name='AMAZON-02'
            )
        ), SatelliteAnswer(
            ip = '13.249.134.44',
            cert = None,
            http = '256e35b8bace0e9fe95f308deb35f82117cd7317f90a08f181516c31abe95b71',
            matches_control=MatchesControl(
                ip=True,
                http=True,
                asnum=True,
                asname=True,
            ),
            ip_metadata=IpMetadata(
                asn=16509,
                as_name='AMAZON-02'
            )
        ), SatelliteAnswer(
            ip = '13.249.134.74',
            cert = None,
            http = '2054d0fd3887e0ded023879770d6cde57633b7881f609f1042d90fedf41685fe',
            matches_control=MatchesControl(
                ip=True,
                http=True,
                asnum=True,
                asname=True,
            ),
            ip_metadata=IpMetadata(
                asn=16509,
                as_name='AMAZON-02'
            )
        ), SatelliteAnswer(
            ip = '13.249.134.89',
            cert = None,
            http = '0509322329cdae79475531a019a3628aa52598caa0135c5534905f0c4b4f1bac',
            matches_control=MatchesControl(
                ip=True,
                http=True,
                asnum=True,
                asname=True,
            ),
            ip_metadata=IpMetadata(
                asn=16509,
                as_name='AMAZON-02'
            )
        )],
        date = '2020-09-02',
        source = 'CP_Satellite-2020-09-02-12-00-01',
        measurement_id = 'baa7c6c8ad7b5774832fb7537943c2b0',
        ip_metadata = IpMetadata(
            country = 'US',
            name = 'special',
        )
    ), SatelliteRow(
        ip = '1.1.1.3',
        is_control_ip = False,
        domain = 'adl.org',
        is_control = False,
        controls_failed = False,
        category = 'Religion',
        error = None,
        anomaly = False,
        success = True,
        rcode = 0,
        received = [SatelliteAnswer(
            ip = '192.124.249.107',
            matches_control=MatchesControl(
                ip=True,
            ),
        )],
        date = '2020-09-02',
        source = 'CP_Satellite-2020-09-02-12-00-01',
        measurement_id = '7a003222d96557a9ad2b658bc025ea79',
        ip_metadata = IpMetadata(
            country = 'US',
            name = 'special',
        )
    )]
    # yapf: enable

    with TestPipeline() as p:
      row_lines = p | 'create data' >> beam.Create(data)
      resolver_tag_lines = p | 'create resolver tags' >> beam.Create(
          resolver_tags)
      answer_tag_lines = p | 'create answer tags' >> beam.Create(answer_tags)

      final = process_satellite_with_tags(row_lines, answer_tag_lines,
                                          resolver_tag_lines)
      beam_test_util.assert_that(final, beam_test_util.equal_to(expected))

  def test_process_satellite_v2p0(self) -> None:
    """Test processing of Satellite v2 interference and tag files."""
    data_filenames = [
        "CP_Satellite-2021-03-01-12-00-01/results.json",
        "CP_Satellite-2021-03-01-12-00-01/results.json",
    ]

    # yapf: disable
    _data = [{
        "vp": "185.228.169.37",
        "location": {
            "country_code": "IE",
            "country_name": "Ireland"
        },
        "test_url": "ar.m.wikipedia.org",
        "response": {
            "198.35.26.96": ["cert", "asnum", "asname"],
            "198.35.26.86": ["cert", "asnum", "asname"],
            "rcode": ["-1", "-1", "0"],
            "error": [
              "read udp 141.212.123.185:51880->185.228.169.37:53: i/o timeout",
              "read udp 141.212.123.185:51430->185.228.169.37:53: i/o timeout"
            ]
        },
        "passed_control": True,
        "connect_error": False,
        "in_control_group": True,
        "anomaly": False,
        "confidence": {
            "average": 60,
            "matches": [60],
            "untagged_controls": False,
            "untagged_response": False
        },
        "start_time": "2021-03-01 12:43:25.3438285 -0500 EST m=+0.421998701",
        "end_time": "2021-03-01 12:43:25.3696119 -0500 EST m=+0.447782001"
    }, {
        "vp": "156.154.71.37",
        "location": {
            "country_code": "US",
            "country_name": "United States"
        },
        "test_url": "www.usacasino.com",
        "response": {
            "15.126.193.233": ["no_tags"],
            "rcode": ["2", "0"]
        },
        "passed_control": True,
        "connect_error": False,
        "in_control_group": True,
        "anomaly": True,
        "confidence": {
            "average": 0,
            "matches": [0],
            "untagged_controls": False,
            "untagged_response": True
        },
        "start_time": "2021-03-01 12:43:25.3438285 -0500 EST m=+0.421998701",
        "end_time": "2021-03-01 12:43:25.3696119 -0500 EST m=+0.447782001"
    }]
    # yapf: enable

    data = zip(data_filenames, [json.dumps(d) for d in _data])

    resolver_tag_filenames = [
        "CP_Satellite-2021-03-01-12-00-01/tagged_resolvers.json",
        "CP_Satellite-2021-03-01-12-00-01/tagged_resolvers.json",
        "CP_Satellite-2021-03-01-12-00-01/resolvers.json",
        "CP_Satellite-2021-03-01-12-00-01/resolvers.json"
    ]

    answer_tag_filenames = [
        "CP_Satellite-2021-03-01-12-00-01/tagged_responses.json",
        "CP_Satellite-2021-03-01-12-00-01/tagged_responses.json",
    ]

    # yapf: disable
    _resolver_tags = [{
        "location": {
            "country_code": "IE",
            "country_name": "Ireland"
        },
        "vp": "185.228.169.37"
    }, {
        "location": {
            "country_code": "US",
            "country_name": "United States"
        },
        "vp": "156.154.71.37"
    }, {
        "name": "rdns37b.ultradns.net.",
        "vp": "156.154.71.37"
    }, {
        "name": "customfilter37-dns2.cleanbrowsing.org.",
        "vp": "185.228.169.37"
    }]

    _answer_tags = [{
        "asname": "WIKIMEDIA",
        "asnum": 14907,
        "cert": "9eb21a74a3cf1ecaaf6b19253025b4ca38f182e9f1f3e7355ba3c3004d4b7a10",
        "http": "7b4b4d1bfb0a645c990f55557202f88be48e1eee0c10bdcc621c7b682bf7d2ca",
        "ip": "198.35.26.96"
    }, {"asname": "CRONON-AS Obermuensterstr. 9",
        "asnum": 25504,
        "cert": "ea6389b446002e14d21bd7fd39d4433a5356948a906634365299b79781b43e2b",
        "http": None,
        "ip": "185.228.169.37"  # Don't tag vps with received ip tags
    }]
    # yapf: enable

    resolver_tags = zip(resolver_tag_filenames,
                        [json.dumps(t) for t in _resolver_tags])
    answer_tags = zip(answer_tag_filenames,
                      [json.dumps(t) for t in _answer_tags])

    # yapf: disable
    expected = [SatelliteRow(
        ip = '185.228.169.37',
        is_control_ip = False,
        domain = 'ar.m.wikipedia.org',
        is_control = False,
        category = 'Culture',
        error = 'read udp 141.212.123.185:51880->185.228.169.37:53: i/o timeout',
        anomaly = False,
        success = True,
        controls_failed = False,
        received = [],
        rcode = -1,
        date = '2021-03-01',
        start_time = '2021-03-01T12:43:25.3438285-05:00',
        end_time = '2021-03-01T12:43:25.3696119-05:00',
        source = 'CP_Satellite-2021-03-01-12-00-01',
        measurement_id = 'faf6d14d2b765c1e8e22724c57230361',
        ip_metadata = IpMetadata(
            country = 'IE',
            name = 'customfilter37-dns2.cleanbrowsing.org.',
        )
    ), SatelliteRow(
        ip = '185.228.169.37',
        is_control_ip = False,
        domain = 'ar.m.wikipedia.org',
        is_control = False,
        category = 'Culture',
        error = 'read udp 141.212.123.185:51430->185.228.169.37:53: i/o timeout',
        anomaly = False,
        success = True,
        controls_failed = False,
        received = [],
        rcode = -1,
        date = '2021-03-01',
        start_time = '2021-03-01T12:43:25.3438285-05:00',
        end_time = '2021-03-01T12:43:25.3696119-05:00',
        source = 'CP_Satellite-2021-03-01-12-00-01',
        measurement_id = 'faf6d14d2b765c1e8e22724c57230361',
        ip_metadata = IpMetadata(
            country = 'IE',
            name = 'customfilter37-dns2.cleanbrowsing.org.',
        )
    ), SatelliteRow(
        ip = '185.228.169.37',
        is_control_ip = False,
        domain = 'ar.m.wikipedia.org',
        is_control = False,
        category = 'Culture',
        error = None,
        anomaly = False,
        success = True,
        controls_failed = False,
        received = [SatelliteAnswer(
            ip = '198.35.26.96',
            cert = '9eb21a74a3cf1ecaaf6b19253025b4ca38f182e9f1f3e7355ba3c3004d4b7a10',
            http = '7b4b4d1bfb0a645c990f55557202f88be48e1eee0c10bdcc621c7b682bf7d2ca',
            matches_control=MatchesControl(
                cert=True,
                asnum=True,
                asname=True,
            ),
            ip_metadata=IpMetadata(
                asn=14907,
                as_name='WIKIMEDIA'
            )
        ), SatelliteAnswer(
            ip = '198.35.26.86',
            matches_control=MatchesControl(
                cert=True,
                asnum=True,
                asname=True,
            )
        )],
        rcode = 0,
        date = '2021-03-01',
        start_time = '2021-03-01T12:43:25.3438285-05:00',
        end_time = '2021-03-01T12:43:25.3696119-05:00',
        source = 'CP_Satellite-2021-03-01-12-00-01',
        measurement_id = 'faf6d14d2b765c1e8e22724c57230361',
        ip_metadata = IpMetadata(
            country = 'IE',
            name = 'customfilter37-dns2.cleanbrowsing.org.',
        )
    ), SatelliteRow(
        ip = '156.154.71.37',
        is_control_ip = False,
        domain = 'www.usacasino.com',
        is_control = False,
        category = 'Gambling',
        error = None,
        anomaly = True,
        success = True,
        controls_failed = False,
        received = [],
        rcode = 2,
        date = '2021-03-01',
        start_time = '2021-03-01T12:43:25.3438285-05:00',
        end_time = '2021-03-01T12:43:25.3696119-05:00',
        source = 'CP_Satellite-2021-03-01-12-00-01',
        measurement_id = '180143e90b5e5082a21f440b4482e78c',
        ip_metadata = IpMetadata(
            country = 'US',
            name = 'rdns37b.ultradns.net.',
        )
    ), SatelliteRow(
        ip = '156.154.71.37',
        is_control_ip = False,
        domain = 'www.usacasino.com',
        is_control = False,
        category = 'Gambling',
        error = None,
        anomaly = True,
        success = True,
        controls_failed = False,
        received = [SatelliteAnswer(
            ip = '15.126.193.233',
            matches_control=MatchesControl()
        )],
        rcode = 0,
        date = '2021-03-01',
        start_time = '2021-03-01T12:43:25.3438285-05:00',
        end_time = '2021-03-01T12:43:25.3696119-05:00',
        source = 'CP_Satellite-2021-03-01-12-00-01',
        measurement_id = '180143e90b5e5082a21f440b4482e78c',
        ip_metadata = IpMetadata(
            country = 'US',
            name = 'rdns37b.ultradns.net.',
        )
    )]
    # yapf: enable

    with TestPipeline() as p:
      row_lines = p | 'create data' >> beam.Create(data)
      resolver_tag_lines = p | 'create resolver tags' >> beam.Create(
          resolver_tags)
      answer_tag_lines = p | 'create answer tags' >> beam.Create(answer_tags)

      final = process_satellite_with_tags(row_lines, answer_tag_lines,
                                          resolver_tag_lines)
      beam_test_util.assert_that(final, beam_test_util.equal_to(expected))

  def test_process_satellite_v2p1(self) -> None:
    """Test processing of Satellite v2p1 interference and tag files."""
    data_filenames = [
        "CP_Satellite-2021-04-18-12-00-01/results.json",
        "CP_Satellite-2021-04-18-12-00-01/results.json",
        "CP_Satellite-2021-04-18-12-00-01/responses_control.json",
        "CP_Satellite-2021-04-18-12-00-01/responses_control.json"
    ]

    # yapf: disable
    _data = [{
        "vp": "87.119.233.243",
        "location": {
            "country_name": "Russia",
            "country_code": "RU"
        },
        "test_url": "feedly.com",
        "response": {},
        "passed_control": False,
        "connect_error": True,
        "in_control_group": True,
        "anomaly": False,
        "confidence": {
            "average": 0,
            "matches": None,
            "untagged_controls": False,
            "untagged_response": False
        },
        "start_time": "2021-04-18 14:49:01.62448452 -0400 EDT m=+10140.555964129",
        "end_time": "2021-04-18 14:49:03.624563629 -0400 EDT m=+10142.556043238"
    }, {
        "vp": "12.5.76.236",
        "location": {
            "country_name": "United States",
            "country_code": "US"
        },
        "test_url": "ultimate-guitar.com",
        "response": {
            "rcode": ["2"]
        },
        "passed_control": True,
        "connect_error": False,
        "in_control_group": True,
        "anomaly": True,
        "confidence": {
            "average": 0,
            "matches": None,
            "untagged_controls": False,
            "untagged_response": False
        },
        "start_time": "2021-04-18 14:49:07.712972288 -0400 EDT m=+10146.644451890",
        "end_time": "2021-04-18 14:49:07.749265765 -0400 EDT m=+10146.680745375"
    }, {
        "vp": "64.6.65.6",
        "test_url": "ultimate-guitar.com",
        "response": [{
            "url": "a.root-servers.net",
            "has_type_a": True,
            "response": ["198.41.0.4"],
            "error": "null",
            "rcode": 0,
            "start_time": "2021-04-18 14:51:57.561175746 -0400 EDT m=+10316.492655353",
            "end_time": "2021-04-18 14:51:57.587097567 -0400 EDT m=+10316.518577181"
        }, {
            "url": "ultimate-guitar.com",
            "has_type_a": True,
            "response": ["178.18.22.152"],
            "error": "null",
            "rcode": 0,
            "start_time": "2021-04-18 14:51:57.587109091 -0400 EDT m=+10316.518588694",
            "end_time": "2021-04-18 14:51:57.61294601 -0400 EDT m=+10316.544425613"
        }],
        "passed_control": True,
        "connect_error": False
    }, {
        "vp": "64.6.65.6",
        "test_url": "www.awid.org",
        "response": [{
            "url": "a.root-servers.net",
            "has_type_a": True,
            "response": ["198.41.0.4"],
            "error": "null",
            "rcode": 0,
            "start_time": "2021-04-18 14:51:45.836310062 -0400 EDT m=+10304.767789664",
            "end_time": "2021-04-18 14:51:45.862080031 -0400 EDT m=+10304.793559633"
        }, {
            "url": "www.awid.org",
            "has_type_a": False,
            "response": None,
            "error": "read udp 141.212.123.185:39868->64.6.65.6:53: i/o timeout",
            "rcode": -1,
            "start_time": "2021-04-18 14:51:45.862091022 -0400 EDT m=+10304.793570624",
            "end_time": "2021-04-18 14:51:47.862170832 -0400 EDT m=+10306.793650435"
        }, {
            "url": "www.awid.org",
            "has_type_a": True,
            "response": ["204.187.13.189"],
            "error": "null",
            "rcode": 0,
            "start_time": "2021-04-18 14:51:47.862183185 -0400 EDT m=+10306.793662792",
            "end_time": "2021-04-18 14:51:48.162724942 -0400 EDT m=+10307.094204544"
        }],
        "passed_control": True,
        "connect_error": False
    }]
    # yapf: enable

    data = zip(data_filenames, [json.dumps(d) for d in _data])

    resolver_tag_filenames = [
        "CP_Satellite-2021-04-18-12-00-01/resolvers.json",
        "CP_Satellite-2021-04-18-12-00-01/resolvers.json",
        "CP_Satellite-2021-04-18-12-00-01/resolvers.json",
        "CP_Satellite-2021-04-18-12-00-01/resolvers.json"
    ]

    # yapf: disable
    _resolver_tags = [{
        "name": "87-119-233-243.saransk.ru.",
        "vp": "87.119.233.243"
    }, {
        "name": "ns1327.ztomy.com.",
        "vp": "12.5.76.236"
    }, {
        "name": "rec1pubns2.ultradns.net.",
        "vp": "64.6.65.6"
    }, {
        "name": "example.com",
        "vp": "204.187.13.189"  # Don't tag received ips with vp tags
    }]
    # yapf: enable

    resolver_tags = zip(resolver_tag_filenames,
                        [json.dumps(t) for t in _resolver_tags])

    # yapf: disable
    expected = [SatelliteRow(
        ip = '87.119.233.243',
        is_control_ip = False,
        domain = 'feedly.com',
        is_control = False,
        category = 'E-commerce',
        error = None,
        anomaly = False,
        success = False,
        controls_failed = True,
        received = [],
        date = '2021-04-18',
        start_time = '2021-04-18T14:49:01.62448452-04:00',
        end_time = '2021-04-18T14:49:03.624563629-04:00',
        source = 'CP_Satellite-2021-04-18-12-00-01',
        measurement_id = '0590413e5980581891beec5c6a80425c',
        ip_metadata = IpMetadata(
            country = 'RU',
            name = '87-119-233-243.saransk.ru.',
        )
    ), SatelliteRow(
        ip = '12.5.76.236',
        is_control_ip = False,
        domain = 'ultimate-guitar.com',
        is_control = False,
        category = 'History arts and literature',
        error = None,
        anomaly = True,
        success = True,
        controls_failed = False,
        received = [],
        rcode = 2,
        date = '2021-04-18',
        start_time = '2021-04-18T14:49:07.712972288-04:00',
        end_time = '2021-04-18T14:49:07.749265765-04:00',
        source = 'CP_Satellite-2021-04-18-12-00-01',
        measurement_id = 'f1c72aa0a7bd5368af0d3f28732da6fe',
        ip_metadata = IpMetadata(
            country = 'US',
            name = 'ns1327.ztomy.com.',
        )
    ), SatelliteRow(
        ip = '64.6.65.6',
        is_control_ip = True,
        domain = 'a.root-servers.net',
        is_control = True,
        category = 'Control',
        error = None,
        anomaly = None,
        success = True,
        controls_failed = False,
        has_type_a = True,
        received = [SatelliteAnswer(
            ip = '198.41.0.4'
        )],
        rcode = 0,
        date = '2021-04-18',
        start_time = '2021-04-18T14:51:57.561175746-04:00',
        end_time = '2021-04-18T14:51:57.587097567-04:00',
        source = 'CP_Satellite-2021-04-18-12-00-01',
        measurement_id = '6df37f5f0e9e58e58d3a6019880efafb',
        ip_metadata = IpMetadata(
            name = 'rec1pubns2.ultradns.net.',
        )
    ), SatelliteRow(
        ip = '64.6.65.6',
        is_control_ip = True,
        domain = 'ultimate-guitar.com',
        is_control = False,
        category = 'History arts and literature',
        error = None,
        anomaly = None,
        success = True,
        controls_failed = False,
        has_type_a = True,
        received = [SatelliteAnswer(
            ip = '178.18.22.152'
        )],
        rcode = 0,
        date = '2021-04-18',
        start_time = '2021-04-18T14:51:57.587109091-04:00',
        end_time = '2021-04-18T14:51:57.61294601-04:00',
        source = 'CP_Satellite-2021-04-18-12-00-01',
        measurement_id = '6df37f5f0e9e58e58d3a6019880efafb',
        ip_metadata = IpMetadata(
            name = 'rec1pubns2.ultradns.net.',
        )
    ), SatelliteRow(
        ip = '64.6.65.6',
        is_control_ip = True,
        domain = 'a.root-servers.net',
        is_control = True,
        category = 'Control',
        error = None,
        anomaly = None,
        success = True,
        controls_failed = False,
        has_type_a = True,
        received = [SatelliteAnswer(
            ip = '198.41.0.4'
        )],
        rcode = 0,
        date = '2021-04-18',
        start_time = '2021-04-18T14:51:45.836310062-04:00',
        end_time = '2021-04-18T14:51:45.862080031-04:00',
        source = 'CP_Satellite-2021-04-18-12-00-01',
        measurement_id = '2697a4f148a553f0bcdc4d6a67af5c56',
        ip_metadata = IpMetadata(
            name = 'rec1pubns2.ultradns.net.',
        )
    ), SatelliteRow(
        ip = '64.6.65.6',
        is_control_ip = True,
        domain = 'www.awid.org',
        is_control = False,
        category = 'Human Rights Issues',
        error = 'read udp 141.212.123.185:39868->64.6.65.6:53: i/o timeout',
        anomaly = None,
        success = True,
        controls_failed = False,
        has_type_a = False,
        received = [],
        rcode = -1,
        date = '2021-04-18',
        start_time = '2021-04-18T14:51:45.862091022-04:00',
        end_time = '2021-04-18T14:51:47.862170832-04:00',
        source = 'CP_Satellite-2021-04-18-12-00-01',
        measurement_id = '2697a4f148a553f0bcdc4d6a67af5c56',
        ip_metadata = IpMetadata(
            name = 'rec1pubns2.ultradns.net.',
        )
    ), SatelliteRow(
        ip = '64.6.65.6',
        is_control_ip = True,
        domain = 'www.awid.org',
        is_control = False,
        category = 'Human Rights Issues',
        error = None,
        anomaly = None,
        success = True,
        controls_failed = False,
        has_type_a = True,
        received = [SatelliteAnswer(
            ip = '204.187.13.189'
        )],
        rcode = 0,
        date = '2021-04-18',
        start_time = '2021-04-18T14:51:47.862183185-04:00',
        end_time = '2021-04-18T14:51:48.162724942-04:00',
        source = 'CP_Satellite-2021-04-18-12-00-01',
        measurement_id = '2697a4f148a553f0bcdc4d6a67af5c56',
        ip_metadata = IpMetadata(
            name = 'rec1pubns2.ultradns.net.',
        )
    )]
    # yapf: enable

    with TestPipeline() as p:
      row_lines = p | 'create data' >> beam.Create(data)
      resolver_tag_lines = p | 'create resolver tags' >> beam.Create(
          resolver_tags)
      answer_tag_lines = p | 'create answer tags' >> beam.Create([])

      final = process_satellite_with_tags(row_lines, answer_tag_lines,
                                          resolver_tag_lines)
      beam_test_util.assert_that(final, beam_test_util.equal_to(expected))

  def test_process_satellite_v2p2(self) -> None:
    """Test processing of Satellite v2p2 interference and tag files."""
    data_filenames = [
        "CP_Satellite-2021-10-20-12-00-01/results.json",
        "CP_Satellite-2021-10-20-12-00-01/results.json",
        "CP_Satellite-2021-10-20-12-00-01/results.json",
    ]

    # yapf: disable
    _data = [{
        "confidence": {
            "average": 100,
            "matches": [100,100],
            "untagged_controls": False,
            "untagged_response": False
        },
        "passed_liveness": True,
        "connect_error": False,
        "in_control_group": True,
        "anomaly": False,
        "excluded": False,
        "vp": "208.67.220.220",
        "test_url": "1337x.to",
        "start_time": "2021-10-20 14:51:41.297636661 -0400 EDT",
        "end_time": "2021-10-20 14:51:41.348612088 -0400 EDT",
        "exclude_reason": [],
        "location": {
            "country_name": "United States",
            "country_code": "US"
        },
        "response": [{
            "url": "1337x.to",
            "has_type_a": True,
            "response": {
                "104.31.16.11": {
                "http": "0728405c8a7cfa601fc6e8a0dff71038624dd672fbbfc91605905a536ff9e1a8",
                "cert": "",
                "asnum": 13335,
                "asname": "CLOUDFLARENET",
                "matched": ["ip","http","asnum","asname"]
                },
                "104.31.16.118": {
                "http": "2022c19b47cac1c5746f9d2efa5b7383f78c4bd1b4443f96e28f3a3019cc8ba0",
                "cert": "",
                "asnum": 13335,
                "asname": "CLOUDFLARENET",
                "matched": ["ip","http","asnum","asname"]
                }
            },
            "error": "null",
            "rcode": 0
            }
        ]
        }, {
        "confidence": {
            "average": 0,
            "matches": None,
            "untagged_controls": False,
            "untagged_response": False
        },
        "passed_liveness": True,
        "connect_error": False,
        "in_control_group": True,
        "anomaly": True,
        "excluded": False,
        "vp": "69.10.61.18",
        "test_url": "1922.gov.tw",
        "start_time": "2021-10-20 14:51:41.288634425 -0400 EDT",
        "end_time": "2021-10-20 14:51:41.506755185 -0400 EDT",
        "exclude_reason": [],
        "location": {
            "country_name": "United States",
            "country_code": "US"
        },
        "response": [
            {
            "url": "1922.gov.tw",
            "has_type_a": False,
            "response": {},
            "error": "null",
            "rcode": 2
            }
        ]
        }, {
        "confidence": {
            "average": 0,
            "matches": None,
            "untagged_controls": False,
            "untagged_response": False
        },
        "passed_liveness": False,
        "connect_error": True,
        "in_control_group": True,
        "anomaly": False,
        "excluded": False,
        "vp": "62.80.182.26",
        "test_url": "alibaba.com",
        "start_time": "2021-10-20 14:51:45.361175691 -0400 EDT",
        "end_time": "2021-10-20 14:51:46.261234037 -0400 EDT",
        "exclude_reason": [],
        "location": {
            "country_name": "Ukraine",
            "country_code": "UA"
        },
        "response": [
            {
            "url": "a.root-servers.net",
            "has_type_a": False,
            "response": {},
            "error": "read udp 141.212.123.185:30437->62.80.182.26:53: read: connection refused",
            "rcode": -1
            },
            {
            "url": "alibaba.com",
            "has_type_a": False,
            "response": {},
            "error": "read udp 141.212.123.185:23315->62.80.182.26:53: read: connection refused",
            "rcode": -1
            },
            {
            "url": "a.root-servers.net",
            "has_type_a": False,
            "response": {},
            "error": "read udp 141.212.123.185:53501->62.80.182.26:53: read: connection refused",
            "rcode": -1
            }
        ]
        }
    ]
    # yapf: enable

    data = zip(data_filenames, [json.dumps(d) for d in _data])

    resolver_tag_filenames = [
        "CP_Satellite-2021-10-20-12-00-01/resolvers.json",
        "CP_Satellite-2021-10-20-12-00-01/resolvers.json",
        "CP_Satellite-2021-10-20-12-00-01/resolvers.json",
    ]

    # yapf: disable
    _resolver_tags = [
        {"vp": "208.67.220.220",
         "name": "resolver2.opendns.com.",
         "location": {"country_name": "United States","country_code": "US"}},
        {"vp": "69.10.61.18",
         "name": "ns1.chpros.com.",
         "location": {"country_name": "United States","country_code": "US"}},
        {"vp": "62.80.182.26",
         "name": "mx4.orlantrans.com.",
         "location": {"country_name": "Ukraine","country_code": "UA"}}
    ]
    # yapf: enable

    resolver_tags = zip(resolver_tag_filenames,
                        [json.dumps(t) for t in _resolver_tags])

    # yapf: disable
    expected = [SatelliteRow(
        domain = '1337x.to',
        is_control = False,
        category = 'Media sharing',
        ip = '208.67.220.220',
        is_control_ip = False,
        date = '2021-10-20',
        start_time = '2021-10-20T14:51:41.297636661-04:00',
        end_time = '2021-10-20T14:51:41.348612088-04:00',
        error = None,
        anomaly = False,
        success = True,
        measurement_id = '6100ab464f425e3c9cdb52db36d45519',
        source = 'CP_Satellite-2021-10-20-12-00-01',
        controls_failed = False,
        average_confidence = 100,
        untagged_controls = False,
        untagged_response = False,
        excluded = False,
        exclude_reason = '',
        rcode = 0,
        has_type_a = True,
        received = [SatelliteAnswer(
            ip = '104.31.16.11',
            http = '0728405c8a7cfa601fc6e8a0dff71038624dd672fbbfc91605905a536ff9e1a8',
            cert = '',
            matches_control=MatchesControl(
                ip=True,
                http=True,
                asnum=True,
                asname=True,
            ),
            match_confidence = 100,
            ip_metadata=IpMetadata(
                asn=13335,
                as_name='CLOUDFLARENET'
            )
        ), SatelliteAnswer(
            ip = '104.31.16.118',
            http = '2022c19b47cac1c5746f9d2efa5b7383f78c4bd1b4443f96e28f3a3019cc8ba0',
            cert = '',
            matches_control=MatchesControl(
                ip=True,
                http=True,
                asnum=True,
                asname=True,
            ),
            match_confidence = 100,
            ip_metadata=IpMetadata(
                asn=13335,
                as_name='CLOUDFLARENET'
            )
        )],
        ip_metadata = IpMetadata(
            country = 'US',
            name = 'resolver2.opendns.com.',
        )
    ), SatelliteRow(
        received = [],
        domain = '1922.gov.tw',
        is_control = False,
        category = None,
        ip = '69.10.61.18',
        is_control_ip = False,
        date = '2021-10-20',
        start_time = '2021-10-20T14:51:41.288634425-04:00',
        end_time = '2021-10-20T14:51:41.506755185-04:00',
        error = None,
        anomaly = True,
        success = False,
        measurement_id = '6d419dab07565bad901aabcc8d2e0cc7',
        source = 'CP_Satellite-2021-10-20-12-00-01',
        controls_failed = False,
        average_confidence = None,
        untagged_controls = False,
        untagged_response = False,
        excluded = False,
        exclude_reason = '',
        rcode = 2,
        ip_metadata = IpMetadata(
            country = 'US',
            name = 'ns1.chpros.com.',
        )
    ), SatelliteRow(
        received = [],
        domain = 'a.root-servers.net',
        is_control =  True,
        category = 'Control',
        ip = '62.80.182.26',
        is_control_ip = False,
        date = '2021-10-20',
        start_time = '2021-10-20T14:51:45.361175691-04:00',
        end_time = '2021-10-20T14:51:46.261234037-04:00',
        error = 'read udp 141.212.123.185:30437->62.80.182.26:53: read: connection refused',
        anomaly = False,
        success = False,
        measurement_id = '47de079652ca53f7bdb57ca956e1c70e',
        source = 'CP_Satellite-2021-10-20-12-00-01',
        controls_failed = True,
        average_confidence = None,
        untagged_controls = False,
        untagged_response = False,
        excluded = False,
        exclude_reason = '',
        rcode = -1,
        ip_metadata = IpMetadata(
            country = 'UA',
            name = 'mx4.orlantrans.com.',
        )
    ), SatelliteRow(
        received = [],
        domain = 'alibaba.com',
        is_control = False,
        category = 'E-commerce',
        ip = '62.80.182.26',
        is_control_ip = False,
        date = '2021-10-20',
        start_time = '2021-10-20T14:51:45.361175691-04:00',
        end_time = '2021-10-20T14:51:46.261234037-04:00',
        error = 'read udp 141.212.123.185:23315->62.80.182.26:53: read: connection refused',
        anomaly = False,
        success = False,
        measurement_id = '47de079652ca53f7bdb57ca956e1c70e',
        source = 'CP_Satellite-2021-10-20-12-00-01',
        controls_failed = True,
        average_confidence = None,
        untagged_controls = False,
        untagged_response = False,
        excluded = False,
        exclude_reason = '',
        rcode = -1,
        ip_metadata = IpMetadata(
            country = 'UA',
            name = 'mx4.orlantrans.com.',
        )
    ), SatelliteRow(
        received = [],
        domain = 'a.root-servers.net',
        is_control = True,
        category = 'Control',
        ip = '62.80.182.26',
        is_control_ip = False,
        date = '2021-10-20',
        start_time = '2021-10-20T14:51:45.361175691-04:00',
        end_time = '2021-10-20T14:51:46.261234037-04:00',
        error = 'read udp 141.212.123.185:53501->62.80.182.26:53: read: connection refused',
        anomaly = False,
        success = False,
        measurement_id = '47de079652ca53f7bdb57ca956e1c70e',
        source = 'CP_Satellite-2021-10-20-12-00-01',
        controls_failed = True,
        average_confidence = None,
        untagged_controls = False,
        untagged_response = False,
        excluded = False,
        exclude_reason = '',
        rcode = -1,
        ip_metadata = IpMetadata(
            country = 'UA',
            name = 'mx4.orlantrans.com.',
        )
    )]
    # yapf: enable

    with TestPipeline() as p:
      row_lines = p | 'create data' >> beam.Create(data)
      resolver_tag_lines = p | 'create resolver tags' >> beam.Create(
          resolver_tags)
      answer_tag_lines = p | 'create answer tags' >> beam.Create([])

      final = process_satellite_with_tags(row_lines, answer_tag_lines,
                                          resolver_tag_lines)
      beam_test_util.assert_that(final, beam_test_util.equal_to(expected))

  def test_partition_satellite_input(self) -> None:
    """Test partitioning of Satellite input into tags, blockpages, and results."""
    data = [
        ("CP_Satellite-2020-09-02-12-00-01/resolvers.json", "resolver_tag"),
        ("CP_Satellite-2020-09-02-12-00-01/resolvers.json", "resolver_tag"),
        ("CP_Satellite-2020-09-02-12-00-01/tagged_resolvers.json",
         "resolver_tag"),
        ("CP_Satellite-2020-09-02-12-00-01/tagged_resolvers.json",
         "resolver_tag"),
        ("CP_Satellite-2020-09-02-12-00-01/tagged_answers.json", "answer_tag"),
        ("CP_Satellite-2020-09-02-12-00-01/tagged_answers.json", "answer_tag"),
        ("CP_Satellite-2021-09-02-12-00-01/blockpages.json", "blockpage"),
        ("CP_Satellite-2020-09-02-12-00-01/interference.json", "row"),
        ("CP_Satellite-2020-09-02-12-00-01/interference.json", "row")
    ]

    expected_resolver_tags = data[0:4]
    expected_answer_tags = data[4:6]
    expected_blockpages = data[6:7]
    expected_rows = data[7:]

    with TestPipeline() as p:
      lines = p | 'create data' >> beam.Create(data)

      answer_tags, resolver_tags, blockpages, rows = lines | beam.Partition(
          satellite.partition_satellite_input, 4)

      beam_test_util.assert_that(
          resolver_tags,
          beam_test_util.equal_to(expected_resolver_tags),
          label='assert_that/resolver_tags')
      beam_test_util.assert_that(
          answer_tags,
          beam_test_util.equal_to(expected_answer_tags),
          label='assert_that/answer_tags')
      beam_test_util.assert_that(
          blockpages,
          beam_test_util.equal_to(expected_blockpages),
          label='assert_that/blockpages')
      beam_test_util.assert_that(
          rows,
          beam_test_util.equal_to(expected_rows),
          label='assert_that/rows')

  def test_add_blockpages_to_answers(self) -> None:
    """Test adding blockpage info to Satellite Rows."""
    row_data = [
        SatelliteRow(
            domain="1337x.to",
            is_control=False,
            category="Media sharing",
            ip="8.8.8.8",
            is_control_ip=True,
            date="2022-01-02",
            start_time="2022-01-02T14:47:22.608859091-05:00",
            end_time="2022-01-02T14:47:22.987814778-05:00",
            error=None,
            anomaly=False,
            success=True,
            source="CP_Satellite-2022-01-02-12-00-01",
            controls_failed=False,
            rcode=0,
            average_confidence=100,
            untagged_controls=False,
            untagged_response=False,
            excluded=False,
            exclude_reason="",
            has_type_a=True,
            received=[
                SatelliteAnswer(
                    ip="104.31.16.11",
                    http=
                    "ecd1a8f3bd8db93d2d69e957cd3a114b43e8ba452d5cb2239f8eb6f6b92574ab",
                    cert="",
                    match_confidence=100,
                    ip_metadata=IpMetadata(asn=13335, as_name="CLOUDFLARENET")),
                SatelliteAnswer(
                    ip="104.31.16.118",
                    http=
                    "7255d6747fcfdc1c16a30c0da7f039571d8a1bdefe2f56fa0ca243fc684fbbb8",
                    cert="",
                    match_confidence=100,
                    ip_metadata=IpMetadata(asn=13335, as_name="CLOUDFLARENET"))
            ],
            ip_metadata=IpMetadata(country="US",))
    ]

    page_fetch_data = [
        PageFetchRow(
            domain='1337x.to',
            ip='104.31.16.11',
            date='2022-01-02',
            start_time='2022-01-02T21:07:55.814062725-04:00',
            end_time='2022-01-02T21:07:56.317107472-04:00',
            success=True,
            source='CP_Satellite-2022-01-02-12-00-01',
            https=False,
            received=HttpsResponse(
                status='302 Moved Temporarily',
                body=
                '<html>\r\n<head><title>302 Found</title></head>\r\n<body bgcolor=\"white\">\r\n<center><h1>302 Found</h1></center>\r\n<hr><center>nginx/1.10.3 (Ubuntu)</center>\r\n</body>\r\n</html>\r\n',
                headers=[
                    'Content-Length: 170', 'Content-Type: text/html',
                    'Date: Fri, 17 Sep 2021 01:07:56 GMT',
                    'Server: nginx/1.10.3 (Ubuntu)'
                ],
                is_known_blockpage=False,
                page_signature='r_fp_26')),
        PageFetchRow(
            domain='1337x.to',
            ip='104.31.16.11',
            date='2022-01-02',
            start_time='2022-01-02T21:07:55.814062725-04:00',
            end_time='2022-01-02T21:07:56.317107472-04:00',
            success=True,
            source='CP_Satellite-2022-01-02-12-00-01',
            https=True,
            received=HttpsResponse(
                is_known_blockpage=None,
                status=
                'Get \"https://104.31.16.11:443/\": tls: oversized record received with length 20527',
                page_signature=None)),
    ]

    expected_rows = [
        SatelliteRow(
            domain="1337x.to",
            is_control=False,
            category="Media sharing",
            ip="8.8.8.8",
            is_control_ip=True,
            date="2022-01-02",
            start_time="2022-01-02T14:47:22.608859091-05:00",
            end_time="2022-01-02T14:47:22.987814778-05:00",
            error=None,
            anomaly=False,
            success=True,
            source="CP_Satellite-2022-01-02-12-00-01",
            controls_failed=False,
            rcode=0,
            average_confidence=100,
            untagged_controls=False,
            untagged_response=False,
            excluded=False,
            exclude_reason="",
            has_type_a=True,
            received=[
                SatelliteAnswer(
                    ip="104.31.16.11",
                    http=
                    "ecd1a8f3bd8db93d2d69e957cd3a114b43e8ba452d5cb2239f8eb6f6b92574ab",
                    cert="",
                    ip_metadata=IpMetadata(asn=13335, as_name="CLOUDFLARENET"),
                    match_confidence=100,
                    http_response=HttpsResponse(
                        status='302 Moved Temporarily',
                        body=
                        '<html>\r\n<head><title>302 Found</title></head>\r\n<body bgcolor=\"white\">\r\n<center><h1>302 Found</h1></center>\r\n<hr><center>nginx/1.10.3 (Ubuntu)</center>\r\n</body>\r\n</html>\r\n',
                        headers=[
                            'Content-Length: 170', 'Content-Type: text/html',
                            'Date: Fri, 17 Sep 2021 01:07:56 GMT',
                            'Server: nginx/1.10.3 (Ubuntu)'
                        ],
                        is_known_blockpage=False,
                        page_signature='r_fp_26'),
                    https_response=HttpsResponse(
                        is_known_blockpage=None,
                        status=
                        'Get \"https://104.31.16.11:443/\": tls: oversized record received with length 20527',
                        page_signature=None)),
                SatelliteAnswer(
                    ip="104.31.16.118",
                    http=
                    "7255d6747fcfdc1c16a30c0da7f039571d8a1bdefe2f56fa0ca243fc684fbbb8",
                    cert="",
                    match_confidence=100,
                    ip_metadata=IpMetadata(asn=13335, as_name="CLOUDFLARENET"))
            ],
            ip_metadata=IpMetadata(country="US",))
    ]

    with TestPipeline() as p:
      rows = p | 'create data' >> beam.Create(row_data)
      page_fetches = p | 'create blockpages' >> beam.Create(page_fetch_data)

      final = satellite.add_page_fetch_to_answers(rows, page_fetches)
      beam_test_util.assert_that(final, beam_test_util.equal_to(expected_rows))

  # pylint: enable=protected-access


if __name__ == '__main__':
  unittest.main()
