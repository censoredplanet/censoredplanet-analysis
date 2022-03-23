"""Unit tests for satellite."""

import json
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
import apache_beam.testing.util as beam_test_util

from pipeline.metadata.schema import SatelliteRow, SatelliteAnswer, SatelliteAnswerMetadata, IpMetadataWithKeys, IpMetadata
from pipeline.metadata import satellite

# pylint: disable=too-many-lines


def _unset_measurement_id(row: SatelliteRow) -> SatelliteRow:
  row.measurement_id = ''
  return row


class SatelliteTest(unittest.TestCase):
  """Unit tests for satellite steps."""

  # pylint: disable=protected-access

  def test_make_date_ip_key(self) -> None:
    row = IpMetadataWithKeys(
        date='2020-01-01',
        ip='1.2.3.4',
    )
    self.assertEqual(satellite.make_date_ip_key(row), ('2020-01-01', '1.2.3.4'))

  def test_read_satellite_resolver_tags(self) -> None:  # pylint: disable=no-self-use
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

    tag1 = IpMetadataWithKeys(
        ip='1.1.1.1',
        date='2020-12-17',
        country='US')
    tag2 = IpMetadataWithKeys(
        ip='1.1.1.3',
        date='2020-12-17',
        country='AU')
    # yapf: enable

    expected_resolver_tags = [tag1, tag2]

    with TestPipeline() as p:
      lines = p | 'create tags' >> beam.Create(data)

      resolver_tags = lines | 'read resolver tags' >> beam.FlatMapTuple(
          satellite._read_satellite_resolver_tags)

      beam_test_util.assert_that(
          resolver_tags, beam_test_util.equal_to(expected_resolver_tags))

  def test_read_satellite_answer_tags(self) -> None:  # pylint: disable=no-self-use
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

    tag1 = SatelliteAnswerMetadata(
      ip='60.210.17.137',
      date='2020-12-17',
      asname='CHINA169-BACKBONE CHINA UNICOM China169 Backbone',
      asnum=4837,
      cert='a2fed117238c94a04ba787cfe69e93de36cc8571bab44d5481df9becb9beec75',
      http='e3c1d34ca489928190b45f0535624b872717d1edd881c8ab4b2c62f898fcd4a5'
    )
    # yapf: enable
    expected_answer_tags = [tag1]

    with TestPipeline() as p:
      lines = p | 'create tags' >> beam.Create(data)

      answer_tags = lines | 'read answer tags' >> beam.FlatMapTuple(
          satellite._read_satellite_answer_tags)

      beam_test_util.assert_that(answer_tags,
                                 beam_test_util.equal_to(expected_answer_tags))

  def test_process_satellite_v1(self) -> None:  # pylint: disable=no-self-use
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
            asname = 'AMAZON-02',
            asnum = 16509,
            cert = None,
            http = 'c5ba7f2da503045170f1d66c3e9f84576d8f3a606bb246db589a8f62c65921af',
            matches_control = 'ip http asnum asname'
        ), SatelliteAnswer(
            ip = '13.249.134.44',
            asname = 'AMAZON-02',
            asnum = 16509,
            cert = None,
            http = '256e35b8bace0e9fe95f308deb35f82117cd7317f90a08f181516c31abe95b71',
            matches_control = 'ip http asnum asname'
        ), SatelliteAnswer(
            ip = '13.249.134.74',
            asname = 'AMAZON-02',
            asnum = 16509,
            cert = None,
            http = '2054d0fd3887e0ded023879770d6cde57633b7881f609f1042d90fedf41685fe',
            matches_control = 'ip http asnum asname'
        ), SatelliteAnswer(
            ip = '13.249.134.89',
            asname = 'AMAZON-02',
            asnum = 16509,
            cert = None,
            http = '0509322329cdae79475531a019a3628aa52598caa0135c5534905f0c4b4f1bac',
            matches_control = 'ip http asnum asname'
        )],
        date = '2020-09-02',
        source = 'CP_Satellite-2020-09-02-12-00-01',
        measurement_id = '',
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
            matches_control = 'ip'
        )],
        date = '2020-09-02',
        source = 'CP_Satellite-2020-09-02-12-00-01',
        measurement_id = '',
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

      tagged = satellite.process_satellite_with_tags(row_lines,
                                                     answer_tag_lines,
                                                     resolver_tag_lines)
      # Measurement ids are random and can't be tested
      final = tagged | 'unset measurement ids' >> beam.Map(
          _unset_measurement_id)

      beam_test_util.assert_that(final, beam_test_util.equal_to(expected))

  def test_process_satellite_v2p0(self) -> None:  # pylint: disable=no-self-use
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
        measurement_id = '',
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
        measurement_id = '',
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
            asname = 'WIKIMEDIA',
            asnum = 14907,
            cert = '9eb21a74a3cf1ecaaf6b19253025b4ca38f182e9f1f3e7355ba3c3004d4b7a10',
            http = '7b4b4d1bfb0a645c990f55557202f88be48e1eee0c10bdcc621c7b682bf7d2ca',
            matches_control = 'cert asnum asname'
        ), SatelliteAnswer(
            ip = '198.35.26.86',
            matches_control = 'cert asnum asname'
        )],
        rcode = 0,
        date = '2021-03-01',
        start_time = '2021-03-01T12:43:25.3438285-05:00',
        end_time = '2021-03-01T12:43:25.3696119-05:00',
        source = 'CP_Satellite-2021-03-01-12-00-01',
        measurement_id = '',
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
        measurement_id = '',
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
            matches_control = ''
        )],
        rcode = 0,
        date = '2021-03-01',
        start_time = '2021-03-01T12:43:25.3438285-05:00',
        end_time = '2021-03-01T12:43:25.3696119-05:00',
        source = 'CP_Satellite-2021-03-01-12-00-01',
        measurement_id = '',
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

      tagged = satellite.process_satellite_with_tags(row_lines,
                                                     answer_tag_lines,
                                                     resolver_tag_lines)
      # Measurement ids are random and can't be tested
      final = tagged | 'unset measurement ids' >> beam.Map(
          _unset_measurement_id)

      beam_test_util.assert_that(final, beam_test_util.equal_to(expected))

  def test_process_satellite_v2p1(self) -> None:  # pylint: disable=no-self-use
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
        measurement_id = '',
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
        measurement_id = '',
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
        measurement_id = '',
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
        measurement_id = '',
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
        measurement_id = '',
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
        measurement_id = '',
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
        measurement_id = '',
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

      tagged = satellite.process_satellite_with_tags(row_lines,
                                                     answer_tag_lines,
                                                     resolver_tag_lines)
      # Measurement ids are random and can't be tested
      final = tagged | 'unset measurement ids' >> beam.Map(
          _unset_measurement_id)

      beam_test_util.assert_that(final, beam_test_util.equal_to(expected))

  def test_process_satellite_v2p2(self) -> None:  # pylint: disable=no-self-use
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
        measurement_id = '',
        source = 'CP_Satellite-2021-10-20-12-00-01',
        controls_failed = False,
        average_confidence = 100,
        matches_confidence = [100, 100],
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
            asname = 'CLOUDFLARENET',
            asnum = 13335,
            matches_control = 'ip http asnum asname'
        ), SatelliteAnswer(
            ip = '104.31.16.118',
            http = '2022c19b47cac1c5746f9d2efa5b7383f78c4bd1b4443f96e28f3a3019cc8ba0',
            cert = '',
            asname = 'CLOUDFLARENET',
            asnum = 13335,
            matches_control = 'ip http asnum asname'
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
        measurement_id = '',
        source = 'CP_Satellite-2021-10-20-12-00-01',
        controls_failed = False,
        average_confidence = None,
        matches_confidence = [],
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
        measurement_id = '',
        source = 'CP_Satellite-2021-10-20-12-00-01',
        controls_failed = True,
        average_confidence = None,
        matches_confidence = [],
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
        measurement_id = '',
        source = 'CP_Satellite-2021-10-20-12-00-01',
        controls_failed = True,
        average_confidence = None,
        matches_confidence = [],
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
        measurement_id = '',
        source = 'CP_Satellite-2021-10-20-12-00-01',
        controls_failed = True,
        average_confidence = None,
        matches_confidence = [],
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

      tagged = satellite.process_satellite_with_tags(row_lines,
                                                     answer_tag_lines,
                                                     resolver_tag_lines)
      # Measurement ids are random and can't be tested
      final = tagged | 'unset measurement ids' >> beam.Map(
          _unset_measurement_id)

      beam_test_util.assert_that(final, beam_test_util.equal_to(expected))

  def test_partition_satellite_input(self) -> None:  # pylint: disable=no-self-use
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

  def test_calculate_confidence(self) -> None:
    """Test calculating the confidence metrics for Satellite v1 data."""
    # yapf: disable
    scans = [
      SatelliteRow(
        ip = '114.114.114.110',
        domain = 'abs-cbn.com',
        category = 'Culture',
        error = None,
        anomaly = True,
        success = True,
        received = [SatelliteAnswer(
            ip = '104.20.161.134',
            matches_control = ''
        )],
        date = '2020-09-02',
        ip_metadata = IpMetadata(
            country = 'CN',
            name = 'name',
        )
      ), SatelliteRow(
        ip = '1.1.1.3',
        domain = 'signal.org',
        category = 'Communication Tools',
        error = None,
        anomaly = False,
        success = True,
        received = [
            SatelliteAnswer(
                ip = '13.249.134.38',
                asname = 'AMAZON-02',
                asnum = 16509,
                cert = None,
                http = 'c5ba7f2da503045170f1d66c3e9f84576d8f3a606bb246db589a8f62c65921af',
                matches_control = 'ip http asnum asname'),
            SatelliteAnswer(
                ip = '13.249.134.44',
                asname = 'AMAZON-02',
                asnum = 16509,
                cert = None,
                http = '256e35b8bace0e9fe95f308deb35f82117cd7317f90a08f181516c31abe95b71',
                matches_control = 'ip http asnum asname'),
            SatelliteAnswer(
                ip = '13.249.134.74',
                asname = 'AMAZON-02',
                asnum = 16509,
                cert = None,
                http = '2054d0fd3887e0ded023879770d6cde57633b7881f609f1042d90fedf41685fe',
                matches_control = 'ip http asnum asname'),
            SatelliteAnswer(
                ip = '13.249.134.89',
                asname = 'AMAZON-02',
                asnum = 16509,
                cert = None,
                http = '0509322329cdae79475531a019a3628aa52598caa0135c5534905f0c4b4f1bac',
                matches_control = 'ip http asnum asname')
        ],
        date = '2020-09-02',
        ip_metadata = IpMetadata(
            country = 'US',
            name = 'special',
        )
      ), SatelliteRow(
        ip = '1.1.1.3',
        domain = 'signal.org',
        category = 'Communication Tools',
        error = None,
        anomaly = False,
        success = True,
        received = [
            SatelliteAnswer(
                ip = '13.249.134.38',
                asname = 'AS1',
                asnum = 11111,
                cert = None,
                http = 'c5ba7f2da503045170f1d66c3e9f84576d8f3a606bb246db589a8f62c65921af',
                matches_control = ''),
            SatelliteAnswer(
                ip = '13.249.134.44',
                asname = 'AS2',
                asnum = 22222,
                cert = 'cert',
                http = '256e35b8bace0e9fe95f308deb35f82117cd7317f90a08f181516c31abe95b71',
                matches_control = 'asnum asname'),
            SatelliteAnswer(
                ip = '13.249.134.74',
                asname = 'AS2',
                asnum = 22222,
                cert = None,
                http = '2054d0fd3887e0ded023879770d6cde57633b7881f609f1042d90fedf41685fe',
                matches_control = 'ip http asnum asname'),
            SatelliteAnswer(
                ip = '13.249.134.89',
                asname = 'AS2',
                asnum = 22222,
                cert = None,
                http = '0509322329cdae79475531a019a3628aa52598caa0135c5534905f0c4b4f1bac',
                matches_control = 'ip http asnum asname')
        ],
        date = '2020-09-02',
        ip_metadata = IpMetadata(
            country = 'US',
            name = 'special',
        )
      )
    ]

    expected = [
      (0, [0], False, True),
      (100, [100, 100, 100, 100], False, False),
      (62.5, [0, 50, 100, 100], False, False)
    ]
    # yapf: enable

    result = []
    for scan in scans:
      scan = satellite._calculate_confidence(scan, 1)
      result.append((scan.average_confidence, scan.matches_confidence,
                     scan.untagged_controls, scan.untagged_response))

    self.assertListEqual(result, expected)

  def test_verify(self) -> None:
    """Test verification of Satellite v1 data."""
    # yapf: disable
    scans = [
      SatelliteRow(
        ip = '114.114.114.110',
        domain = 'abs-cbn.com',
        category = 'Culture',
        error = None,
        anomaly = True,
        success = True,
        received = [SatelliteAnswer(
            ip = '104.20.161.134',
            matches_control = ''
        )],
        date = '2020-09-02',
        ip_metadata = IpMetadata(
            country = 'CN',
            name = 'name',
        )
      ), SatelliteRow(
        ip = '1.1.1.3',
        domain = 'signal.org',
        category = 'Communication Tools',
        error = None,
        anomaly = True,
        success = True,
        received = [
            SatelliteAnswer(
                ip = '13.249.134.38',
                asname = 'AMAZON-02',
                asnum = 16509,
                cert = None,
                http = 'c5ba7f2da503045170f1d66c3e9f84576d8f3a606bb246db589a8f62c65921af',
                matches_control = ''),
            SatelliteAnswer(
                ip = '13.249.134.44',
                asname = 'AMAZON-02',
                asnum = 16509,
                cert = None,
                http = '256e35b8bace0e9fe95f308deb35f82117cd7317f90a08f181516c31abe95b71',
                matches_control = ''),
        ],
        date = '2020-09-02',
        ip_metadata = IpMetadata(
            country = 'US',
            name = 'special',
        )
      ),
    ]
    # yapf: enable

    expected = [
        # answer IP is returned for multiple domains: likely to be interference
        (False, ''),
        # answer IPs are CDN: False positive
        (True, 'is_CDN is_CDN'),
    ]
    result = []
    for scan in scans:
      scan = satellite._verify(scan)
      result.append((scan.excluded, scan.exclude_reason))

    self.assertListEqual(result, expected)

  def test_postprocessing_satellite_v2p2(self) -> None:  # pylint: disable=no-self-use
    """Test postprocessing on Satellite v2.2 data."""
    # yapf: disable
    data = [
        SatelliteRow(
            domain = "1337x.to",
            is_control = False,
            category = "Media sharing",
            ip = "8.8.8.8",
            is_control_ip = True,
            date = "2022-01-02",
            start_time = "2022-01-02T14:47:22.608859091-05:00",
            end_time = "2022-01-02T14:47:22.987814778-05:00",
            error = None,
            anomaly = False,
            success = True,
            source = "CP_Satellite-2022-01-02-12-00-01",
            controls_failed = False,
            rcode = 0,
            average_confidence = 100,
            matches_confidence = [100,100],
            untagged_controls = False,
            untagged_response = False,
            excluded = False,
            exclude_reason = "",
            has_type_a = True,
            received = [
                SatelliteAnswer(
                    ip = "104.31.16.11",
                    http = "ecd1a8f3bd8db93d2d69e957cd3a114b43e8ba452d5cb2239f8eb6f6b92574ab",
                    cert = "",
                    asname = "CLOUDFLARENET",
                    asnum = 13335,
                    matches_control = "ip http asnum asname"
                ),
                SatelliteAnswer(
                    ip = "104.31.16.118",
                    http = "7255d6747fcfdc1c16a30c0da7f039571d8a1bdefe2f56fa0ca243fc684fbbb8",
                    cert = "",
                    asname = "CLOUDFLARENET",
                    asnum = 13335,
                    matches_control = "ip http asnum asname"
                )
            ],
            ip_metadata = IpMetadata(
                country = "US",
            )
        ), SatelliteRow(
            domain = "1337x.to",
            is_control = False,
            category = "Media sharing",
            ip = "8.8.4.4",
            is_control_ip = True,
            date = "2022-01-02",
            start_time = "2022-01-02T14:47:22.609624461-05:00",
            end_time = "2022-01-02T14:47:22.98110208-05:00",
            error = None,
            anomaly = False,
            success = True,
            source = "CP_Satellite-2022-01-02-12-00-01",
            controls_failed = True,
            rcode = 0,
            average_confidence = 0,
            matches_confidence = [],
            untagged_controls = False,
            untagged_response = False,
            excluded = False,
            exclude_reason = "",
            has_type_a = True,
            received = [
                SatelliteAnswer(
                    ip = "104.31.16.11",
                    http = "ecd1a8f3bd8db93d2d69e957cd3a114b43e8ba452d5cb2239f8eb6f6b92574ab",
                    cert = "",
                    asname = "CLOUDFLARENET",
                    asnum = 13335,
                    matches_control = ""
                ),
                SatelliteAnswer(
                    ip = "104.31.16.118",
                    http = "7255d6747fcfdc1c16a30c0da7f039571d8a1bdefe2f56fa0ca243fc684fbbb8",
                    cert = "",
                    asname = "CLOUDFLARENET",
                    asnum = 13335,
                    matches_control = ""
                )
            ],
            ip_metadata = IpMetadata(
                country = "US",
            )
        ), SatelliteRow(
            domain = "1337x.to",
            is_control = False,
            category = "Media sharing",
            ip = "64.6.64.6",
            is_control_ip = True,
            date = "2022-01-02",
            start_time = "2022-01-02T16:41:54.579216934-05:00",
            end_time = "2022-01-02T16:41:54.617330171-05:00",
            error = None,
            anomaly = False,
            success = True,
            source = "CP_Satellite-2022-01-02-12-00-01",
            controls_failed = False,
            rcode = 0,
            average_confidence = 100,
            matches_confidence = [100,100],
            untagged_controls = False,
            untagged_response = False,
            excluded = False,
            exclude_reason = "",
            has_type_a = True,
            received = [
                SatelliteAnswer(
                    ip = "104.31.16.11",
                    http = "ecd1a8f3bd8db93d2d69e957cd3a114b43e8ba452d5cb2239f8eb6f6b92574ab",
                    cert = "",
                    asname = "CLOUDFLARENET",
                    asnum = 13335,
                    matches_control = "ip http asnum asname"
                ),
                SatelliteAnswer(
                    ip = "104.31.16.118",
                    http = "7255d6747fcfdc1c16a30c0da7f039571d8a1bdefe2f56fa0ca243fc684fbbb8",
                    cert = "",
                    asname = "CLOUDFLARENET",
                    asnum = 13335,
                    matches_control = "ip http asnum asname"
                )
            ],
            ip_metadata = IpMetadata(
                country = "US",
            )
        ), SatelliteRow(
            domain = "1337x.to",
            is_control = False,
            category = "Media sharing",
            ip = "64.6.65.6",
            is_control_ip = True,
            date = "2022-01-02",
            start_time = "2022-01-02T15:08:04.399147076-05:00",
            end_time = "2022-01-02T15:08:04.437950734-05:00",
            error = None,
            anomaly = False,
            success = True,
            source = "CP_Satellite-2022-01-02-12-00-01",
            controls_failed = False,
            rcode = 0,
            average_confidence = 100,
            matches_confidence = [100,100],
            untagged_controls = False,
            untagged_response = False,
            excluded = False,
            exclude_reason = "",
            has_type_a = True,
            received = [
                SatelliteAnswer(
                    ip = "104.31.16.11",
                    http = "ecd1a8f3bd8db93d2d69e957cd3a114b43e8ba452d5cb2239f8eb6f6b92574ab",
                    cert = "",
                    asname = "CLOUDFLARENET",
                    asnum = 13335,
                    matches_control = "ip http asnum asname"
                ),
                SatelliteAnswer(
                    ip = "104.31.16.118",
                    http = "7255d6747fcfdc1c16a30c0da7f039571d8a1bdefe2f56fa0ca243fc684fbbb8",
                    cert = "",
                    asname = "CLOUDFLARENET",
                    asnum = 13335,
                    matches_control = "ip http asnum asname"
                )
            ],
            ip_metadata = IpMetadata(
                country = "US",
            )
        ), SatelliteRow(
            domain = "1337x.to",
            is_control = False,
            category = "Media sharing",
            ip = "77.247.174.150",
            is_control_ip = False,
            date = "2022-01-02",
            start_time = "2022-01-02T14:47:22.708705995-05:00",
            end_time = "2022-01-02T14:47:22.983863812-05:00",
            error = None,
            anomaly = True,
            success = True,
            source = "CP_Satellite-2022-01-02-12-00-01",
            controls_failed = False,
            rcode = 0,
            average_confidence = 0,
            matches_confidence = [0],
            untagged_controls = False,
            untagged_response = False,
            excluded = False,
            exclude_reason = "",
            has_type_a = True,
            received = [
                SatelliteAnswer(
                    ip = "188.186.157.49",
                    http = "177a8341782a57778766a7334d3e99ecb61ce54bbcc48838ddda846ea076726d",
                    cert = "",
                    asname = "ERTELECOM-DC-AS",
                    asnum = 31483,
                    matches_control = ""
                )
            ],
            ip_metadata = IpMetadata(
                country = "RU",
            )
        )
    ]
    # yapf: enable

    # Data contains 4 measurements to control resolvers and
    # 1 measurement to a test resolver for the same domain.
    # v2.2 already has the confidence and verification fields in the raw data and
    # here both the control resolver and test resolver measurements have tags,
    # so we expect that postprocessing will not change the data
    # (confidence, verification, or the anomaly field).

    with TestPipeline() as p:
      rows = p | 'create data' >> beam.Create(data)

      final = satellite.post_processing_satellite(rows)
      beam_test_util.assert_that(final, beam_test_util.equal_to(data))

  # pylint: enable=protected-access


if __name__ == '__main__':
  unittest.main()
