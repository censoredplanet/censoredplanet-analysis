"""Unit tests for satellite."""

from typing import Dict, List
import unittest
import json

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
import apache_beam.testing.util as beam_test_util

from pipeline.metadata import satellite
from pipeline.metadata import flatten


class SatelliteTest(unittest.TestCase):
  """Unit tests for satellite steps."""

  # pylint: disable=protected-access

  def test_make_date_ip_key(self) -> None:
    row = {'date': '2020-01-01', 'ip': '1.2.3.4', 'other_field': None}
    self.assertEqual(satellite.make_date_ip_key(row), ('2020-01-01', '1.2.3.4'))

  def test_read_satellite_tags(self) -> None:
    """Test reading rows from Satellite tag files."""
    tagged_resolver1 = {'resolver': '1.1.1.1', 'country': 'United States'}
    tagged_resolver2 = {'resolver': '1.1.1.3', 'country': 'Australia'}
    # yapf: disable
    tagged_answer1 = {
      'ip': '60.210.17.137',
      'asname': 'CHINA169-BACKBONE CHINA UNICOM China169 Backbone',
      'asnum': 4837,
      'cert': 'a2fed117238c94a04ba787cfe69e93de36cc8571bab44d5481df9becb9beec75',
      'http': 'e3c1d34ca489928190b45f0535624b872717d1edd881c8ab4b2c62f898fcd4a5'
    }

    row1 = {'ip': '1.1.1.1', 'date': '2020-12-17', 'country': 'US'}
    row2 = {'ip': '1.1.1.3', 'date': '2020-12-17', 'country': 'AU'}
    row3 = {
      'ip': '60.210.17.137',
      'date': '2020-12-17',
      'asname': 'CHINA169-BACKBONE CHINA UNICOM China169 Backbone',
      'asnum': 4837,
      'cert': 'a2fed117238c94a04ba787cfe69e93de36cc8571bab44d5481df9becb9beec75',
      'http': 'e3c1d34ca489928190b45f0535624b872717d1edd881c8ab4b2c62f898fcd4a5'
    }
    # yapf: enable

    data = [
        json.dumps(tagged_resolver1),
        json.dumps(tagged_resolver2),
        json.dumps(tagged_answer1)
    ]
    expected = [row1, row2, row3]
    result = [
        next(satellite._read_satellite_tags('2020-12-17', d)) for d in data
    ]
    self.assertListEqual(result, expected)

  def test_process_satellite_v1(self) -> None:  # pylint: disable=no-self-use
    """Test processing of Satellite v1 interference and tag files."""
    # yapf: disable
    _data = [
      ("CP_Satellite-2020-09-02-12-00-01/interference.json", {'resolver': '1.1.1.3','query': 'signal.org', 'answers': {'13.249.134.38': ['ip', 'http', 'asnum', 'asname'], '13.249.134.44': ['ip', 'http', 'asnum', 'asname'],'13.249.134.74': ['ip', 'http', 'asnum', 'asname'], '13.249.134.89': ['ip', 'http', 'asnum', 'asname']}, 'passed': True}),
      ("CP_Satellite-2020-09-02-12-00-01/interference.json", {'resolver': '1.1.1.3','query': 'adl.org', 'answers': {'192.124.249.107': ['ip', 'no_tags']}, 'passed': True}),
    ]

    data = [(filename, json.dumps(d)) for filename, d in _data]

    _tags = [
        ("CP_Satellite-2020-09-02-12-00-01/resolvers.json", {'name': 'special','resolver': '1.1.1.3'}),
        ("CP_Satellite-2020-09-02-12-00-01/tagged_resolvers.json", {'resolver': '1.1.1.3', 'country': 'United States'}),
        ("CP_Satellite-2020-09-02-12-00-01/tagged_answers.json", {'asname': 'AMAZON-02','asnum': 16509,'cert': None,'http': 'c5ba7f2da503045170f1d66c3e9f84576d8f3a606bb246db589a8f62c65921af','ip': '13.249.134.38'}),
        ("CP_Satellite-2020-09-02-12-00-01/tagged_answers.json", {'asname': 'AMAZON-02','asnum': 16509,'cert': None,'http': '256e35b8bace0e9fe95f308deb35f82117cd7317f90a08f181516c31abe95b71','ip': '13.249.134.44'}),
        ("CP_Satellite-2020-09-02-12-00-01/tagged_answers.json", {'asname': 'AMAZON-02','asnum': 16509,'cert': None,'http': '2054d0fd3887e0ded023879770d6cde57633b7881f609f1042d90fedf41685fe','ip': '13.249.134.74'}),
        ("CP_Satellite-2020-09-02-12-00-01/tagged_answers.json", {'asname': 'AMAZON-02','asnum': 16509,'cert': None,'http': '0509322329cdae79475531a019a3628aa52598caa0135c5534905f0c4b4f1bac','ip': '13.249.134.89'})
    ]

    tags = [(filename, json.dumps(t)) for filename, t in _tags]

    expected = [
        {
          'ip': '1.1.1.3',
          'country': 'US',
          'name': 'special',
          'domain': 'signal.org',
          'category': 'Communication Tools',
          'error': None,
          'anomaly': False,
          'success': True,
          'rcode': ['0'],
          'received': [
              {'ip': '13.249.134.38', 'asname': 'AMAZON-02','asnum': 16509,'cert': None,'http': 'c5ba7f2da503045170f1d66c3e9f84576d8f3a606bb246db589a8f62c65921af', 'matches_control': 'ip http asnum asname'},
              {'ip': '13.249.134.44', 'asname': 'AMAZON-02','asnum': 16509,'cert': None,'http': '256e35b8bace0e9fe95f308deb35f82117cd7317f90a08f181516c31abe95b71', 'matches_control': 'ip http asnum asname'},
              {'ip': '13.249.134.74', 'asname': 'AMAZON-02','asnum': 16509,'cert': None,'http': '2054d0fd3887e0ded023879770d6cde57633b7881f609f1042d90fedf41685fe', 'matches_control': 'ip http asnum asname'},
              {'ip': '13.249.134.89', 'asname': 'AMAZON-02','asnum': 16509,'cert': None,'http': '0509322329cdae79475531a019a3628aa52598caa0135c5534905f0c4b4f1bac', 'matches_control': 'ip http asnum asname'}
          ],
          'date': '2020-09-02'
        },
        {
          'ip': '1.1.1.3',
          'country': 'US',
          'name': 'special',
          'domain': 'adl.org',
          'category': 'Religion',
          'error': None,
          'anomaly': False,
          'success': True,
          'rcode': ['0'],
          'received': [
              {'ip': '192.124.249.107', 'matches_control': 'ip'}
          ],
          'date': '2020-09-02'
        }
    ]
    # yapf: enable

    with TestPipeline() as p:
      lines = p | 'create data' >> beam.Create(data)
      lines2 = p | 'create tags' >> beam.Create(tags)

      final = satellite.process_satellite_with_tags(lines, lines2)
      beam_test_util.assert_that(final, beam_test_util.equal_to(expected))

  def test_process_satellite_v2(self) -> None:  # pylint: disable=no-self-use
    """Test processing of Satellite v2 interference and tag files."""
    # yapf: disable
    data = [
      ("CP_Satellite-2021-03-01-12-00-01/results.json", """{"vp":"185.228.169.37","location":{"country_code":"IE","country_name":"Ireland"},"test_url":"ar.m.wikipedia.org","response":{"198.35.26.96":["cert","asnum","asname"],"rcode":["0","0","0"]},"passed_control":true,"connect_error":false,"in_control_group":true,"anomaly":false,"confidence":{"average":60,"matches":[60],"untagged_controls":false,"untagged_response":false},"start_time":"2021-03-01 12:43:25.3438285 -0500 EST m=+0.421998701","end_time":"2021-03-01 12:43:25.3696119 -0500 EST m=+0.447782001"}"""),
      ("CP_Satellite-2021-03-01-12-00-01/results.json", """{"vp":"156.154.71.37","location":{"country_code":"US","country_name":"United States"},"test_url":"www.usacasino.com","response":{"15.126.193.233":["no_tags"],"rcode":["0","0","0"]},"passed_control":true,"connect_error":false,"in_control_group":true,"anomaly":true,"confidence":{"average":0,"matches":[0],"untagged_controls":false,"untagged_response":true},"start_time":"2021-03-01 12:43:25.3438285 -0500 EST m=+0.421998701","end_time":"2021-03-01 12:43:25.3696119 -0500 EST m=+0.447782001"}"""),
      ("CP_Satellite-2021-04-18-12-00-01/results.json", """{"vp":"87.119.233.243","location":{"country_name":"Russia","country_code":"RU"},"test_url":"feedly.com","response":{},"passed_control":false,"connect_error":true,"in_control_group":true,"anomaly":false,"confidence":{"average":0,"matches":null,"untagged_controls":false,"untagged_response":false},"start_time":"2021-04-18 14:49:01.62448452 -0400 EDT m=+10140.555964129","end_time":"2021-04-18 14:49:03.624563629 -0400 EDT m=+10142.556043238"}"""),
      ("CP_Satellite-2021-04-18-12-00-01/results.json", """{"vp":"12.5.76.236","location":{"country_name":"United States","country_code":"US"},"test_url":"ultimate-guitar.com","response":{"rcode":["2"]},"passed_control":true,"connect_error":false,"in_control_group":true,"anomaly":true,"confidence":{"average":0,"matches":null,"untagged_controls":false,"untagged_response":false},"start_time":"2021-04-18 14:49:07.712972288 -0400 EDT m=+10146.644451890","end_time":"2021-04-18 14:49:07.749265765 -0400 EDT m=+10146.680745375"}"""),
      ("CP_Satellite-2021-04-18-12-00-01/responses_control.json", """{"vp":"64.6.65.6","test_url":"ultimate-guitar.com","response":[{"url":"a.root-servers.net","has_type_a":true,"response":["198.41.0.4"],"error":"null","rcode":0,"start_time":"2021-04-18 14:51:57.561175746 -0400 EDT m=+10316.492655353","end_time":"2021-04-18 14:51:57.587097567 -0400 EDT m=+10316.518577181"},{"url":"ultimate-guitar.com","has_type_a":true,"response":["178.18.22.152"],"error":"null","rcode":0,"start_time":"2021-04-18 14:51:57.587109091 -0400 EDT m=+10316.518588694","end_time":"2021-04-18 14:51:57.61294601 -0400 EDT m=+10316.544425613"}],"passed_control":true,"connect_error":false}"""),
      ("CP_Satellite-2021-04-18-12-00-01/responses_control.json", """{"vp":"64.6.65.6","test_url":"www.awid.org","response":[{"url":"a.root-servers.net","has_type_a":true,"response":["198.41.0.4"],"error":"null","rcode":0,"start_time":"2021-04-18 14:51:45.836310062 -0400 EDT m=+10304.767789664","end_time":"2021-04-18 14:51:45.862080031 -0400 EDT m=+10304.793559633"},{"url":"www.awid.org","has_type_a":false,"response":null,"error":"read udp 141.212.123.185:39868->64.6.65.6:53: i/o timeout","rcode":-1,"start_time":"2021-04-18 14:51:45.862091022 -0400 EDT m=+10304.793570624","end_time":"2021-04-18 14:51:47.862170832 -0400 EDT m=+10306.793650435"},{"url":"www.awid.org","has_type_a":true,"response":["204.187.13.189"],"error":"null","rcode":0,"start_time":"2021-04-18 14:51:47.862183185 -0400 EDT m=+10306.793662792","end_time":"2021-04-18 14:51:48.162724942 -0400 EDT m=+10307.094204544"}],"passed_control":true,"connect_error":false}""")
    ]

    tags = [
      ("CP_Satellite-2021-03-01-12-00-01/tagged_resolvers.json", """{"location":{"country_code":"IE","country_name":"Ireland"},"vp":"185.228.169.37"}"""),
      ("CP_Satellite-2021-03-01-12-00-01/tagged_resolvers.json", """{"location":{"country_code":"US","country_name":"United States"},"vp":"156.154.71.37"}"""),
      ("CP_Satellite-2021-03-01-12-00-01/resolvers.json", """{"name":"rdns37b.ultradns.net.","vp":"156.154.71.37"}"""),
      ("CP_Satellite-2021-03-01-12-00-01/resolvers.json", """{"name":"customfilter37-dns2.cleanbrowsing.org.","vp":"185.228.169.37"}"""),
      ("CP_Satellite-2021-03-01-12-00-01/tagged_responses.json", """{"asname":"WIKIMEDIA","asnum":14907,"cert":"9eb21a74a3cf1ecaaf6b19253025b4ca38f182e9f1f3e7355ba3c3004d4b7a10","http":"7b4b4d1bfb0a645c990f55557202f88be48e1eee0c10bdcc621c7b682bf7d2ca","ip":"198.35.26.96"}"""),
      ("CP_Satellite-2021-04-18-12-00-01/resolvers.json", """{"name":"87-119-233-243.saransk.ru.","vp":"87.119.233.243"}"""),
      ("CP_Satellite-2021-04-18-12-00-01/resolvers.json", """{"name":"ns1327.ztomy.com.","vp":"12.5.76.236"}"""),
      ("CP_Satellite-2021-04-18-12-00-01/resolvers.json", """{"name": "rec1pubns2.ultradns.net.", "vp": "64.6.65.6"}"""),
    ]

    expected = [
      {
        'ip': '185.228.169.37',
        'country': 'IE',
        'name': 'customfilter37-dns2.cleanbrowsing.org.',
        'domain': 'ar.m.wikipedia.org',
        'category': 'Culture',
        'error': None,
        'anomaly': False,
        'success': True,
        'controls_failed': False,
        'received': [
            {'ip': '198.35.26.96', 'asname': 'WIKIMEDIA','asnum': 14907,'cert': '9eb21a74a3cf1ecaaf6b19253025b4ca38f182e9f1f3e7355ba3c3004d4b7a10','http': '7b4b4d1bfb0a645c990f55557202f88be48e1eee0c10bdcc621c7b682bf7d2ca', 'matches_control': 'cert asnum asname'},
        ],
        'rcode': ['0', '0', '0'],
        'date': '2021-03-01',
        'start_time': '2021-03-01T12:43:25.3438285-05:00',
        'end_time': '2021-03-01T12:43:25.3696119-05:00'
      },
      {
        'ip': '156.154.71.37',
        'country': 'US',
        'name': 'rdns37b.ultradns.net.',
        'domain': 'www.usacasino.com',
        'category': 'Gambling',
        'error': None,
        'anomaly': True,
        'success': True,
        'controls_failed': False,
        'received': [
            {'ip': '15.126.193.233', 'matches_control': ''},
        ],
        'rcode': ['0', '0', '0'],
        'date': '2021-03-01',
        'start_time': '2021-03-01T12:43:25.3438285-05:00',
        'end_time': '2021-03-01T12:43:25.3696119-05:00'
      },
      {
        'ip': '87.119.233.243',
        'country': 'RU',
        'name': '87-119-233-243.saransk.ru.',
        'domain': 'feedly.com',
        'category': 'E-commerce',
        'error': None,
        'anomaly': False,
        'success': False,
        'controls_failed': True,
        'received': [],
        'rcode': [],
        'date': '2021-04-18',
        'start_time': '2021-04-18T14:49:01.62448452-04:00',
        'end_time': '2021-04-18T14:49:03.624563629-04:00'
      },
      {
        'ip': '12.5.76.236',
        'country': 'US',
        'name': 'ns1327.ztomy.com.',
        'domain': 'ultimate-guitar.com',
        'category': 'History arts and literature',
        'error': None,
        'anomaly': True,
        'success': True,
        'controls_failed': False,
        'received': [],
        'rcode': ['2'],
        'date': '2021-04-18',
        'start_time': '2021-04-18T14:49:07.712972288-04:00',
        'end_time': '2021-04-18T14:49:07.749265765-04:00'
      },
      {
        'ip': '64.6.65.6',
        'name': 'rec1pubns2.ultradns.net.',
        'domain': 'ultimate-guitar.com',
        'category': 'History arts and literature',
        'error': None,
        'anomaly': None,
        'success': True,
        'controls_failed': False,
        'has_type_a': True,
        'received': [
            {'ip': '178.18.22.152'}
        ],
        'rcode': ['0', '0'],
        'date': '2021-04-18',
        'start_time': '2021-04-18T14:51:57.561175746-04:00',
        'end_time': '2021-04-18T14:51:57.61294601-04:00'
      },
      {
        'ip': '64.6.65.6',
        'name': 'rec1pubns2.ultradns.net.',
        'domain': 'www.awid.org',
        'category': 'Human Rights Issues',
        'error': 'read udp 141.212.123.185:39868->64.6.65.6:53: i/o timeout',
        'anomaly': None,
        'success': True,
        'controls_failed': False,
        'has_type_a': True,
        'received': [
            {'ip': '204.187.13.189'}
        ],
        'rcode': ['0', '-1', '0'],
        'date': '2021-04-18',
        'start_time': '2021-04-18T14:51:45.836310062-04:00',
        'end_time': '2021-04-18T14:51:48.162724942-04:00'
      }
    ]
    # yapf: enable

    with TestPipeline() as p:
      lines = p | 'create data' >> beam.Create(data)
      lines2 = p | 'create tags' >> beam.Create(tags)

      final = satellite.process_satellite_with_tags(lines, lines2)
      beam_test_util.assert_that(final, beam_test_util.equal_to(expected))

  def test_partition_satellite_input(self) -> None:  # pylint: disable=no-self-use
    """Test partitioning of Satellite input into tags, blockpages, and results."""
    data = [("CP_Satellite-2020-09-02-12-00-01/resolvers.json", "tag"),
            ("CP_Satellite-2020-09-02-12-00-01/resolvers.json", "tag"),
            ("CP_Satellite-2020-09-02-12-00-01/tagged_resolvers.json", "tag"),
            ("CP_Satellite-2020-09-02-12-00-01/tagged_resolvers.json", "tag"),
            ("CP_Satellite-2020-09-02-12-00-01/tagged_answers.json", "tag"),
            ("CP_Satellite-2020-09-02-12-00-01/tagged_answers.json", "tag"),
            ("CP_Satellite-2021-09-02-12-00-01/blockpages.json", "blockpage"),
            ("CP_Satellite-2020-09-02-12-00-01/interference.json", "row"),
            ("CP_Satellite-2020-09-02-12-00-01/interference.json", "row")]

    expected_tags = data[0:6]
    expected_blockpages = data[6:7]
    expected_rows = data[7:]

    with TestPipeline() as p:
      lines = p | 'create data' >> beam.Create(data)

      tags, blockpages, rows = lines | beam.Partition(
          satellite.partition_satellite_input, 3)

      beam_test_util.assert_that(
          tags,
          beam_test_util.equal_to(expected_tags),
          label='assert_that/tags')
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
    scans: List[flatten.Row] = [
      {
        'ip': '114.114.114.110',
        'country': 'CN',
        'name': 'name',
        'domain': 'abs-cbn.com',
        'category': 'Culture',
        'error': None,
        'anomaly': True,
        'success': True,
        'received': [{'ip': '104.20.161.134', 'matches_control': ''}],
        'date': '2020-09-02'
      },
      {
        'ip': '1.1.1.3',
        'country': 'US',
        'name': 'special',
        'domain': 'signal.org',
        'category': 'Communication Tools',
        'error': None,
        'anomaly': False,
        'success': True,
        'received': [
            {'ip': '13.249.134.38', 'asname': 'AMAZON-02','asnum': 16509,'cert': None,'http': 'c5ba7f2da503045170f1d66c3e9f84576d8f3a606bb246db589a8f62c65921af', 'matches_control': 'ip http asnum asname'},
            {'ip': '13.249.134.44', 'asname': 'AMAZON-02','asnum': 16509,'cert': None,'http': '256e35b8bace0e9fe95f308deb35f82117cd7317f90a08f181516c31abe95b71', 'matches_control': 'ip http asnum asname'},
            {'ip': '13.249.134.74', 'asname': 'AMAZON-02','asnum': 16509,'cert': None,'http': '2054d0fd3887e0ded023879770d6cde57633b7881f609f1042d90fedf41685fe', 'matches_control': 'ip http asnum asname'},
            {'ip': '13.249.134.89', 'asname': 'AMAZON-02','asnum': 16509,'cert': None,'http': '0509322329cdae79475531a019a3628aa52598caa0135c5534905f0c4b4f1bac', 'matches_control': 'ip http asnum asname'}
        ],
        'date': '2020-09-02'
      },
      {
        'ip': '1.1.1.3',
        'country': 'US',
        'name': 'special',
        'domain': 'signal.org',
        'category': 'Communication Tools',
        'error': None,
        'anomaly': False,
        'success': True,
        'received': [
            {'ip': '13.249.134.38', 'asname': 'AS1','asnum': 11111,'cert': None,'http': 'c5ba7f2da503045170f1d66c3e9f84576d8f3a606bb246db589a8f62c65921af', 'matches_control': ''},
            {'ip': '13.249.134.44', 'asname': 'AS2','asnum': 22222,'cert': 'cert','http': '256e35b8bace0e9fe95f308deb35f82117cd7317f90a08f181516c31abe95b71', 'matches_control': 'asnum asname'},
            {'ip': '13.249.134.74', 'asname': 'AS2','asnum': 22222,'cert': None,'http': '2054d0fd3887e0ded023879770d6cde57633b7881f609f1042d90fedf41685fe', 'matches_control': 'ip http asnum asname'},
            {'ip': '13.249.134.89', 'asname': 'AS2','asnum': 22222,'cert': None,'http': '0509322329cdae79475531a019a3628aa52598caa0135c5534905f0c4b4f1bac', 'matches_control': 'ip http asnum asname'}
        ],
        'date': '2020-09-02'
      }
    ]

    expected = [
      {
        'average': 0,
        'matches': [0],
        'untagged_controls': False,
        'untagged_response': True
      },
      {
        'average': 100,
        'matches': [100, 100, 100, 100],
        'untagged_controls': False,
        'untagged_response': False
      },
      {
        'average': 62.5,
        'matches': [0, 50, 100, 100],
        'untagged_controls': False,
        'untagged_response': False
      }
    ]
    # yapf: enable
    result = [
        satellite._calculate_confidence(scan, 1)['confidence'] for scan in scans
    ]
    self.assertListEqual(result, expected)

  def test_verify(self) -> None:
    """Test verification of Satellite v1 data."""
    # yapf: disable
    scans: List[flatten.Row] = [
      {
        'ip': '114.114.114.110',
        'country': 'CN',
        'name': 'name',
        'domain': 'abs-cbn.com',
        'category': 'Culture',
        'error': None,
        'anomaly': True,
        'success': True,
        'received': [{'ip': '104.20.161.134', 'matches_control': ''}],
        'date': '2020-09-02'
      },
      {
        'ip': '114.114.114.110',
        'country': 'CN',
        'name': 'name',
        'domain': 'ar.m.wikipedia.org',
        'category': 'E-commerce',
        'error': None,
        'anomaly': True,
        'success': True,
        'received': [{'ip': '198.35.26.96', 'matches_control': ''}],
        'date': '2020-09-02'
      },
      {
        'ip': '1.1.1.3',
        'country': 'US',
        'name': 'special',
        'domain': 'signal.org',
        'category': 'Communication Tools',
        'error': None,
        'anomaly': True,
        'success': True,
        'received': [
            {'ip': '13.249.134.38', 'asname': 'AMAZON-02','asnum': 16509,'cert': None,'http': 'c5ba7f2da503045170f1d66c3e9f84576d8f3a606bb246db589a8f62c65921af', 'matches_control': ''},
            {'ip': '13.249.134.44', 'asname': 'AMAZON-02','asnum': 16509,'cert': None,'http': '256e35b8bace0e9fe95f308deb35f82117cd7317f90a08f181516c31abe95b71', 'matches_control': ''},
        ],
        'date': '2020-09-02'
      },
    ]
    # yapf: enable

    # mock data for the global interference IP - DOMAIN mapping
    flatten.INTERFERENCE_IPDOMAIN = {
        '104.20.161.134': {'abs-cbn.com', 'xyz.com', 'blah.com'},
        '198.35.26.96': {'ar.m.wikipedia.org'},
    }
    expected = [
        # answer IP is returned for multiple domains: likely to be interference
        (False, ''),
        # answer IP is returned for one domain: false positive
        (True, 'domain_below_threshold'),
        # answer IPs are CDN: false positive
        (True, 'is_CDN is_CDN'),
    ]
    result = []
    for scan in scans:
      scan = satellite._verify(scan)
      result.append(
          (scan['verify']['excluded'], scan['verify']['exclude_reason']))

    self.assertListEqual(result, expected)

  def test_merge_metadata_with_rows(self) -> None:
    """Test merging IP metadata pcollection with rows pcollection."""
    key: satellite.DateIpKey = ('2020-01-01', '1.1.1.1')
    ip_metadata: flatten.Row = {
        'netblock': '1.0.0.1/24',
        'asn': 13335,
        'as_name': 'CLOUDFLARENET',
        'as_full_name': 'Cloudflare Inc.',
        'as_class': 'Content',
        'country': 'US',
    }
    rows: List[flatten.Row] = [{
        'domain': 'www.example.com',
        'ip': '1.1.1.1',
        'date': '2020-01-01',
    }, {
        'domain': 'www.example2.com',
        'ip': '1.1.1.1',
        'date': '2020-01-01',
    }]
    value: Dict[str, List[flatten.Row]] = {
        satellite.IP_METADATA_PCOLLECTION_NAME: [ip_metadata],
        satellite.ROWS_PCOLLECION_NAME: rows
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

    rows_with_metadata = list(satellite.merge_metadata_with_rows(key, value))
    self.assertListEqual(rows_with_metadata, expected_rows)

  # pylint: enable=protected-access


if __name__ == '__main__':
  unittest.main()