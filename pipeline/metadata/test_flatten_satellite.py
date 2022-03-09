"""Unit tests for satellite flattening functions"""

import json
import unittest
from typing import List

from pipeline.metadata.blockpage import BlockpageMatcher
from pipeline.metadata.domain_categories import DomainCategoryMatcher

from pipeline.metadata import flatten_satellite

# pylint: disable=too-many-lines


def get_satellite_flattener() -> flatten_satellite.SatelliteFlattener:
  blockpage_matcher = BlockpageMatcher()
  category_matcher = DomainCategoryMatcher()
  return flatten_satellite.SatelliteFlattener(blockpage_matcher,
                                              category_matcher)


def get_blockpage_flattener() -> flatten_satellite.FlattenBlockpages:
  flattener = flatten_satellite.FlattenBlockpages()
  flattener.setup()
  return flattener


class FlattenSatelliteTest(unittest.TestCase):
  """Unit tests for satellite pipeline flattening."""

  def setUp(self) -> None:
    self.maxDiff = None  # pylint: disable=invalid-name

  def test_flattenmeasurement_satellite_v1(self) -> None:
    """Test flattening of Satellite v1 measurements."""

    filenames = [
        'gs://firehook-scans/satellite/CP_Satellite-2020-09-02-12-00-01/interference.json',
        'gs://firehook-scans/satellite/CP_Satellite-2020-09-02-12-00-01/interference.json',
        'gs://firehook-scans/satellite/CP_Satellite-2020-09-02-12-00-01/interference.json',
        'gs://firehook-scans/satellite/CP_Satellite-2020-09-02-12-00-01/answers_control.json',
    ]

    lines = [{
        "resolver": "67.69.184.215",
        "query": "asana.com",
        "answers": {
            "151.101.1.184": ["ip", "http", "cert", "asnum", "asname"],
            "151.101.129.184": ["ip", "http", "cert", "asnum", "asname"],
            "151.101.193.184": ["ip", "http", "cert", "asnum", "asname"],
            "151.101.65.184": ["ip", "cert", "asnum", "asname"]
        },
        "passed": True
    }, {
        "resolver": "145.239.6.50",
        "query": "www.ecequality.org",
        "answers": {
            "160.153.136.3": []
        },
        "passed": False
    }, {
        "resolver": "185.228.168.149",
        "query": "www.sportsinteraction.com",
        "error": "no_answer"
    }, {
        "resolver": "1.1.1.1",
        "query": "www.usacasino.com",
        "answers": ["217.19.248.132"]
    }]

    expected = [{
        'domain': 'asana.com',
        'is_control': False,
        'controls_failed': False,
        'category': 'E-commerce',
        'ip': '67.69.184.215',
        'is_control_ip': False,
        'date': '2020-09-02',
        'error': None,
        'anomaly': False,
        'success': True,
        'received': [{
            'ip': '151.101.1.184',
            'matches_control': 'ip http cert asnum asname'
        }, {
            'ip': '151.101.129.184',
            'matches_control': 'ip http cert asnum asname'
        }, {
            'ip': '151.101.193.184',
            'matches_control': 'ip http cert asnum asname'
        }, {
            'ip': '151.101.65.184',
            'matches_control': 'ip cert asnum asname'
        }],
        'rcode': 0,
        'measurement_id': 'ab3b0ed527334c6ba988362e6a2c98fc',
        'source': 'CP_Satellite-2020-09-02-12-00-01'
    }, {
        'domain': 'www.ecequality.org',
        'is_control': False,
        'controls_failed': False,
        'category': 'LGBT',
        'ip': '145.239.6.50',
        'is_control_ip': False,
        'date': '2020-09-02',
        'error': None,
        'anomaly': True,
        'success': True,
        'received': [{
            'ip': '160.153.136.3'
        }],
        'rcode': 0,
        'measurement_id': 'ab3b0ed527334c6ba988362e6a2c98fc',
        'source': 'CP_Satellite-2020-09-02-12-00-01'
    }, {
        'domain': 'www.sportsinteraction.com',
        'is_control': False,
        'controls_failed': False,
        'category': 'Gambling',
        'ip': '185.228.168.149',
        'is_control_ip': False,
        'date': '2020-09-02',
        'error': 'no_answer',
        'anomaly': None,
        'success': False,
        'received': [],
        'rcode': -1,
        'measurement_id': 'ab3b0ed527334c6ba988362e6a2c98fc',
        'source': 'CP_Satellite-2020-09-02-12-00-01'
    }, {
        'domain': 'www.usacasino.com',
        'is_control': False,
        'controls_failed': False,
        'category': 'Gambling',
        'ip': '1.1.1.1',
        'is_control_ip': True,
        'date': '2020-09-02',
        'error': None,
        'anomaly': None,
        'success': True,
        'received': [{
            'ip': '217.19.248.132'
        }],
        'rcode': 0,
        'measurement_id': 'ab3b0ed527334c6ba988362e6a2c98fc',
        'source': 'CP_Satellite-2020-09-02-12-00-01'
    }]

    flattener = get_satellite_flattener()
    results = []

    for filename, line in zip(filenames, lines):
      rows = flattener.process_satellite(filename, line,
                                         'ab3b0ed527334c6ba988362e6a2c98fc')
      results.extend(list(rows))

    self.assertListEqual(results, expected)

  def test_flattenmeasurement_satellite_v2p1(self) -> None:
    """Test flattening of Satellite v2.1 measurements."""
    filenames = [
        'gs://firehook-scans/satellite/CP_Satellite-2021-03-01-12-00-01/interference.json',
        'gs://firehook-scans/satellite/CP-Satellite-2021-03-15-12-00-01/responses_control.json.gz',
        'gs://firehook-scans/satellite/CP-Satellite-2021-09-16-12-00-01/results.json'
    ]

    interference = [{
        "vp": "114.114.114.110",
        "location": {
            "country_name": "China",
            "country_code": "CN"
        },
        "test_url": "abs-cbn.com",
        "response": {
            "104.20.161.135": [],
            "104.20.161.134": [],
            "rcode": ["0"]
        },
        "passed_control": True,
        "connect_error": False,
        "in_control_group": True,
        "anomaly": True,
        "confidence": {
            "average": 0,
            "matches": [0, 0],
            "untagged_controls": False,
            "untagged_response": False
        },
        "start_time": "2021-03-05 15:32:05.324807502 -0500 EST m=+6.810936646",
        "end_time": "2021-03-05 15:32:05.366104911 -0500 EST m=+6.852233636"
    }, {
        "vp": "8.8.4.4",
        "test_url": "login.live.com",
        "response": [{
            "url":
                "www.example.com",
            "has_type_a":
                True,
            "response": ["93.184.216.34"],
            "error":
                "null",
            "rcode":
                0,
            "start_time":
                "2021-03-15 20:02:06.996014164 -0400 EDT m=+10807.095374809",
            "end_time":
                "2021-03-15 20:02:07.002365608 -0400 EDT m=+10807.101726261"
        }, {
            "url":
                "login.live.com",
            "has_type_a":
                True,
            "response": ["40.126.31.135", "40.126.31.8", "40.126.31.6"],
            "error":
                "null",
            "rcode":
                0,
            "start_time":
                "2021-03-15 20:02:07.002380008 -0400 EDT m=+10807.101740649",
            "end_time":
                "2021-03-15 20:02:07.008908491 -0400 EDT m=+10807.108269140"
        }, {
            "url":
                "www.example.com",
            "has_type_a":
                True,
            "response": ["93.184.216.34"],
            "error":
                "null",
            "rcode":
                0,
            "start_time":
                "2021-03-15 20:02:07.008915491 -0400 EDT m=+10807.108276134",
            "end_time":
                "2021-03-15 20:02:07.015386713 -0400 EDT m=+10807.114747357"
        }],
        "passed_control": True,
        "connect_error": False
    }, {
        "vp": "91.135.154.38",
        "test_url": "03.ru",
        "location": {
            "country_name": "Russia",
            "country_code": "RU"
        },
        "passed_liveness": True,
        "in_control_group": True,
        "connect_error": False,
        "anomaly": False,
        "start_time": "2021-09-16 17:49:17.488153752 -0400 EDT",
        "end_time": "2021-09-16 17:49:17.741250869 -0400 EDT",
        "response": [{
            "url": "03.ru",
            "has_type_a": True,
            "response": {
                "88.212.202.9": {
                    "http":
                        "8351c0267c2cd7866ff04c04261f06cd75af9a7130aac848ca43fd047404e229",
                    "cert":
                        "162be4020c68591192f906bcc7c27cec0ce3eb905531bec517b97e17cc1c1c49",
                    "asnum":
                        39134,
                    "asname":
                        "UNITEDNET",
                    "matched": ["ip", "http", "cert", "asnum", "asname"]
                }
            },
            "error": "null",
            "rcode": 0
        }],
        "confidence": {
            "average": 100,
            "matches": [100],
            "untagged_controls": False,
            "untagged_response": False
        },
        "excluded": False,
        "exclude_reason": []
    }]

    expected = [{
        'domain': 'abs-cbn.com',
        'is_control': False,
        'category': 'Culture',
        'ip': '114.114.114.110',
        'is_control_ip': False,
        'country': 'CN',
        'date': '2021-03-05',
        'start_time': '2021-03-05T15:32:05.324807502-05:00',
        'end_time': '2021-03-05T15:32:05.366104911-05:00',
        'error': None,
        'anomaly': True,
        'success': True,
        'controls_failed': False,
        'received': [{
            'ip': '104.20.161.135'
        }, {
            'ip': '104.20.161.134'
        }],
        'rcode': 0,
        'measurement_id': 'ab3b0ed527334c6ba988362e6a2c98fc',
        'source': 'CP_Satellite-2021-03-01-12-00-01'
    }, {
        'domain': 'www.example.com',
        'is_control': True,
        'category': 'Control',
        'ip': '8.8.4.4',
        'is_control_ip': True,
        'date': '2021-03-15',
        'start_time': '2021-03-15T20:02:06.996014164-04:00',
        'end_time': '2021-03-15T20:02:07.002365608-04:00',
        'error': None,
        'anomaly': None,
        'success': True,
        'controls_failed': False,
        'received': [{
            'ip': '93.184.216.34'
        }],
        'rcode': 0,
        'measurement_id': 'ab3b0ed527334c6ba988362e6a2c98fc',
        'source': 'CP-Satellite-2021-03-15-12-00-01',
        'has_type_a': True
    }, {
        'domain': 'login.live.com',
        'is_control': False,
        'category': 'Communication Tools',
        'ip': '8.8.4.4',
        'is_control_ip': True,
        'date': '2021-03-15',
        'start_time': '2021-03-15T20:02:07.002380008-04:00',
        'end_time': '2021-03-15T20:02:07.008908491-04:00',
        'error': None,
        'anomaly': None,
        'success': True,
        'controls_failed': False,
        'received': [
            {
                'ip': '40.126.31.135'
            },
            {
                'ip': '40.126.31.8'
            },
            {
                'ip': '40.126.31.6'
            },
        ],
        'rcode': 0,
        'measurement_id': 'ab3b0ed527334c6ba988362e6a2c98fc',
        'source': 'CP-Satellite-2021-03-15-12-00-01',
        'has_type_a': True
    }, {
        'domain': 'www.example.com',
        'is_control': True,
        'category': 'Control',
        'ip': '8.8.4.4',
        'is_control_ip': True,
        'date': '2021-03-15',
        'start_time': '2021-03-15T20:02:07.008915491-04:00',
        'end_time': '2021-03-15T20:02:07.015386713-04:00',
        'error': None,
        'anomaly': None,
        'success': True,
        'controls_failed': False,
        'received': [{
            'ip': '93.184.216.34'
        }],
        'rcode': 0,
        'measurement_id': 'ab3b0ed527334c6ba988362e6a2c98fc',
        'source': 'CP-Satellite-2021-03-15-12-00-01',
        'has_type_a': True
    }, {
        'domain': '03.ru',
        'is_control': False,
        'category': None,
        'ip': '91.135.154.38',
        'is_control_ip': False,
        'country': 'RU',
        'date': '2021-09-16',
        'start_time': '2021-09-16T17:49:17.488153752-04:00',
        'end_time': '2021-09-16T17:49:17.741250869-04:00',
        'error': None,
        'anomaly': False,
        'success': True,
        'controls_failed': False,
        'received': [{
            'ip':
                '88.212.202.9',
            'http':
                '8351c0267c2cd7866ff04c04261f06cd75af9a7130aac848ca43fd047404e229',
            'cert':
                '162be4020c68591192f906bcc7c27cec0ce3eb905531bec517b97e17cc1c1c49',
            'asnum':
                39134,
            'asname':
                'UNITEDNET',
            'matches_control':
                'ip http cert asnum asname'
        }],
        'rcode': 0,
        'average_confidence': 100,
        'matches_confidence': [100],
        'untagged_controls': False,
        'untagged_response': False,
        'excluded': False,
        'exclude_reason': '',
        'measurement_id': 'ab3b0ed527334c6ba988362e6a2c98fc',
        'source': 'CP-Satellite-2021-09-16-12-00-01',
        'has_type_a': True
    }]

    flattener = get_satellite_flattener()
    results = []

    for filename, line in zip(filenames, interference):
      rows = flattener.process_satellite(filename, line,
                                         'ab3b0ed527334c6ba988362e6a2c98fc')
      results.extend(list(rows))

    self.assertListEqual(results, expected)

  def test_flattenmeasurement_satellite_v2p1_single(self) -> None:
    """Test flattening of a single Satellite v2.1 measurements."""
    filename = "CP_Satellite-2021-04-18-12-00-01/results.json"

    # yapf: disable
    line = {
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
    }
    # yapf: enable

    expected = {
        'ip': '12.5.76.236',
        'is_control_ip': False,
        'country': 'US',
        'domain': 'ultimate-guitar.com',
        'is_control': False,
        'category': 'History arts and literature',
        'error': None,
        'anomaly': True,
        'success': True,
        'controls_failed': False,
        'received': [],
        'rcode': 2,
        'date': '2021-04-18',
        'start_time': '2021-04-18T14:49:07.712972288-04:00',
        'end_time': '2021-04-18T14:49:07.749265765-04:00',
        'source': 'CP_Satellite-2021-04-18-12-00-01',
        'measurement_id': 'ab3b0ed527334c6ba988362e6a2c98fc'
    }

    flattener = get_satellite_flattener()
    rows = list(
        flattener.process_satellite(filename, line,
                                    'ab3b0ed527334c6ba988362e6a2c98fc'))

    self.assertEqual(len(rows), 1)
    self.assertEqual(rows[0], expected)

  def test_flattenmeasurement_satellite_v2p1_neg_rcode_no_error(self) -> None:
    """Test flattening of a Satellite v2.1 measurement with a missing error."""
    filename = "CP_Satellite-2021-04-18-12-00-01/results.json"

    # yapf: disable
    line = {
        "vp": "109.69.144.30",
        "location": {
            "country_name": "Italy",
            "country_code": "IT"
        },
        "test_url": "ok.ru",
        "response": {
            "rcode": ["-1"]
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
        "start_time": "2021-04-18 14:49:01.517087379 -0400 EDT m=+10140.448566990",
        "end_time": "2021-04-18 14:49:01.788309957 -0400 EDT m=+10140.719789564"
    }
    # yapf: enable

    expected = {
        'anomaly': True,
        'category': 'News Media',
        'controls_failed': False,
        'country': 'IT',
        'date': '2021-04-18',
        'domain': 'ok.ru',
        'end_time': '2021-04-18T14:49:01.788309957-04:00',
        'error': None,
        'ip': '109.69.144.30',
        'is_control': False,
        'is_control_ip': False,
        'measurement_id': 'ab3b0ed527334c6ba988362e6a2c98fc',
        'rcode': -1,
        'received': [],
        'source': 'CP_Satellite-2021-04-18-12-00-01',
        'start_time': '2021-04-18T14:49:01.517087379-04:00',
        'success': True
    }

    flattener = get_satellite_flattener()
    rows = list(
        flattener.process_satellite(filename, line,
                                    'ab3b0ed527334c6ba988362e6a2c98fc'))

    self.assertEqual(len(rows), 1)
    self.assertEqual(rows[0], expected)

  def test_flattenmeasurement_satellite_v2p1_ips_but_no_0_rcode(self) -> None:
    """Test flattening of a Satellite v2.1 measurement with a missing 0 rcode."""
    filename = "CP_Satellite-2021-05-16-12-00-01/results.json"

    # yapf: disable
    line = {
        'vp': '111.164.180.5',
        'location': {
            'country_name': 'China',
            'country_code': 'CN'
        },
        'test_url': 'protectionline.org',
        'response': {
            '217.70.184.55': ['ip', 'no_tags'],
            'rcode': ['2']
        },
        'passed_control': True,
        'connect_error': False,
        'in_control_group': True,
        'anomaly': False,
        'confidence': {
            'average': 100,
            'matches': [100],
            'untagged_controls': False,
            'untagged_response': True
        },
        'start_time': '2021-05-16 14:46:57.433350046 -0400 EDT m=+10016.312771401',
        'end_time': '2021-05-16 14:46:57.686318846 -0400 EDT m=+10016.565740205'
    }
    # yapf: enable

    flattener = get_satellite_flattener()

    with self.assertLogs() as captured:
      rows = list(
          flattener.process_satellite(filename, line,
                                      'ab3b0ed527334c6ba988362e6a2c98fc'))

    # This line contains an error and currently we drop the data
    self.assertEqual(len(rows), 0)

    self.assertEqual(len(captured.records), 1)
    self.assertIn(
        "Satellite v2.1 measurement has ips but no 0 rcode: CP_Satellite-2021-05-16-12-00-01/results.json",
        captured.records[0].getMessage())

  def test_flattenmeasurement_satellite_v2p2(self) -> None:
    """Test flattening of Satellite v2.2 measurements."""
    filenames = [
        'gs://firehook-scans/satellite/CP-Satellite-2021-10-20-12-00-01/results.json',
        'gs://firehook-scans/satellite/CP-Satellite-2021-10-20-12-00-01/results.json',
        'gs://firehook-scans/satellite/CP-Satellite-2021-10-20-12-00-01/results.json'
    ]

    # yapf: disable
    interference = [{
        "confidence": {
            "average": 100,
            "matches": [100],
            "untagged_controls": False,
            "untagged_response": False
        },
        "passed_liveness": True,
        "connect_error": False,
        "in_control_group": True,
        "anomaly": False,
        "excluded": False,
        "vp": "216.238.19.1",
        "test_url": "11st.co.kr",
        "start_time": "2021-10-20 14:51:41.295287198 -0400 EDT",
        "end_time": "2021-10-20 14:51:41.397573385 -0400 EDT",
        "exclude_reason": [],
        "location": {
            "country_name": "United States",
            "country_code": "US"
        },
        "response": [{
            "url": "11st.co.kr",
            "has_type_a": True,
            "response": {
                "113.217.247.90": {
                    "http": "db2f9ca747f3e2e0896a1b783b27738fddfb4ba8f0500c0bfc0ad75e8f082090",
                    "cert": "6908c7e0f2cc9a700ddd05efc41836da3057842a6c070cdc41251504df3735f4",
                    "asnum": 9644,
                    "asname": "SKTELECOM-NET-AS SK Telecom",
                    "matched": ["ip", "http", "cert", "asnum", "asname"]
                }
            },
            "error": "null",
            "rcode": 0
        }]
    }, {
        "confidence": {
            "average": 0,
            "matches": None,
            "untagged_controls": False,
            "untagged_response": False
        },
        "passed_liveness": False,
        "connect_error": False,
        "in_control_group": True,
        "anomaly": False,
        "excluded": False,
        "vp": "5.39.25.152",
        "test_url": "www.chantelle.com",
        "start_time": "2021-10-20 17:17:27.241422553 -0400 EDT",
        "end_time": "2021-10-20 17:17:27.535929324 -0400 EDT",
        "exclude_reason": [],
        "location": {
            "country_name": "France",
            "country_code": "FR"
        },
        "response": [{
            "url": "a.root-servers.net",
            "has_type_a": False,
            "response": {},
            "error": "null",
            "rcode": 5
        }, {
            "url": "www.chantelle.com",
            "has_type_a": False,
            "response": {},
            "error": "null",
            "rcode": 5
        }, {
            "url": "a.root-servers.net",
            "has_type_a": False,
            "response": {},
            "error": "null",
            "rcode": 5
        }]
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
        "test_url": "alipay.com",
        "start_time": "2021-10-20 14:51:45.952255246 -0400 EDT",
        "end_time": "2021-10-20 14:51:46.865275642 -0400 EDT",
        "exclude_reason": [],
        "location": {
            "country_name": "Ukraine",
            "country_code": "UA"
        },
        "response": [{
            "url": "a.root-servers.net",
            "has_type_a": False,
            "response": {},
            "error": "read udp 141.212.123.185:6280->62.80.182.26:53: read: connection refused",
            "rcode": -1
        }, {
            "url": "alipay.com",
            "has_type_a": False,
            "response": {},
            "error": "read udp 141.212.123.185:61676->62.80.182.26:53: read: connection refused",
            "rcode": -1
        }, {
            "url": "alipay.com",
            "has_type_a": False,
            "response": {},
            "error": "read udp 141.212.123.185:15097->62.80.182.26:53: read: connection refused",
            "rcode": -1
        }, {
            "url": "alipay.com",
            "has_type_a": False,
            "response": {},
            "error": "read udp 141.212.123.185:45072->62.80.182.26:53: read: connection refused",
            "rcode": -1
        }, {
            "url": "alipay.com",
            "has_type_a": False,
            "response": {},
            "error": "read udp 141.212.123.185:36765->62.80.182.26:53: read: connection refused",
            "rcode": -1
        }, {
            "url": "a.root-servers.net",
            "has_type_a": False,
            "response": {},
            "error": "read udp 141.212.123.185:1295->62.80.182.26:53: read: connection refused",
            "rcode": -1
        }]
    }]


    expected = [{
        'anomaly': False,
        'category': 'E-commerce',
        'average_confidence': 100,
        'matches_confidence': [100],
        'untagged_controls': False,
        'untagged_response': False,
        'controls_failed': False,
        'country': 'US',
        'date': '2021-10-20',
        'domain': '11st.co.kr',
        'is_control': False,
        'start_time': '2021-10-20T14:51:41.295287198-04:00',
        'end_time': '2021-10-20T14:51:41.397573385-04:00',
        'error': None,
        'has_type_a': True,
        'ip': '216.238.19.1',
        'is_control_ip': False,
        'measurement_id': 'ab3b0ed527334c6ba988362e6a2c98fc',
        'source': 'CP-Satellite-2021-10-20-12-00-01',
        'rcode': 0,
        'received': [{
            'asname': 'SKTELECOM-NET-AS SK Telecom',
            'asnum': 9644,
            'cert': '6908c7e0f2cc9a700ddd05efc41836da3057842a6c070cdc41251504df3735f4',
            'http': 'db2f9ca747f3e2e0896a1b783b27738fddfb4ba8f0500c0bfc0ad75e8f082090',
            'ip': '113.217.247.90',
            'matches_control': 'ip http cert asnum asname'
        }],
        'success': True,
        'exclude_reason': '',
        'excluded': False
    }, {
        'anomaly': False,
        'category': 'Control',
        'untagged_controls': False,
        'untagged_response': False,
        'controls_failed': True,
        'country': 'FR',
        'date': '2021-10-20',
        'domain': 'a.root-servers.net',
        'is_control': True,
        'start_time': '2021-10-20T17:17:27.241422553-04:00',
        'end_time': '2021-10-20T17:17:27.535929324-04:00',
        'error': None,
        'ip': '5.39.25.152',
        'is_control_ip': False,
        'measurement_id': 'ab3b0ed527334c6ba988362e6a2c98fc',
        'source': 'CP-Satellite-2021-10-20-12-00-01',
        'rcode': 5,
        'received': [],
        'success': False,
        'exclude_reason': '',
        'excluded': False
    }, {
        'anomaly': False,
        'category': 'Provocative Attire',
        'untagged_controls': False,
        'untagged_response': False,
        'controls_failed': True,
        'country': 'FR',
        'date': '2021-10-20',
        'domain': 'www.chantelle.com',
        'is_control': False,
        'start_time': '2021-10-20T17:17:27.241422553-04:00',
        'end_time': '2021-10-20T17:17:27.535929324-04:00',
        'error': None,
        'ip': '5.39.25.152',
        'is_control_ip': False,
        'measurement_id': 'ab3b0ed527334c6ba988362e6a2c98fc',
        'source': 'CP-Satellite-2021-10-20-12-00-01',
        'rcode': 5,
        'received': [],
        'success': False,
        'exclude_reason': '',
        'excluded': False
    }, {
        'anomaly': False,
        'category': 'Control',
        'untagged_controls': False,
        'untagged_response': False,
        'controls_failed': True,
        'country': 'FR',
        'date': '2021-10-20',
        'domain': 'a.root-servers.net',
        'is_control': True,
        'start_time': '2021-10-20T17:17:27.241422553-04:00',
        'end_time': '2021-10-20T17:17:27.535929324-04:00',
        'error': None,
        'ip': '5.39.25.152',
        'is_control_ip': False,
        'measurement_id': 'ab3b0ed527334c6ba988362e6a2c98fc',
        'source': 'CP-Satellite-2021-10-20-12-00-01',
        'rcode': 5,
        'received': [],
        'success': False,
        'exclude_reason': '',
        'excluded': False
    }, {
        'anomaly': False,
        'category': 'Control',
        'untagged_controls': False,
        'untagged_response': False,
        'controls_failed': True,
        'country': 'UA',
        'date': '2021-10-20',
        'domain': 'a.root-servers.net',
        'is_control': True,
        'end_time': '2021-10-20T14:51:46.865275642-04:00',
        'error': 'read udp 141.212.123.185:6280->62.80.182.26:53: read: connection refused',
        'ip': '62.80.182.26',
        'is_control_ip': False,
        'measurement_id': 'ab3b0ed527334c6ba988362e6a2c98fc',
        'source': 'CP-Satellite-2021-10-20-12-00-01',
        'rcode': -1,
        'received': [],
        'start_time': '2021-10-20T14:51:45.952255246-04:00',
        'success': False,
        'exclude_reason': '',
        'excluded': False
    }, {
        'anomaly': False,
        'category': 'E-commerce',
        'untagged_controls': False,
        'untagged_response': False,
        'controls_failed': True,
        'country': 'UA',
        'date': '2021-10-20',
        'domain': 'alipay.com',
        'is_control': False,
        'end_time': '2021-10-20T14:51:46.865275642-04:00',
        'error': 'read udp 141.212.123.185:61676->62.80.182.26:53: read: connection refused',
        'ip': '62.80.182.26',
        'is_control_ip': False,
        'measurement_id': 'ab3b0ed527334c6ba988362e6a2c98fc',
        'source': 'CP-Satellite-2021-10-20-12-00-01',
        'rcode': -1,
        'received': [],
        'start_time': '2021-10-20T14:51:45.952255246-04:00',
        'success': False,
        'exclude_reason': '',
        'excluded': False
    }, {
        'anomaly': False,
        'category': 'E-commerce',
        'untagged_controls': False,
        'untagged_response': False,
        'controls_failed': True,
        'country': 'UA',
        'date': '2021-10-20',
        'domain': 'alipay.com',
        'is_control': False,
        'end_time': '2021-10-20T14:51:46.865275642-04:00',
        'error': 'read udp 141.212.123.185:15097->62.80.182.26:53: read: connection refused',
        'ip': '62.80.182.26',
        'is_control_ip': False,
        'measurement_id': 'ab3b0ed527334c6ba988362e6a2c98fc',
        'source': 'CP-Satellite-2021-10-20-12-00-01',
        'rcode': -1,
        'received': [],
        'start_time': '2021-10-20T14:51:45.952255246-04:00',
        'success': False,
        'exclude_reason': '',
        'excluded': False
    }, {
        'anomaly': False,
        'category': 'E-commerce',
        'untagged_controls': False,
        'untagged_response': False,
        'controls_failed': True,
        'country': 'UA',
        'date': '2021-10-20',
        'domain': 'alipay.com',
        'is_control': False,
        'end_time': '2021-10-20T14:51:46.865275642-04:00',
        'error': 'read udp 141.212.123.185:45072->62.80.182.26:53: read: connection refused',
        'ip': '62.80.182.26',
        'is_control_ip': False,
        'measurement_id': 'ab3b0ed527334c6ba988362e6a2c98fc',
        'source': 'CP-Satellite-2021-10-20-12-00-01',
        'rcode': -1,
        'received': [],
        'start_time': '2021-10-20T14:51:45.952255246-04:00',
        'success': False,
        'exclude_reason': '',
        'excluded': False
    }, {
        'anomaly': False,
        'category': 'E-commerce',
        'untagged_controls': False,
        'untagged_response': False,
        'controls_failed': True,
        'country': 'UA',
        'date': '2021-10-20',
        'domain': 'alipay.com',
        'is_control': False,
        'end_time': '2021-10-20T14:51:46.865275642-04:00',
        'error': 'read udp 141.212.123.185:36765->62.80.182.26:53: read: connection refused',
        'ip': '62.80.182.26',
        'is_control_ip': False,
        'measurement_id': 'ab3b0ed527334c6ba988362e6a2c98fc',
        'source': 'CP-Satellite-2021-10-20-12-00-01',
        'rcode': -1,
        'received': [],
        'start_time': '2021-10-20T14:51:45.952255246-04:00',
        'success': False,
        'exclude_reason': '',
        'excluded': False
    }, {
        'anomaly': False,
        'category': 'Control',
        'untagged_controls': False,
        'untagged_response': False,
        'controls_failed': True,
        'country': 'UA',
        'date': '2021-10-20',
        'domain': 'a.root-servers.net',
        'is_control': True,
        'end_time': '2021-10-20T14:51:46.865275642-04:00',
        'error': 'read udp 141.212.123.185:1295->62.80.182.26:53: read: connection refused',
        'ip': '62.80.182.26',
        'is_control_ip': False,
        'measurement_id': 'ab3b0ed527334c6ba988362e6a2c98fc',
        'source': 'CP-Satellite-2021-10-20-12-00-01',
        'rcode': -1,
        'received': [],
        'start_time': '2021-10-20T14:51:45.952255246-04:00',
        'success': False,
        'exclude_reason': '',
        'excluded': False
    }]
    # yapf: enable

    flattener = get_satellite_flattener()
    results = []

    for filename, line in zip(filenames, interference):
      rows = flattener.process_satellite(filename, line,
                                         'ab3b0ed527334c6ba988362e6a2c98fc')
      results.extend(list(rows))

    self.assertListEqual(results, expected)

  def test_flattenmeasurement_satellite_v2p2_fail_then_success(self) -> None:
    """Test flattening of Satellite v2.2 measurements that fail but eventually succeed."""
    filenames = [
        'gs://firehook-scans/satellite/CP_Satellite-2022-01-23-12-00-01/results.json',
    ]

    # yapf: disable
    interference = [
        {
        "confidence": {
            "average": 66.66666666666667,
            "matches": [66.66666666666667, 66.66666666666667],
            "untagged_controls": False,
            "untagged_response": False
        },
        "passed_liveness": True,
        "connect_error": False,
        "in_control_group": True,
        "anomaly": False,
        "excluded": False,
        "vp": "178.45.11.236",
        "test_url": "www.army.mil",
        "start_time": "2022-01-23 16:54:03.57575605 -0500 EST",
        "end_time": "2022-01-23 16:54:10.009546222 -0500 EST",
        "exclude_reason": [],
        "location": {
            "country_name": "Russia",
            "country_code": "RU"
        },
        "response": [
            {
                "url": "www.army.mil",
                "has_type_a": False,
                "response": {},
                "error": "read udp 141.212.123.185:61223->178.45.11.236:53: i/o timeout",
                "rcode": -1
            },
            {
                "url": "www.army.mil",
                "has_type_a": True,
                "response": {
                    "92.123.189.40": {
                    "http": "b7e803c4b738908b8c525dd7d96a49ea96c4e532ad91a027b65ba9b520a653fb",
                    "cert": "",
                    "asnum": 20940,
                    "asname": "AKAMAI-ASN1",
                    "matched": ["asnum", "asname"]
                    },
                    "92.123.189.41": {
                    "http": "65a6a40c1b153b87b20b789f0dc93442e3ed172774c5dfa77c07b5146333802e",
                    "cert": "",
                    "asnum": 20940,
                    "asname": "AKAMAI-ASN1",
                    "matched": ["asnum", "asname"]
                    }
                },
                "error": "null",
                "rcode": 0
            }
        ]
        }
    ]


    expected = [{
        'anomaly': False,
        'category': 'Government',
        'untagged_controls': False,
        'untagged_response': False,
        'controls_failed': False,
        'country': 'RU',
        'date': '2022-01-23',
        'domain': 'www.army.mil',
        'is_control': False,
        'start_time': '2022-01-23T16:54:03.57575605-05:00',
        'end_time': '2022-01-23T16:54:10.009546222-05:00',
        'error': 'read udp 141.212.123.185:61223->178.45.11.236:53: i/o timeout',
        'ip': '178.45.11.236',
        'is_control_ip': False,
        'measurement_id': 'ab3b0ed527334c6ba988362e6a2c98fc',
        'source': 'CP_Satellite-2022-01-23-12-00-01',
        'rcode': -1,
        'received': [],
        'success': False,
        'exclude_reason': '',
        'excluded': False
    }, {
        'anomaly': False,
        'category': 'Government',
        'average_confidence': 66.66666666666667,
        'matches_confidence': [66.66666666666667, 66.66666666666667],
        'untagged_controls': False,
        'untagged_response': False,
        'controls_failed': False,
        'country': 'RU',
        'date': '2022-01-23',
        'domain': 'www.army.mil',
        'is_control': False,
        'start_time': '2022-01-23T16:54:03.57575605-05:00',
        'end_time': '2022-01-23T16:54:10.009546222-05:00',
        'error': None,
        'has_type_a': True,
        'ip': '178.45.11.236',
        'is_control_ip': False,
        'measurement_id': 'ab3b0ed527334c6ba988362e6a2c98fc',
        'source': 'CP_Satellite-2022-01-23-12-00-01',
        'rcode': 0,
        'received': [{
            'asname': 'AKAMAI-ASN1',
            'asnum': 20940,
            'cert': '',
            'http': 'b7e803c4b738908b8c525dd7d96a49ea96c4e532ad91a027b65ba9b520a653fb',
            'ip': '92.123.189.40',
            'matches_control': 'asnum asname'
        }, {
            'asname': 'AKAMAI-ASN1',
            'asnum': 20940,
            'cert': '',
            'http': '65a6a40c1b153b87b20b789f0dc93442e3ed172774c5dfa77c07b5146333802e',
            'ip': '92.123.189.41',
            'matches_control': 'asnum asname'
        }],
        'success': True,
        'exclude_reason': '',
        'excluded': False
    }]
    # yapf: enable

    flattener = get_satellite_flattener()
    results = []

    for filename, line in zip(filenames, interference):
      rows = flattener.process_satellite(filename, line,
                                         'ab3b0ed527334c6ba988362e6a2c98fc')
      results.extend(list(rows))

    self.assertListEqual(results, expected)

  def test_flattenmeasurement_blockpage(self) -> None:
    """Test parsing a blockpage measurement."""
    # yapf: disable
    line = {
      "ip": "93.158.134.250",
      "keyword": "xvideos.com",
      "http": {
        "status_line": "302 Moved Temporarily",
        "headers": {
          "Content-Length": [
            "170"
          ],
          "Content-Type": [
            "text/html"
          ],
          "Date": [
            "Fri, 17 Sep 2021 01:07:56 GMT"
          ],
          "Location": [
            "https://yandex.ru/safety/?url=xvideos.com&infectedalert=yes&fromdns=adult"
          ],
          "Server": [
            "nginx/1.10.3 (Ubuntu)"
          ]
        },
        "body": "<html>\r\n<head><title>302 Found</title></head>\r\n<body bgcolor=\"white\">\r\n<center><h1>302 Found</h1></center>\r\n<hr><center>nginx/1.10.3 (Ubuntu)</center>\r\n</body>\r\n</html>\r\n",
        },
      "https": "Get \"https://93.158.134.250:443/\": tls: oversized record received with length 20527",
      "fetched": True,
      "start_time": "2021-09-16 21:07:55.814062725 -0400 EDT m=+8.070381531",
      "end_time": "2021-09-16 21:07:56.317107472 -0400 EDT m=+8.573426410"
    }
    # yapf: enable

    filename = 'gs://firehook-scans/satellite/CP-Satellite-2021-09-16-12-00-01/blockpages.json'

    # yapf: disable
    expected_rows: List[flatten_satellite.BlockpageRow] = [
      {
        'domain': 'xvideos.com',
        'ip': '93.158.134.250',
        'date': '2021-09-16',
        'start_time': '2021-09-16T21:07:55.814062725-04:00',
        'end_time': '2021-09-16T21:07:56.317107472-04:00',
        'success': True,
        'source': 'CP-Satellite-2021-09-16-12-00-01',
        'https': False,
        'received_status': '302 Moved Temporarily',
        'received_body': '<html>\r\n<head><title>302 Found</title></head>\r\n<body bgcolor=\"white\">\r\n<center><h1>302 Found</h1></center>\r\n<hr><center>nginx/1.10.3 (Ubuntu)</center>\r\n</body>\r\n</html>\r\n',
        'received_headers': [
          'Content-Length: 170',
          'Content-Type: text/html',
          'Date: Fri, 17 Sep 2021 01:07:56 GMT',
          'Location: https://yandex.ru/safety/?url=xvideos.com&infectedalert=yes&fromdns=adult',
          'Server: nginx/1.10.3 (Ubuntu)'
        ],
        'blockpage': False,
        'page_signature': 'r_fp_26'
      },
      {
        'domain': 'xvideos.com',
        'ip': '93.158.134.250',
        'date': '2021-09-16',
        'start_time': '2021-09-16T21:07:55.814062725-04:00',
        'end_time': '2021-09-16T21:07:56.317107472-04:00',
        'success': True,
        'source': 'CP-Satellite-2021-09-16-12-00-01',
        'https': True,
        'received_status': 'Get \"https://93.158.134.250:443/\": tls: oversized record received with length 20527',
        'received_tls_cert': 'MIIMQjCCCyqgAwIBAgIQDcjI3GKsZdP0vWK4tcFMVDANBgkqhkiG9w0BAQsFADBwMQswCQYDVQQGEwJVUzEVMBMGA1UEChMMRGlnaUNlcnQgSW5jMRkwFwYDVQQLExB3d3cuZGlnaWNlcnQuY29tMS8wLQYDVQQDEyZEaWdpQ2VydCBTSEEyIEhpZ2ggQXNzdXJhbmNlIFNlcnZlciBDQTAeFw0yMjAxMzEwMDAwMDBaFw0yMjAzMjMyMzU5NTlaMHExCzAJBgNVBAYTAlVTMRMwEQYDVQQIEwpDYWxpZm9ybmlhMRIwEAYDVQQHEwlTdW5ueXZhbGUxETAPBgNVBAoTCE9hdGggSW5jMSYwJAYDVQQDDB0qLmFwaS5mYW50YXN5c3BvcnRzLnlhaG9vLmNvbTBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABIEG8APXOTQqDN3xBw/Z/FCYZ1ZoPRVVwlJQG+WTirzDAy+XfAQLQBCW+K5nSHKpAkI48c3vOV8C9AS49CvolX+jggmgMIIJnDAfBgNVHSMEGDAWgBRRaP+QrwIHdTzM2WVkYqISuFlyOzAdBgNVHQ4EFgQUsjjmG101+l038mPZwlTjiQDii+YwggZcBgNVHREEggZTMIIGT4IdKi5hcGkuZmFudGFzeXNwb3J0cy55YWhvby5jb22CCXltYWlsLmNvbYIKcy55aW1nLmNvbYILKi55YWhvby5jb22CFCouY2FsZW5kYXIueWFob28uY29tghIqLmdyb3Vwcy55YWhvby5jb22CECoubWFpbC55YWhvby5jb22CDyoubXNnLnlhaG9vLmNvbYILKi55bWFpbC5jb22CEyouZmluYW5jZS55YWhvby5jb22CECoubmV3cy55YWhvby5jb22CESoudmlkZW8ueWFob28uY29tgg0qLm0ueWFob28uY29tgg4qLm15LnlhaG9vLmNvbYISKi5zZWFyY2gueWFob28uY29tghIqLnNlY3VyZS55YWhvby5jb22CDyoueWFob29hcGlzLmNvbYITKi5tZy5tYWlsLnlhaG9vLmNvbYIZKi5mYW50YXN5c3BvcnRzLnlhaG9vLmNvbYIRKi5hdXRvcy55YWhvby5jb22CEyouY3JpY2tldC55YWhvby5jb22CIiouZm9vdGJhbGwuZmFudGFzeXNwb3J0cy55YWhvby5jb22CESouZ2FtZXMueWFob28uY29tghUqLmxpZmVzdHlsZS55YWhvby5jb22CEioubW92aWVzLnlhaG9vLmNvbYIRKi5tdWplci55YWhvby5jb22CESoubXVzaWMueWFob28uY29tghIqLnNhZmVseS55YWhvby5jb22CEiouc2NyZWVuLnlhaG9vLmNvbYIRKi5zaGluZS55YWhvby5jb22CEiouc3BvcnRzLnlhaG9vLmNvbYISKi50cmF2ZWwueWFob28uY29tgg4qLnR2LnlhaG9vLmNvbYIcKi53Yy5mYW50YXN5c3BvcnRzLnlhaG9vLmNvbYITKi53ZWF0aGVyLnlhaG9vLmNvbYITKi5ub3RlcGFkLnlhaG9vLmNvbYIOKi5wcm90cmFkZS5jb22CDyoueXFsLnlhaG9vLmNvbYIRKi53Yy55YWhvb2Rucy5uZXSCESouZGVhbHMueWFob28uY29tghAqLmhlbHAueWFob28uY29tghUqLmNlbGVicml0eS55YWhvby5jb22CFCouYXVjdGlvbnMueWFob28uY29tgg8qLnlicC55YWhvby5jb22CDyouZ2VvLnlhaG9vLmNvbYIVKi5tZXNzZW5nZXIueWFob28uY29tghQqLmFudGlzcGFtLnlhaG9vLmNvbYIPKi55c20ueWFob28uY29tghl2aWRlby5tZWRpYS55cWwueWFob28uY29tghIqLnRyaXBvZC55YWhvby5jb22CECouaXJpcy55YWhvby5jb22CEioubW9iaWxlLnlhaG9vLmNvbYIZKi5vdmVydmlldy5tYWlsLnlhaG9vLmNvbYIZKi5tYWlscGx1cy5tYWlsLnlhaG9vLmNvbYIXdGVhcnNoZWV0LmFkcy55YWhvby5jb22CESoueG9ibmkueWFob28uY29tghdvbmVwdXNoLnF1ZXJ5LnlhaG9vLmNvbYIbYXBpLW9uZXB1c2gucXVlcnkueWFob28uY29tgiFhcGkuZGlnaXRhbGhvbWVzZXJ2aWNlcy55YWhvby5jb22CF2NvbW1zZGF0YS5hcGkueWFob28uY29tghkqLmNvbW1zZGF0YS5hcGkueWFob28uY29tghVjb21tc2RhdGEuYXBpLmFvbC5jb22CFyouY29tbXNkYXRhLmFwaS5hb2wuY29tghxnYWxsZXJ5LnR2LndpZGdldHMueWFob28uY29tghQqLmNvbW1lcmNlLnlhaG9vLmNvbYIQKi52c3NwLnlhaG9vLmNvbYIUKi5zb21icmVyby55YWhvby5uZXSCFyoudHcuY2FtcGFpZ24ueWFob28uY29tghYqLmRpc3BhdGNoZXIueWFob28uY29tghBjZG4ubGF1bmNoM2QuY29tggxjZG4uanM3ay5jb22CGHZpZGVvLWFwaS5zYXBpLnlhaG9vLmNvbYIXbWFuaWZlc3Quc2FwaS55YWhvby5jb22CGCoudnRvLmNvbW1lcmNlLnlhaG9vLmNvbYIYZXMtdXMuZmluYW56YXMueWFob28uY29tghVici5maW5hbmNhcy55YWhvby5jb20wDgYDVR0PAQH/BAQDAgeAMB0GA1UdJQQWMBQGCCsGAQUFBwMBBggrBgEFBQcDAjB1BgNVHR8EbjBsMDSgMqAwhi5odHRwOi8vY3JsMy5kaWdpY2VydC5jb20vc2hhMi1oYS1zZXJ2ZXItZzYuY3JsMDSgMqAwhi5odHRwOi8vY3JsNC5kaWdpY2VydC5jb20vc2hhMi1oYS1zZXJ2ZXItZzYuY3JsMD4GA1UdIAQ3MDUwMwYGZ4EMAQICMCkwJwYIKwYBBQUHAgEWG2h0dHA6Ly93d3cuZGlnaWNlcnQuY29tL0NQUzCBgwYIKwYBBQUHAQEEdzB1MCQGCCsGAQUFBzABhhhodHRwOi8vb2NzcC5kaWdpY2VydC5jb20wTQYIKwYBBQUHMAKGQWh0dHA6Ly9jYWNlcnRzLmRpZ2ljZXJ0LmNvbS9EaWdpQ2VydFNIQTJIaWdoQXNzdXJhbmNlU2VydmVyQ0EuY3J0MAwGA1UdEwEB/wQCMAAwggF+BgorBgEEAdZ5AgQCBIIBbgSCAWoBaAB2ACl5vvCeOTkh8FZzn2Old+W+V32cYAr4+U1dJlwlXceEAAABfq12kV4AAAQDAEcwRQIgPX2Jdhj6Rfp46BK/xEO+wYlv8VV45kLzTGxUpLYhDUoCIQCuGJ8bFomPsQBnplsIX7hgeEnE/V6AEDt8CoKprh259QB3AFGjsPX9AXmcVm24N3iPDKR6zBsny/eeiEKaDf7UiwXlAAABfq12kXEAAAQDAEgwRgIhAPn2ISK7zSarSxmIh2l8+SVLGSPJVgVqY3T+c3h8pIF3AiEA5dfr11Zb+vLz/eokU3yD8rG6TuO+Sg7isLeXz6YNJRcAdQBByMqx3yJGShDGoToJQodeTjGLGwPr60vHaPCQYpYG9gAAAX6tdpEZAAAEAwBGMEQCIHyMB7fH/iw6/os/vDvnEAFH/HhETRe242V6tubs0qllAiAxM7Cm9HLPuMgcJXbrd7IfHjyefFd8tuqHJHU+ZZ10WzANBgkqhkiG9w0BAQsFAAOCAQEAjWBIZjdbFHMwYb74JtaYZQGPp1BszsxFcD3tTPupj0mQUfDl48norRbk+FxMxWLcZ5QbDYww/Z8WOhUt3EpZ1O49bk2WT7DU7ex2njFXhRq+zdXzVEvMNQyibHQO8UZwmA75TzIcU88CEjWiTRKiTsD5wQGHmU7cWcA8xwazcbbsLAf+ByX52ePEHd4o0GVYIn9a4YJS1qmQUZIHj+Hkng3fQPBBTLS+p77gxfpK0YHYqhDYtNYVNmn41aiif/IGU7hURtmgr/T9Bz3vfN19S3Imv5NsiQ6Wgw01uw9YYIUX05OkQ5pyVEXnsSrOOzJT30M0b9mpa3PYQEUBIkGLyQ==',
        'blockpage': None,
        'page_signature': None
      },
    ]
    # yapf: enable

    flattener = get_blockpage_flattener()

    rows = flattener.process((filename, json.dumps(line)))

    self.assertEqual(list(rows), expected_rows)


  def test_parse_tls_cert(self):
    from pprint import pprint
    import cryptography
    import base64
    import ssl

    received_tls_cert = "MIIMQjCCCyqgAwIBAgIQDcjI3GKsZdP0vWK4tcFMVDANBgkqhkiG9w0BAQsFADBwMQswCQYDVQQGEwJVUzEVMBMGA1UEChMMRGlnaUNlcnQgSW5jMRkwFwYDVQQLExB3d3cuZGlnaWNlcnQuY29tMS8wLQYDVQQDEyZEaWdpQ2VydCBTSEEyIEhpZ2ggQXNzdXJhbmNlIFNlcnZlciBDQTAeFw0yMjAxMzEwMDAwMDBaFw0yMjAzMjMyMzU5NTlaMHExCzAJBgNVBAYTAlVTMRMwEQYDVQQIEwpDYWxpZm9ybmlhMRIwEAYDVQQHEwlTdW5ueXZhbGUxETAPBgNVBAoTCE9hdGggSW5jMSYwJAYDVQQDDB0qLmFwaS5mYW50YXN5c3BvcnRzLnlhaG9vLmNvbTBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABIEG8APXOTQqDN3xBw/Z/FCYZ1ZoPRVVwlJQG+WTirzDAy+XfAQLQBCW+K5nSHKpAkI48c3vOV8C9AS49CvolX+jggmgMIIJnDAfBgNVHSMEGDAWgBRRaP+QrwIHdTzM2WVkYqISuFlyOzAdBgNVHQ4EFgQUsjjmG101+l038mPZwlTjiQDii+YwggZcBgNVHREEggZTMIIGT4IdKi5hcGkuZmFudGFzeXNwb3J0cy55YWhvby5jb22CCXltYWlsLmNvbYIKcy55aW1nLmNvbYILKi55YWhvby5jb22CFCouY2FsZW5kYXIueWFob28uY29tghIqLmdyb3Vwcy55YWhvby5jb22CECoubWFpbC55YWhvby5jb22CDyoubXNnLnlhaG9vLmNvbYILKi55bWFpbC5jb22CEyouZmluYW5jZS55YWhvby5jb22CECoubmV3cy55YWhvby5jb22CESoudmlkZW8ueWFob28uY29tgg0qLm0ueWFob28uY29tgg4qLm15LnlhaG9vLmNvbYISKi5zZWFyY2gueWFob28uY29tghIqLnNlY3VyZS55YWhvby5jb22CDyoueWFob29hcGlzLmNvbYITKi5tZy5tYWlsLnlhaG9vLmNvbYIZKi5mYW50YXN5c3BvcnRzLnlhaG9vLmNvbYIRKi5hdXRvcy55YWhvby5jb22CEyouY3JpY2tldC55YWhvby5jb22CIiouZm9vdGJhbGwuZmFudGFzeXNwb3J0cy55YWhvby5jb22CESouZ2FtZXMueWFob28uY29tghUqLmxpZmVzdHlsZS55YWhvby5jb22CEioubW92aWVzLnlhaG9vLmNvbYIRKi5tdWplci55YWhvby5jb22CESoubXVzaWMueWFob28uY29tghIqLnNhZmVseS55YWhvby5jb22CEiouc2NyZWVuLnlhaG9vLmNvbYIRKi5zaGluZS55YWhvby5jb22CEiouc3BvcnRzLnlhaG9vLmNvbYISKi50cmF2ZWwueWFob28uY29tgg4qLnR2LnlhaG9vLmNvbYIcKi53Yy5mYW50YXN5c3BvcnRzLnlhaG9vLmNvbYITKi53ZWF0aGVyLnlhaG9vLmNvbYITKi5ub3RlcGFkLnlhaG9vLmNvbYIOKi5wcm90cmFkZS5jb22CDyoueXFsLnlhaG9vLmNvbYIRKi53Yy55YWhvb2Rucy5uZXSCESouZGVhbHMueWFob28uY29tghAqLmhlbHAueWFob28uY29tghUqLmNlbGVicml0eS55YWhvby5jb22CFCouYXVjdGlvbnMueWFob28uY29tgg8qLnlicC55YWhvby5jb22CDyouZ2VvLnlhaG9vLmNvbYIVKi5tZXNzZW5nZXIueWFob28uY29tghQqLmFudGlzcGFtLnlhaG9vLmNvbYIPKi55c20ueWFob28uY29tghl2aWRlby5tZWRpYS55cWwueWFob28uY29tghIqLnRyaXBvZC55YWhvby5jb22CECouaXJpcy55YWhvby5jb22CEioubW9iaWxlLnlhaG9vLmNvbYIZKi5vdmVydmlldy5tYWlsLnlhaG9vLmNvbYIZKi5tYWlscGx1cy5tYWlsLnlhaG9vLmNvbYIXdGVhcnNoZWV0LmFkcy55YWhvby5jb22CESoueG9ibmkueWFob28uY29tghdvbmVwdXNoLnF1ZXJ5LnlhaG9vLmNvbYIbYXBpLW9uZXB1c2gucXVlcnkueWFob28uY29tgiFhcGkuZGlnaXRhbGhvbWVzZXJ2aWNlcy55YWhvby5jb22CF2NvbW1zZGF0YS5hcGkueWFob28uY29tghkqLmNvbW1zZGF0YS5hcGkueWFob28uY29tghVjb21tc2RhdGEuYXBpLmFvbC5jb22CFyouY29tbXNkYXRhLmFwaS5hb2wuY29tghxnYWxsZXJ5LnR2LndpZGdldHMueWFob28uY29tghQqLmNvbW1lcmNlLnlhaG9vLmNvbYIQKi52c3NwLnlhaG9vLmNvbYIUKi5zb21icmVyby55YWhvby5uZXSCFyoudHcuY2FtcGFpZ24ueWFob28uY29tghYqLmRpc3BhdGNoZXIueWFob28uY29tghBjZG4ubGF1bmNoM2QuY29tggxjZG4uanM3ay5jb22CGHZpZGVvLWFwaS5zYXBpLnlhaG9vLmNvbYIXbWFuaWZlc3Quc2FwaS55YWhvby5jb22CGCoudnRvLmNvbW1lcmNlLnlhaG9vLmNvbYIYZXMtdXMuZmluYW56YXMueWFob28uY29tghVici5maW5hbmNhcy55YWhvby5jb20wDgYDVR0PAQH/BAQDAgeAMB0GA1UdJQQWMBQGCCsGAQUFBwMBBggrBgEFBQcDAjB1BgNVHR8EbjBsMDSgMqAwhi5odHRwOi8vY3JsMy5kaWdpY2VydC5jb20vc2hhMi1oYS1zZXJ2ZXItZzYuY3JsMDSgMqAwhi5odHRwOi8vY3JsNC5kaWdpY2VydC5jb20vc2hhMi1oYS1zZXJ2ZXItZzYuY3JsMD4GA1UdIAQ3MDUwMwYGZ4EMAQICMCkwJwYIKwYBBQUHAgEWG2h0dHA6Ly93d3cuZGlnaWNlcnQuY29tL0NQUzCBgwYIKwYBBQUHAQEEdzB1MCQGCCsGAQUFBzABhhhodHRwOi8vb2NzcC5kaWdpY2VydC5jb20wTQYIKwYBBQUHMAKGQWh0dHA6Ly9jYWNlcnRzLmRpZ2ljZXJ0LmNvbS9EaWdpQ2VydFNIQTJIaWdoQXNzdXJhbmNlU2VydmVyQ0EuY3J0MAwGA1UdEwEB/wQCMAAwggF+BgorBgEEAdZ5AgQCBIIBbgSCAWoBaAB2ACl5vvCeOTkh8FZzn2Old+W+V32cYAr4+U1dJlwlXceEAAABfq12kV4AAAQDAEcwRQIgPX2Jdhj6Rfp46BK/xEO+wYlv8VV45kLzTGxUpLYhDUoCIQCuGJ8bFomPsQBnplsIX7hgeEnE/V6AEDt8CoKprh259QB3AFGjsPX9AXmcVm24N3iPDKR6zBsny/eeiEKaDf7UiwXlAAABfq12kXEAAAQDAEgwRgIhAPn2ISK7zSarSxmIh2l8+SVLGSPJVgVqY3T+c3h8pIF3AiEA5dfr11Zb+vLz/eokU3yD8rG6TuO+Sg7isLeXz6YNJRcAdQBByMqx3yJGShDGoToJQodeTjGLGwPr60vHaPCQYpYG9gAAAX6tdpEZAAAEAwBGMEQCIHyMB7fH/iw6/os/vDvnEAFH/HhETRe242V6tubs0qllAiAxM7Cm9HLPuMgcJXbrd7IfHjyefFd8tuqHJHU+ZZ10WzANBgkqhkiG9w0BAQsFAAOCAQEAjWBIZjdbFHMwYb74JtaYZQGPp1BszsxFcD3tTPupj0mQUfDl48norRbk+FxMxWLcZ5QbDYww/Z8WOhUt3EpZ1O49bk2WT7DU7ex2njFXhRq+zdXzVEvMNQyibHQO8UZwmA75TzIcU88CEjWiTRKiTsD5wQGHmU7cWcA8xwazcbbsLAf+ByX52ePEHd4o0GVYIn9a4YJS1qmQUZIHj+Hkng3fQPBBTLS+p77gxfpK0YHYqhDYtNYVNmn41aiif/IGU7hURtmgr/T9Bz3vfN19S3Imv5NsiQ6Wgw01uw9YYIUX05OkQ5pyVEXnsSrOOzJT30M0b9mpa3PYQEUBIkGLyQ=="
    
    #pem_cert = '-----BEGIN PUBLIC KEY-----\r{received_tls_cert}\r-----END PUBLIC KEY-----'
    #cert_bytes = bytes(pem_cert, encoding='utf8')
    
    cert_bytes = base64.b64decode(received_tls_cert)
    #cert_pem = ssl.DER_cert_to_PEM_cert(cert_bytes)
    cert = cryptography.x509.load_der_x509_certificate(cert_bytes)
    
    pprint(cert)
    pprint(vars(cert))
    pprint(cert.subject)
