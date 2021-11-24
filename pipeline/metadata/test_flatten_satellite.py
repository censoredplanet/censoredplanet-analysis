"""Unit tests for satellite flattening functions"""

import unittest
from typing import List

from pipeline.metadata.flatten_base import Row
from pipeline.metadata.blockpage import BlockpageMatcher
from pipeline.metadata.domain_categories import DomainCategoryMatcher

from pipeline.metadata import flatten_satellite


def get_satellite_flattener() -> flatten_satellite.SatelliteFlattener:
  blockpage_matcher = BlockpageMatcher()
  category_matcher = DomainCategoryMatcher()
  return flatten_satellite.SatelliteFlattener(blockpage_matcher,
                                              category_matcher)


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
        'category': 'E-commerce',
        'ip': '67.69.184.215',
        'is_control_ip': False,
        'date': '2020-09-02',
        'error': None,
        'anomaly': False,
        'success': True,
        'received': {
            'ip': '151.101.1.184',
            'matches_control': 'ip http cert asnum asname'
        },
        'rcode': ['0'],
        'measurement_id': 'ab3b0ed527334c6ba988362e6a2c98fc'
    }, {
        'domain': 'asana.com',
        'is_control': False,
        'category': 'E-commerce',
        'ip': '67.69.184.215',
        'is_control_ip': False,
        'date': '2020-09-02',
        'error': None,
        'anomaly': False,
        'success': True,
        'received': {
            'ip': '151.101.129.184',
            'matches_control': 'ip http cert asnum asname'
        },
        'rcode': ['0'],
        'measurement_id': 'ab3b0ed527334c6ba988362e6a2c98fc'
    }, {
        'domain': 'asana.com',
        'is_control': False,
        'category': 'E-commerce',
        'ip': '67.69.184.215',
        'is_control_ip': False,
        'date': '2020-09-02',
        'error': None,
        'anomaly': False,
        'success': True,
        'received': {
            'ip': '151.101.193.184',
            'matches_control': 'ip http cert asnum asname'
        },
        'rcode': ['0'],
        'measurement_id': 'ab3b0ed527334c6ba988362e6a2c98fc'
    }, {
        'domain': 'asana.com',
        'is_control': False,
        'category': 'E-commerce',
        'ip': '67.69.184.215',
        'is_control_ip': False,
        'date': '2020-09-02',
        'error': None,
        'anomaly': False,
        'success': True,
        'received': {
            'ip': '151.101.65.184',
            'matches_control': 'ip cert asnum asname'
        },
        'rcode': ['0'],
        'measurement_id': 'ab3b0ed527334c6ba988362e6a2c98fc'
    }, {
        'domain': 'www.ecequality.org',
        'is_control': False,
        'category': 'LGBT',
        'ip': '145.239.6.50',
        'is_control_ip': False,
        'date': '2020-09-02',
        'error': None,
        'anomaly': True,
        'success': True,
        'received': {
            'ip': '160.153.136.3',
            'matches_control': ''
        },
        'rcode': ['0'],
        'measurement_id': 'ab3b0ed527334c6ba988362e6a2c98fc'
    }, {
        'domain': 'www.sportsinteraction.com',
        'is_control': False,
        'category': 'Gambling',
        'ip': '185.228.168.149',
        'is_control_ip': False,
        'date': '2020-09-02',
        'error': 'no_answer',
        'anomaly': None,
        'success': False,
        'received': None,
        'rcode': ['-1'],
        'measurement_id': 'ab3b0ed527334c6ba988362e6a2c98fc'
    }, {
        'domain': 'www.usacasino.com',
        'is_control': False,
        'category': 'Gambling',
        'ip': '1.1.1.1',
        'is_control_ip': True,
        'date': '2020-09-02',
        'error': None,
        'anomaly': None,
        'success': True,
        'received': {
            'ip': '217.19.248.132'
        },
        'rcode': ['0'],
        'measurement_id': 'ab3b0ed527334c6ba988362e6a2c98fc'
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
            "rcode": ["0", "0", "0"]
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
        'received': {
            'ip': '104.20.161.135',
            'matches_control': ''
        },
        'rcode': ["0", "0", "0"],
        'measurement_id': 'ab3b0ed527334c6ba988362e6a2c98fc'
    }, {
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
        'received': {
            'ip': '104.20.161.134',
            'matches_control': ''
        },
        'rcode': ["0", "0", "0"],
        'measurement_id': 'ab3b0ed527334c6ba988362e6a2c98fc'
    }, {
        'domain': 'login.live.com',
        'is_control': False,
        'category': 'Communication Tools',
        'ip': '8.8.4.4',
        'is_control_ip': True,
        'date': '2021-03-15',
        'start_time': '2021-03-15T20:02:06.996014164-04:00',
        'end_time': '2021-03-15T20:02:07.015386713-04:00',
        'error': None,
        'anomaly': None,
        'success': True,
        'controls_failed': False,
        'received': {
            'ip': '40.126.31.135'
        },
        'rcode': ['0', '0', '0'],
        'measurement_id': 'ab3b0ed527334c6ba988362e6a2c98fc',
        'has_type_a': True
    }, {
        'domain': 'login.live.com',
        'is_control': False,
        'category': 'Communication Tools',
        'ip': '8.8.4.4',
        'is_control_ip': True,
        'date': '2021-03-15',
        'start_time': '2021-03-15T20:02:06.996014164-04:00',
        'end_time': '2021-03-15T20:02:07.015386713-04:00',
        'error': None,
        'anomaly': None,
        'success': True,
        'controls_failed': False,
        'received': {
            'ip': '40.126.31.8'
        },
        'rcode': ['0', '0', '0'],
        'measurement_id': 'ab3b0ed527334c6ba988362e6a2c98fc',
        'has_type_a': True
    }, {
        'domain': 'login.live.com',
        'is_control': False,
        'category': 'Communication Tools',
        'ip': '8.8.4.4',
        'is_control_ip': True,
        'date': '2021-03-15',
        'start_time': '2021-03-15T20:02:06.996014164-04:00',
        'end_time': '2021-03-15T20:02:07.015386713-04:00',
        'error': None,
        'anomaly': None,
        'success': True,
        'controls_failed': False,
        'received': {
            'ip': '40.126.31.6'
        },
        'rcode': ['0', '0', '0'],
        'measurement_id': 'ab3b0ed527334c6ba988362e6a2c98fc',
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
        'rcode': ['0'],
        'confidence': {
            'average': 100,
            'matches': [100],
            'untagged_controls': False,
            'untagged_response': False
        },
        'verify': {
            'excluded': False,
            'exclude_reason': '',
        },
        'measurement_id': 'ab3b0ed527334c6ba988362e6a2c98fc',
        'has_type_a': True
    }]

    flattener = get_satellite_flattener()
    results = []

    for filename, line in zip(filenames, interference):
      rows = flattener.process_satellite(filename, line,
                                         'ab3b0ed527334c6ba988362e6a2c98fc')
      results.extend(list(rows))

    self.assertListEqual(results, expected)

  def test_flattenmeasurement_satellite_v2p2(self) -> None:
    """Test flattening of Satellite v2.2 measurements."""
    filenames = [
        'gs://firehook-scans/satellite/CP-Satellite-2021-10-20-12-00-01/results.json',
        'gs://firehook-scans/satellite/CP-Satellite-2021-10-20-12-00-01/results.json',
        'gs://firehook-scans/satellite/CP-Satellite-2021-10-20-12-00-01/results.json'
    ]

    interference = [{
        "confidence": {
            "average": 100,
            "matches": [100],
            "untagged_controls": False,
            "untagged_response": False
        },
        "passed_liveness":
            True,
        "connect_error":
            False,
        "in_control_group":
            True,
        "anomaly":
            False,
        "excluded":
            False,
        "vp":
            "216.238.19.1",
        "test_url":
            "11st.co.kr",
        "start_time":
            "2021-10-20 14:51:41.295287198 -0400 EDT",
        "end_time":
            "2021-10-20 14:51:41.397573385 -0400 EDT",
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
                    "http":
                        "db2f9ca747f3e2e0896a1b783b27738fddfb4ba8f0500c0bfc0ad75e8f082090",
                    "cert":
                        "6908c7e0f2cc9a700ddd05efc41836da3057842a6c070cdc41251504df3735f4",
                    "asnum":
                        9644,
                    "asname":
                        "SKTELECOM-NET-AS SK Telecom",
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
        "passed_liveness":
            False,
        "connect_error":
            False,
        "in_control_group":
            True,
        "anomaly":
            False,
        "excluded":
            False,
        "vp":
            "5.39.25.152",
        "test_url":
            "www.chantelle.com",
        "start_time":
            "2021-10-20 17:17:27.241422553 -0400 EDT",
        "end_time":
            "2021-10-20 17:17:27.535929324 -0400 EDT",
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
        "passed_liveness":
            False,
        "connect_error":
            True,
        "in_control_group":
            True,
        "anomaly":
            False,
        "excluded":
            False,
        "vp":
            "62.80.182.26",
        "test_url":
            "alipay.com",
        "start_time":
            "2021-10-20 14:51:45.952255246 -0400 EDT",
        "end_time":
            "2021-10-20 14:51:46.865275642 -0400 EDT",
        "exclude_reason": [],
        "location": {
            "country_name": "Ukraine",
            "country_code": "UA"
        },
        "response": [{
            "url":
                "a.root-servers.net",
            "has_type_a":
                False,
            "response": {},
            "error":
                "read udp 141.212.123.185:6280->62.80.182.26:53: read: connection refused",
            "rcode":
                -1
        }, {
            "url":
                "alipay.com",
            "has_type_a":
                False,
            "response": {},
            "error":
                "read udp 141.212.123.185:61676->62.80.182.26:53: read: connection refused",
            "rcode":
                -1
        }, {
            "url":
                "alipay.com",
            "has_type_a":
                False,
            "response": {},
            "error":
                "read udp 141.212.123.185:15097->62.80.182.26:53: read: connection refused",
            "rcode":
                -1
        }, {
            "url":
                "alipay.com",
            "has_type_a":
                False,
            "response": {},
            "error":
                "read udp 141.212.123.185:45072->62.80.182.26:53: read: connection refused",
            "rcode":
                -1
        }, {
            "url":
                "alipay.com",
            "has_type_a":
                False,
            "response": {},
            "error":
                "read udp 141.212.123.185:36765->62.80.182.26:53: read: connection refused",
            "rcode":
                -1
        }, {
            "url":
                "a.root-servers.net",
            "has_type_a":
                False,
            "response": {},
            "error":
                "read udp 141.212.123.185:1295->62.80.182.26:53: read: connection refused",
            "rcode":
                -1
        }]
    }]

    expected = [{
        'anomaly': False,
        'category': 'E-commerce',
        'confidence': {
            'average': 100,
            'matches': [100],
            'untagged_controls': False,
            'untagged_response': False
        },
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
        'rcode': ['0'],
        'received': [{
            'asname':
                'SKTELECOM-NET-AS SK Telecom',
            'asnum':
                9644,
            'cert':
                '6908c7e0f2cc9a700ddd05efc41836da3057842a6c070cdc41251504df3735f4',
            'http':
                'db2f9ca747f3e2e0896a1b783b27738fddfb4ba8f0500c0bfc0ad75e8f082090',
            'ip':
                '113.217.247.90',
            'matches_control':
                'ip http cert asnum asname'
        }],
        'success': True,
        'verify': {
            'exclude_reason': '',
            'excluded': False
        },
    }, {
        'anomaly': False,
        'category': 'Provocative Attire',
        'confidence': {
            'average': 0,
            'matches': None,
            'untagged_controls': False,
            'untagged_response': False
        },
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
        'rcode': ['5', '5', '5'],
        'received': None,
        'success': True,
        'verify': {
            'exclude_reason': '',
            'excluded': False
        }
    }, {
        'anomaly': False,
        'category': 'E-commerce',
        'confidence': {
            'average': 0,
            'matches': None,
            'untagged_controls': False,
            'untagged_response': False
        },
        'controls_failed': True,
        'country': 'UA',
        'date': '2021-10-20',
        'domain': 'alipay.com',
        'is_control': False,
        'end_time': '2021-10-20T14:51:46.865275642-04:00',
        'error':
            'read udp 141.212.123.185:6280->62.80.182.26:53: read: connection '
            'refused | read udp 141.212.123.185:61676->62.80.182.26:53: read: '
            'connection refused | read udp '
            '141.212.123.185:15097->62.80.182.26:53: read: connection refused | '
            'read udp 141.212.123.185:45072->62.80.182.26:53: read: connection '
            'refused | read udp 141.212.123.185:36765->62.80.182.26:53: read: '
            'connection refused | read udp '
            '141.212.123.185:1295->62.80.182.26:53: read: connection refused',
        'ip': '62.80.182.26',
        'is_control_ip': False,
        'measurement_id': 'ab3b0ed527334c6ba988362e6a2c98fc',
        'rcode': ['-1', '-1', '-1', '-1', '-1', '-1'],
        'received': None,
        'start_time': '2021-10-20T14:51:45.952255246-04:00',
        'success': False,
        'verify': {
            'exclude_reason': '',
            'excluded': False
        }
    }]

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
    expected_rows: List[Row] = [
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
        'blockpage': None,
        'page_signature': None
      },
    ]
    # yapf: enable

    flattener = get_satellite_flattener()

    rows = flattener.process_satellite(filename, line,
                                       'ab3b0ed527334c6ba988362e6a2c98fc')

    self.assertEqual(list(rows), expected_rows)
