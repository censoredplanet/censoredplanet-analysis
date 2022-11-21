"""Unit tests for satellite flattening functions"""

import json
import unittest

from pipeline.metadata.schema import SatelliteRow, SatelliteAnswer, PageFetchRow, IpMetadata, HttpsResponse, MatchesControl
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

    expected = [
        SatelliteRow(
            domain='asana.com',
            is_control=False,
            controls_failed=False,
            category='E-commerce',
            ip='67.69.184.215',
            is_control_ip=False,
            date='2020-09-02',
            retry=0,
            error=None,
            anomaly=False,
            success=True,
            received=[
                SatelliteAnswer(
                    ip='151.101.1.184',
                    matches_control=MatchesControl(
                        ip=True,
                        http=True,
                        cert=True,
                        asnum=True,
                        asname=True,
                    )),
                SatelliteAnswer(
                    ip='151.101.129.184',
                    matches_control=MatchesControl(
                        ip=True,
                        http=True,
                        cert=True,
                        asnum=True,
                        asname=True,
                    )),
                SatelliteAnswer(
                    ip='151.101.193.184',
                    matches_control=MatchesControl(
                        ip=True,
                        http=True,
                        cert=True,
                        asnum=True,
                        asname=True,
                    )),
                SatelliteAnswer(
                    ip='151.101.65.184',
                    matches_control=MatchesControl(
                        ip=True,
                        cert=True,
                        asnum=True,
                        asname=True,
                    ))
            ],
            rcode=0,
            measurement_id='ab3b0ed527334c6ba988362e6a2c98fc',
            source='CP_Satellite-2020-09-02-12-00-01'),
        SatelliteRow(
            domain='www.ecequality.org',
            is_control=False,
            controls_failed=False,
            category='LGBT',
            ip='145.239.6.50',
            is_control_ip=False,
            date='2020-09-02',
            retry=0,
            error=None,
            anomaly=True,
            success=True,
            received=[SatelliteAnswer(ip='160.153.136.3')],
            rcode=0,
            measurement_id='ab3b0ed527334c6ba988362e6a2c98fc',
            source='CP_Satellite-2020-09-02-12-00-01'),
        SatelliteRow(
            domain='www.sportsinteraction.com',
            is_control=False,
            controls_failed=False,
            category='Gambling',
            ip='185.228.168.149',
            is_control_ip=False,
            date='2020-09-02',
            retry=0,
            error='no_answer',
            anomaly=None,
            success=False,
            received=[],
            rcode=-1,
            measurement_id='ab3b0ed527334c6ba988362e6a2c98fc',
            source='CP_Satellite-2020-09-02-12-00-01'),
        SatelliteRow(
            domain='www.usacasino.com',
            is_control=False,
            controls_failed=False,
            category='Gambling',
            ip='1.1.1.1',
            is_control_ip=True,
            date='2020-09-02',
            retry=0,
            error=None,
            anomaly=None,
            success=True,
            received=[SatelliteAnswer(ip='217.19.248.132')],
            rcode=0,
            measurement_id='ab3b0ed527334c6ba988362e6a2c98fc',
            source='CP_Satellite-2020-09-02-12-00-01'),
    ]

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

    expected = [
        SatelliteRow(
            domain='abs-cbn.com',
            is_control=False,
            category='Culture',
            ip='114.114.114.110',
            is_control_ip=False,
            date='2021-03-05',
            start_time='2021-03-05T15:32:05.324807502-05:00',
            end_time='2021-03-05T15:32:05.366104911-05:00',
            retry=0,
            error=None,
            anomaly=True,
            success=True,
            controls_failed=False,
            received=[
                SatelliteAnswer(ip='104.20.161.135'),
                SatelliteAnswer(ip='104.20.161.134')
            ],
            rcode=0,
            measurement_id='ab3b0ed527334c6ba988362e6a2c98fc',
            source='CP_Satellite-2021-03-01-12-00-01',
            ip_metadata=IpMetadata(country='CN')),
        SatelliteRow(
            domain='www.example.com',
            is_control=True,
            category='Control',
            ip='8.8.4.4',
            is_control_ip=True,
            date='2021-03-15',
            start_time='2021-03-15T20:02:06.996014164-04:00',
            end_time='2021-03-15T20:02:07.002365608-04:00',
            retry=None,
            error=None,
            anomaly=None,
            success=True,
            controls_failed=False,
            received=[SatelliteAnswer(ip='93.184.216.34')],
            rcode=0,
            measurement_id='ab3b0ed527334c6ba988362e6a2c98fc',
            source='CP-Satellite-2021-03-15-12-00-01',
            has_type_a=True),
        SatelliteRow(
            domain='login.live.com',
            is_control=False,
            category='Communication Tools',
            ip='8.8.4.4',
            is_control_ip=True,
            date='2021-03-15',
            start_time='2021-03-15T20:02:07.002380008-04:00',
            end_time='2021-03-15T20:02:07.008908491-04:00',
            retry=0,
            error=None,
            anomaly=None,
            success=True,
            controls_failed=False,
            received=[
                SatelliteAnswer(ip='40.126.31.135'),
                SatelliteAnswer(ip='40.126.31.8'),
                SatelliteAnswer(ip='40.126.31.6'),
            ],
            rcode=0,
            measurement_id='ab3b0ed527334c6ba988362e6a2c98fc',
            source='CP-Satellite-2021-03-15-12-00-01',
            has_type_a=True),
        SatelliteRow(
            domain='www.example.com',
            is_control=True,
            category='Control',
            ip='8.8.4.4',
            is_control_ip=True,
            date='2021-03-15',
            start_time='2021-03-15T20:02:07.008915491-04:00',
            end_time='2021-03-15T20:02:07.015386713-04:00',
            retry=None,
            error=None,
            anomaly=None,
            success=True,
            controls_failed=False,
            received=[SatelliteAnswer(ip='93.184.216.34')],
            rcode=0,
            measurement_id='ab3b0ed527334c6ba988362e6a2c98fc',
            source='CP-Satellite-2021-03-15-12-00-01',
            has_type_a=True),
        SatelliteRow(
            domain='03.ru',
            is_control=False,
            category=None,
            ip='91.135.154.38',
            is_control_ip=False,
            date='2021-09-16',
            start_time='2021-09-16T17:49:17.488153752-04:00',
            end_time='2021-09-16T17:49:17.741250869-04:00',
            retry=0,
            error=None,
            anomaly=False,
            success=True,
            controls_failed=False,
            received=[
                SatelliteAnswer(
                    ip='88.212.202.9',
                    matches_control=MatchesControl(
                        ip=True,
                        http=True,
                        cert=True,
                        asnum=True,
                        asname=True,
                    ),
                    match_confidence=100,
                    http=
                    '8351c0267c2cd7866ff04c04261f06cd75af9a7130aac848ca43fd047404e229',
                    cert=
                    '162be4020c68591192f906bcc7c27cec0ce3eb905531bec517b97e17cc1c1c49',
                    ip_metadata=IpMetadata(asn=39134, as_name='UNITEDNET'))
            ],
            rcode=0,
            average_confidence=100,
            untagged_controls=False,
            untagged_response=False,
            excluded=False,
            exclude_reason='',
            measurement_id='ab3b0ed527334c6ba988362e6a2c98fc',
            source='CP-Satellite-2021-09-16-12-00-01',
            has_type_a=True,
            ip_metadata=IpMetadata(country='RU'))
    ]

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

    expected = SatelliteRow(
        ip='12.5.76.236',
        is_control_ip=False,
        domain='ultimate-guitar.com',
        is_control=False,
        category='History arts and literature',
        error=None,
        anomaly=True,
        success=True,
        controls_failed=False,
        received=[],
        rcode=2,
        date='2021-04-18',
        start_time='2021-04-18T14:49:07.712972288-04:00',
        end_time='2021-04-18T14:49:07.749265765-04:00',
        retry=0,
        source='CP_Satellite-2021-04-18-12-00-01',
        measurement_id='ab3b0ed527334c6ba988362e6a2c98fc',
        ip_metadata=IpMetadata(country='US'))

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

    expected = SatelliteRow(
        anomaly=True,
        category='News Media',
        controls_failed=False,
        date='2021-04-18',
        start_time='2021-04-18T14:49:01.517087379-04:00',
        end_time='2021-04-18T14:49:01.788309957-04:00',
        retry=0,
        domain='ok.ru',
        error=None,
        ip='109.69.144.30',
        is_control=False,
        is_control_ip=False,
        measurement_id='ab3b0ed527334c6ba988362e6a2c98fc',
        rcode=-1,
        received=[],
        source='CP_Satellite-2021-04-18-12-00-01',
        success=True,
        ip_metadata=IpMetadata(country='IT'))

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


    expected = [SatelliteRow(
        anomaly = False,
        category = 'E-commerce',
        average_confidence = 100,
        untagged_controls = False,
        untagged_response = False,
        controls_failed = False,
        date = '2021-10-20',
        retry=0,
        domain = '11st.co.kr',
        is_control = False,
        start_time = '2021-10-20T14:51:41.295287198-04:00',
        end_time = '2021-10-20T14:51:41.397573385-04:00',
        error = None,
        has_type_a = True,
        ip = '216.238.19.1',
        is_control_ip = False,
        measurement_id = 'ab3b0ed527334c6ba988362e6a2c98fc',
        source = 'CP-Satellite-2021-10-20-12-00-01',
        rcode = 0,
        received = [SatelliteAnswer(
            ip = '113.217.247.90',
            matches_control=MatchesControl(
                ip=True,
                http=True,
                cert=True,
                asnum=True,
                asname=True,
            ),
            match_confidence=100,
            cert = '6908c7e0f2cc9a700ddd05efc41836da3057842a6c070cdc41251504df3735f4',
            http = 'db2f9ca747f3e2e0896a1b783b27738fddfb4ba8f0500c0bfc0ad75e8f082090',
            ip_metadata=IpMetadata(
                      asn=9644,
                      as_name='SKTELECOM-NET-AS SK Telecom'
                    )
        )],
        success = True,
        exclude_reason = '',
        excluded = False,
        ip_metadata = IpMetadata(
            country = 'US',
        ),
    ), SatelliteRow(
        anomaly = False,
        category = 'Control',
        average_confidence = None,
        untagged_controls = False,
        untagged_response = False,
        controls_failed = True,
        date = '2021-10-20',
        retry=None,
        domain = 'a.root-servers.net',
        is_control = True,
        start_time = '2021-10-20T17:17:27.241422553-04:00',
        end_time = '2021-10-20T17:17:27.535929324-04:00',
        error = None,
        ip = '5.39.25.152',
        is_control_ip = False,
        measurement_id = 'ab3b0ed527334c6ba988362e6a2c98fc',
        source = 'CP-Satellite-2021-10-20-12-00-01',
        rcode = 5,
        received = [],
        success = False,
        exclude_reason = '',
        excluded = False,
        ip_metadata = IpMetadata(
            country = 'FR',
        ),
    ), SatelliteRow(
        anomaly = False,
        category = 'Provocative Attire',
        average_confidence = None,
        untagged_controls = False,
        untagged_response = False,
        controls_failed = True,
        date = '2021-10-20',
        retry=0,
        domain = 'www.chantelle.com',
        is_control = False,
        start_time = '2021-10-20T17:17:27.241422553-04:00',
        end_time = '2021-10-20T17:17:27.535929324-04:00',
        error = None,
        ip = '5.39.25.152',
        is_control_ip = False,
        measurement_id = 'ab3b0ed527334c6ba988362e6a2c98fc',
        source = 'CP-Satellite-2021-10-20-12-00-01',
        rcode = 5,
        received = [],
        success = False,
        exclude_reason = '',
        excluded = False,
        ip_metadata = IpMetadata(
            country = 'FR',
        ),
    ), SatelliteRow(
        anomaly = False,
        category = 'Control',
        average_confidence = None,
        untagged_controls = False,
        untagged_response = False,
        controls_failed = True,
        date = '2021-10-20',
        retry=None,
        domain = 'a.root-servers.net',
        is_control = True,
        start_time = '2021-10-20T17:17:27.241422553-04:00',
        end_time = '2021-10-20T17:17:27.535929324-04:00',
        error = None,
        ip = '5.39.25.152',
        is_control_ip = False,
        measurement_id = 'ab3b0ed527334c6ba988362e6a2c98fc',
        source = 'CP-Satellite-2021-10-20-12-00-01',
        rcode = 5,
        received = [],
        success = False,
        exclude_reason = '',
        excluded = False,
        ip_metadata = IpMetadata(
            country = 'FR',
        ),
    ), SatelliteRow(
        anomaly = False,
        category = 'Control',
        average_confidence = None,
        untagged_controls = False,
        untagged_response = False,
        controls_failed = True,
        date = '2021-10-20',
        retry=None,
        domain = 'a.root-servers.net',
        is_control = True,
        end_time = '2021-10-20T14:51:46.865275642-04:00',
        error = 'read udp 141.212.123.185:6280->62.80.182.26:53: read: connection refused',
        ip = '62.80.182.26',
        is_control_ip = False,
        measurement_id = 'ab3b0ed527334c6ba988362e6a2c98fc',
        source = 'CP-Satellite-2021-10-20-12-00-01',
        rcode = -1,
        received = [],
        start_time = '2021-10-20T14:51:45.952255246-04:00',
        success = False,
        exclude_reason = '',
        excluded = False,
        ip_metadata = IpMetadata(
            country = 'UA',
        ),
    ), SatelliteRow(
        anomaly = False,
        category = 'E-commerce',
        average_confidence = None,
        untagged_controls = False,
        untagged_response = False,
        controls_failed = True,
        date = '2021-10-20',
        retry=0,
        domain = 'alipay.com',
        is_control = False,
        end_time = '2021-10-20T14:51:46.865275642-04:00',
        error = 'read udp 141.212.123.185:61676->62.80.182.26:53: read: connection refused',
        ip = '62.80.182.26',
        is_control_ip = False,
        measurement_id = 'ab3b0ed527334c6ba988362e6a2c98fc',
        source = 'CP-Satellite-2021-10-20-12-00-01',
        rcode = -1,
        received = [],
        start_time = '2021-10-20T14:51:45.952255246-04:00',
        success = False,
        exclude_reason = '',
        excluded = False,
        ip_metadata = IpMetadata(
            country = 'UA',
        ),
    ), SatelliteRow(
        anomaly = False,
        category = 'E-commerce',
        average_confidence = None,
        untagged_controls = False,
        untagged_response = False,
        controls_failed = True,
        date = '2021-10-20',
        retry=1,
        domain = 'alipay.com',
        is_control = False,
        end_time = '2021-10-20T14:51:46.865275642-04:00',
        error = 'read udp 141.212.123.185:15097->62.80.182.26:53: read: connection refused',
        ip = '62.80.182.26',
        is_control_ip = False,
        measurement_id = 'ab3b0ed527334c6ba988362e6a2c98fc',
        source = 'CP-Satellite-2021-10-20-12-00-01',
        rcode = -1,
        received = [],
        start_time = '2021-10-20T14:51:45.952255246-04:00',
        success = False,
        exclude_reason = '',
        excluded = False,
        ip_metadata = IpMetadata(
            country = 'UA',
        ),
    ), SatelliteRow(
        anomaly = False,
        category = 'E-commerce',
        average_confidence = None,
        untagged_controls = False,
        untagged_response = False,
        controls_failed = True,
        date = '2021-10-20',
        retry=2,
        domain = 'alipay.com',
        is_control = False,
        end_time = '2021-10-20T14:51:46.865275642-04:00',
        error = 'read udp 141.212.123.185:45072->62.80.182.26:53: read: connection refused',
        ip = '62.80.182.26',
        is_control_ip = False,
        measurement_id = 'ab3b0ed527334c6ba988362e6a2c98fc',
        source = 'CP-Satellite-2021-10-20-12-00-01',
        rcode = -1,
        received = [],
        start_time = '2021-10-20T14:51:45.952255246-04:00',
        success = False,
        exclude_reason = '',
        excluded = False,
        ip_metadata = IpMetadata(
            country = 'UA',
        ),
    ), SatelliteRow(
        anomaly = False,
        category = 'E-commerce',
        average_confidence = None,
        untagged_controls = False,
        untagged_response = False,
        controls_failed = True,
        date = '2021-10-20',
        retry=3,
        domain = 'alipay.com',
        is_control = False,
        end_time = '2021-10-20T14:51:46.865275642-04:00',
        error = 'read udp 141.212.123.185:36765->62.80.182.26:53: read: connection refused',
        ip = '62.80.182.26',
        is_control_ip = False,
        measurement_id = 'ab3b0ed527334c6ba988362e6a2c98fc',
        source = 'CP-Satellite-2021-10-20-12-00-01',
        rcode = -1,
        received = [],
        start_time = '2021-10-20T14:51:45.952255246-04:00',
        success = False,
        exclude_reason = '',
        excluded = False,
        ip_metadata = IpMetadata(
            country = 'UA',
        ),
    ), SatelliteRow(
        anomaly = False,
        category = 'Control',
        average_confidence = None,
        untagged_controls = False,
        untagged_response = False,
        controls_failed = True,
        date = '2021-10-20',
        retry=None,
        domain = 'a.root-servers.net',
        is_control = True,
        end_time = '2021-10-20T14:51:46.865275642-04:00',
        error = 'read udp 141.212.123.185:1295->62.80.182.26:53: read: connection refused',
        ip = '62.80.182.26',
        is_control_ip = False,
        measurement_id = 'ab3b0ed527334c6ba988362e6a2c98fc',
        source = 'CP-Satellite-2021-10-20-12-00-01',
        rcode = -1,
        received = [],
        start_time = '2021-10-20T14:51:45.952255246-04:00',
        success = False,
        exclude_reason = '',
        excluded = False,
        ip_metadata = IpMetadata(
            country = 'UA',
        ),
    )]
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


    expected = [SatelliteRow(
        anomaly=False,
        category='Government',
        untagged_controls=False,
        untagged_response=False,
        controls_failed=False,
        date='2022-01-23',
        retry=0,
        domain='www.army.mil',
        is_control=False,
        start_time='2022-01-23T16:54:03.57575605-05:00',
        end_time='2022-01-23T16:54:10.009546222-05:00',
        error='read udp 141.212.123.185:61223->178.45.11.236:53: i/o timeout',
        ip='178.45.11.236',
        is_control_ip=False,
        measurement_id='ab3b0ed527334c6ba988362e6a2c98fc',
        source='CP_Satellite-2022-01-23-12-00-01',
        rcode=-1,
        received=[],
        success=False,
        exclude_reason='',
        excluded=False,
        ip_metadata = IpMetadata(
            country='RU',
        ),
    ), SatelliteRow(
        anomaly=False,
        category='Government',
        average_confidence=66.66666666666667,
        untagged_controls=False,
        untagged_response=False,
        controls_failed=False,
        date='2022-01-23',
        retry=1,
        domain='www.army.mil',
        is_control=False,
        start_time='2022-01-23T16:54:03.57575605-05:00',
        end_time='2022-01-23T16:54:10.009546222-05:00',
        error=None,
        has_type_a=True,
        ip='178.45.11.236',
        is_control_ip=False,
        measurement_id='ab3b0ed527334c6ba988362e6a2c98fc',
        source='CP_Satellite-2022-01-23-12-00-01',
        rcode=0,
        received=[SatelliteAnswer(
            ip='92.123.189.40',
            cert='',
            http='b7e803c4b738908b8c525dd7d96a49ea96c4e532ad91a027b65ba9b520a653fb',
            matches_control=MatchesControl(
                asnum=True,
                asname=True,
            ),
            match_confidence=66.66666666666667,
            ip_metadata=IpMetadata(
                asn=20940,
                as_name='AKAMAI-ASN1'
            )
        ), SatelliteAnswer(
            ip='92.123.189.41',
            cert='',
            http='65a6a40c1b153b87b20b789f0dc93442e3ed172774c5dfa77c07b5146333802e',
            matches_control=MatchesControl(
                asnum=True,
                asname=True,
            ),
            match_confidence=66.66666666666667,
            ip_metadata=IpMetadata(
                asn=20940,
                as_name='AKAMAI-ASN1'
            )
        )],
        success=True,
        exclude_reason='',
        excluded=False,
        ip_metadata = IpMetadata(
            country='RU',
        ),
    )]
    # yapf: enable

    flattener = get_satellite_flattener()
    results = []

    for filename, line in zip(filenames, interference):
      rows = flattener.process_satellite(filename, line,
                                         'ab3b0ed527334c6ba988362e6a2c98fc')
      results.extend(list(rows))

    self.assertListEqual(results, expected)

  def test_flattenmeasurement_satellite_v2p2_no_match_confidence(self) -> None:
    """Test flattening of Satellite v2.2 measurements that fail but eventually succeed."""
    filenames = [
        'gs://firehook-scans/satellite/CP_Satellite-2021-08-22-12-00-01/results.json',
    ]

    # yapf: disable
    interference = [{
        "confidence": {
            "average": 0,
            "matches": None,
            "untagged_controls": False,
            "untagged_response": False
        },
        "passed_liveness": False,
        "connect_error": False,
        "in_control_group":  True,
        "anomaly": False,
        "excluded": False,
        "vp": "8.8.8.8",
        "test_url": "zhibo8.cc",
        "start_time": "2021-08-22 14:51:20.888764605 -0400 EDT",
        "end_time": "2021-08-22 14:51:20.959570192 -0400 EDT",
        "exclude_reason": [],
        "location": {
            "country_name": "United States",
            "country_code": "US"
        },
        "response": [{
            "url": "a.root-servers.net",
            "has_type_a": False,
            "response": {},
            "error": "null",
            "rcode": -1
        }, {
            "url": "zhibo8.cc",
            "has_type_a": True,
            "response": {
                "101.37.178.168": {
                    "http": "7bb5038f4572646cb72ef3ac67762b6545b421df215b7b6b2e1b29597ba5302b",
                    "cert": "df49f4736dcaac175f585e97605e6179e188d73d85c2f1becf48e223921c19b1",
                    "asnum": 37963,
                    "asname": "CNNIC-ALIBABA-CN-NET-AP Hangzhou Alibaba Advertising Co.,Ltd.",
                    "matched": None
                }
            },
            "error": "null",
            "rcode": 0
        }, {
            "url": "a.root-servers.net",
            "has_type_a": False,
            "response": {},
            "error": "null",
            "rcode": -1
        }]
    }]
    # yapf: enable

    expected = [
        SatelliteRow(
            domain='a.root-servers.net',
            category='Control',
            ip='8.8.8.8',
            date='2021-08-22',
            retry=None,
            start_time='2021-08-22T14:51:20.888764605-04:00',
            end_time='2021-08-22T14:51:20.959570192-04:00',
            anomaly=False,
            success=False,
            is_control=True,
            controls_failed=True,
            measurement_id='ab3b0ed527334c6ba988362e6a2c98fc',
            source='CP_Satellite-2021-08-22-12-00-01',
            ip_metadata=IpMetadata(country='US'),
            rcode=-1,
            is_control_ip=True,
            untagged_controls=False,
            untagged_response=False,
            excluded=False,
            exclude_reason='',
            received=[]),
        SatelliteRow(
            domain='zhibo8.cc',
            category='Media sharing',
            ip='8.8.8.8',
            date='2021-08-22',
            retry=0,
            start_time='2021-08-22T14:51:20.888764605-04:00',
            end_time='2021-08-22T14:51:20.959570192-04:00',
            anomaly=False,
            success=True,
            is_control=False,
            controls_failed=True,
            measurement_id='ab3b0ed527334c6ba988362e6a2c98fc',
            source='CP_Satellite-2021-08-22-12-00-01',
            ip_metadata=IpMetadata(country='US',),
            rcode=0,
            is_control_ip=True,
            average_confidence=0,
            untagged_controls=False,
            untagged_response=False,
            excluded=False,
            exclude_reason='',
            has_type_a=True,
            received=[
                SatelliteAnswer(
                    ip='101.37.178.168',
                    http=
                    '7bb5038f4572646cb72ef3ac67762b6545b421df215b7b6b2e1b29597ba5302b',
                    cert=
                    'df49f4736dcaac175f585e97605e6179e188d73d85c2f1becf48e223921c19b1',
                    ip_metadata=IpMetadata(
                        asn=37963,
                        as_name=
                        'CNNIC-ALIBABA-CN-NET-AP Hangzhou Alibaba Advertising Co.,Ltd.'
                    ),
                )
            ]),
        SatelliteRow(
            domain='a.root-servers.net',
            category='Control',
            ip='8.8.8.8',
            date='2021-08-22',
            retry=None,
            start_time='2021-08-22T14:51:20.888764605-04:00',
            end_time='2021-08-22T14:51:20.959570192-04:00',
            anomaly=False,
            success=False,
            is_control=True,
            controls_failed=True,
            measurement_id='ab3b0ed527334c6ba988362e6a2c98fc',
            source='CP_Satellite-2021-08-22-12-00-01',
            ip_metadata=IpMetadata(country='US'),
            rcode=-1,
            is_control_ip=True,
            untagged_controls=False,
            untagged_response=False,
            excluded=False,
            exclude_reason='',
            received=[])
    ]

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
    expected_rows = [
      PageFetchRow(
        domain = 'xvideos.com',
        ip = '93.158.134.250',
        date = '2021-09-16',
        start_time = '2021-09-16T21:07:55.814062725-04:00',
        end_time = '2021-09-16T21:07:56.317107472-04:00',
        success = True,
        source = 'CP-Satellite-2021-09-16-12-00-01',
        https = False,
        received = HttpsResponse(
            status = '302 Moved Temporarily',
            body = None,
            headers = [
            'Content-Length: 170',
            'Content-Type: text/html',
            'Date: Fri, 17 Sep 2021 01:07:56 GMT',
            'Location: https://yandex.ru/safety/?url=xvideos.com&infectedalert=yes&fromdns=adult',
            'Server: nginx/1.10.3 (Ubuntu)'
            ],
            is_known_blockpage = None,
            page_signature = None
        )
      ),
      PageFetchRow(
        domain = 'xvideos.com',
        ip = '93.158.134.250',
        date = '2021-09-16',
        start_time = '2021-09-16T21:07:55.814062725-04:00',
        end_time = '2021-09-16T21:07:56.317107472-04:00',
        success = True,
        source = 'CP-Satellite-2021-09-16-12-00-01',
        https = True,
        error = 'Get \"https://93.158.134.250:443/\": tls: oversized record received with length 20527',
      ),
    ]
    # yapf: enable

    flattener = get_blockpage_flattener()
    rows = flattener.process((filename, json.dumps(line)))
    self.assertEqual(list(rows), expected_rows)

  def test_flattenmeasurement_blockpage_with_cert_validation(self) -> None:
    """Test parsing a blockpage after 2022-06-26 which has extra fields"""

    # yapf: disable
    line = {
        "ip": "184.25.113.140",
        "keyword": "adobe.com",
        "http": {
            "status_line": "301 Moved Permanently",
            "body": ""
        },
        "https": {
            "status_line": "301 Moved Permanently",
            "body": "",
            "TlsVersion": 771,
            "CipherSuite": 49195,
            "Certificate": [
            "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"
            ]
        },
        "fetched": True,
        "start_time": "2022-06-26 12:37:45.45643359 -0400 EDT m=+45402.040608357",
        "end_time": "2022-06-26 12:37:45.531362568 -0400 EDT m=+45402.115537351",
        "trusted_cert": True,
        "cert_hostname_match": True
    }
    # yapf: enable

    filename = 'gs://firehook-scans/satellite/CP-Satellite-2021-09-16-12-00-01/blockpages.json'

    expected_rows = [
        PageFetchRow(
            received=HttpsResponse(
                is_known_blockpage=None,
                page_signature=None,
                status='301 Moved Permanently',
                body=None),
            domain='adobe.com',
            ip='184.25.113.140',
            date='2022-06-26',
            start_time='2022-06-26T12:37:45.45643359-04:00',
            end_time='2022-06-26T12:37:45.531362568-04:00',
            success=True,
            https=False,
            source='CP-Satellite-2021-09-16-12-00-01',
            error=None),
        PageFetchRow(
            received=HttpsResponse(
                is_known_blockpage=None,
                page_signature=None,
                status='301 Moved Permanently',
                body=None,
                tls_version=771,
                tls_cipher_suite=49195,
                tls_cert=
                'MIIFwzCCBKugAwIBAgIQCeK9ffFaGC3s+NA4gQLAPzANBgkqhkiG9w0BAQsFADBNMQswCQYDVQQGEwJVUzEVMBMGA1UEChMMRGlnaUNlcnQgSW5jMScwJQYDVQQDEx5EaWdpQ2VydCBTSEEyIFNlY3VyZSBTZXJ2ZXIgQ0EwHhcNMjExMjA5MDAwMDAwWhcNMjIxMjA5MjM1OTU5WjBfMQswCQYDVQQGEwJVUzETMBEGA1UECBMKQ2FsaWZvcm5pYTERMA8GA1UEBxMIU2FuIEpvc2UxEjAQBgNVBAoTCUFkb2JlIEluYzEUMBIGA1UEAwwLKi5hZG9iZS5jb20wWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAASxdIr6pLGBNvCnxEJOzliAHSEQ9TfGEeXQQg1ehNzlXS46VC7Qg+yy3JFAYEgfJczkzHSF3DCtwyRJlW4eKmERo4IDVjCCA1IwHwYDVR0jBBgwFoAUD4BhHIIxYdUvKOeNRji0LOHG2eIwHQYDVR0OBBYEFO9Jjige0jwgPB/U/sRy45LLvjd1MCEGA1UdEQQaMBiCCyouYWRvYmUuY29tgglhZG9iZS5jb20wDgYDVR0PAQH/BAQDAgeAMB0GA1UdJQQWMBQGCCsGAQUFBwMBBggrBgEFBQcDAjBvBgNVHR8EaDBmMDGgL6AthitodHRwOi8vY3JsMy5kaWdpY2VydC5jb20vc3NjYS1zaGEyLWc2LTEuY3JsMDGgL6AthitodHRwOi8vY3JsNC5kaWdpY2VydC5jb20vc3NjYS1zaGEyLWc2LTEuY3JsMD4GA1UdIAQ3MDUwMwYGZ4EMAQICMCkwJwYIKwYBBQUHAgEWG2h0dHA6Ly93d3cuZGlnaWNlcnQuY29tL0NQUzB8BggrBgEFBQcBAQRwMG4wJAYIKwYBBQUHMAGGGGh0dHA6Ly9vY3NwLmRpZ2ljZXJ0LmNvbTBGBggrBgEFBQcwAoY6aHR0cDovL2NhY2VydHMuZGlnaWNlcnQuY29tL0RpZ2lDZXJ0U0hBMlNlY3VyZVNlcnZlckNBLmNydDAMBgNVHRMBAf8EAjAAMIIBfwYKKwYBBAHWeQIEAgSCAW8EggFrAWkAdgBGpVXrdfqRIDC1oolp9PN9ESxBdL79SbiFq/L8cP5tRwAAAX2cgnwtAAAEAwBHMEUCIQDW06vzR0rYTV4EO30q646eCJ1rTXtJoO2zwFYU0oXGDQIgbKDg4DkaZuQMMysB8Lc0mCHzglnxihN1fzQlr24KAYEAdgBRo7D1/QF5nFZtuDd4jwykeswbJ8v3nohCmg3+1IsF5QAAAX2cgnxOAAAEAwBHMEUCIG/94rVQnxq4LYMwp1gD8fZ9PcPcGFI+J5Hq97W6jCpsAiEAybXymAGSgJ0h2as6kj/b5tyhYnVSl50i1AmBrvmxBiYAdwBByMqx3yJGShDGoToJQodeTjGLGwPr60vHaPCQYpYG9gAAAX2cgnwuAAAEAwBIMEYCIQCHJwOdLh1Fmd5ZhyCWtlmop7Dhl0WQarhVJaD1n5MpbAIhAOKyKQNDRimHhbxjk/aNhh10ZSefAzlk6w0Cpa8SBzWNMA0GCSqGSIb3DQEBCwUAA4IBAQBfJcLcGVds+2Wa5KPIQ5+sPHphhEHBm2tN1FWU70XMN0YNBw60SBT/jsEIcOI1kE75Oqm+iv1x/UEJK7+6i2qCS++R/mkmf7zcIcj6maZmy3H1CAMj4DzalVRwp9zTzA2xVRRSMzi2SsW8BJj4rL8wFy1VhW0KYWdxmHyVoLs4dBrBJsvqQiUiMhDzReIxNjJ+CJO+iLtWOfZTbfVnlhAx5/4Uukl/mOfByoVOH0xQVP9c910wAzrzL1iGM9qi8bQPzTAPhG7WjLwJW7v6pXWXwuDW5XIQ+UPbBsFsV/PJBy4McvOpH3TDsiKtgh+79bWrCETncyyJq55dI9vsfhWY',
                tls_cert_common_name='*.adobe.com',
                tls_cert_issuer='DigiCert SHA2 Secure '
                'Server CA',
                tls_cert_start_date='2021-12-09T00:00:00',
                tls_cert_end_date='2022-12-09T23:59:59',
                tls_cert_alternative_names=['*.adobe.com', 'adobe.com'],
                tls_cert_has_trusted_ca=True,
                tls_cert_matches_domain=True),
            domain='adobe.com',
            ip='184.25.113.140',
            date='2022-06-26',
            start_time='2022-06-26T12:37:45.45643359-04:00',
            end_time='2022-06-26T12:37:45.531362568-04:00',
            success=True,
            https=True,
            source='CP-Satellite-2021-09-16-12-00-01',
            error=None)
    ]

    flattener = get_blockpage_flattener()
    rows = flattener.process((filename, json.dumps(line)))
    self.assertEqual(list(rows), expected_rows)
