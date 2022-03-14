import unittest

#from pipeline.metadata.flatten_base import Row, SatelliteAnswer
from pprint import pprint


class FormatTest(unittest.TestCase):

  def test_format(self) -> None:

    self.maxDiff = None

    test1 = [{
        'domain':
            '1337x.to',
        'is_control':
            False,
        'category':
            'Media sharing',
        'ip':
            '8.8.8.8',
        'is_control_ip':
            True,
        'country':
            'US',
        'date':
            '2022-01-02',
        'start_time':
            '2022-01-02T14:47:22.608859091-05:00',
        'end_time':
            '2022-01-02T14:47:22.987814778-05:00',
        'error':
            None,
        'anomaly':
            False,
        'success':
            True,
        'source':
            'CP_Satellite-2022-01-02-12-00-01',
        'controls_failed':
            False,
        'rcode':
            0,
        'average_confidence':
            100,
        'matches_confidence': [100, 100],
        'untagged_controls':
            False,
        'untagged_response':
            False,
        'excluded':
            False,
        'exclude_reason':
            '',
        'has_type_a':
            True,
        'received': [{
            'ip':
                '104.31.16.11',
            'http':
                'ecd1a8f3bd8db93d2d69e957cd3a114b43e8ba452d5cb2239f8eb6f6b92574ab',
            'cert':
                '',
            'asname':
                'CLOUDFLARENET',
            'asnum':
                13335,
            'matches_control':
                'ip http asnum asname'
        }, {
            'ip':
                '104.31.16.118',
            'http':
                '7255d6747fcfdc1c16a30c0da7f039571d8a1bdefe2f56fa0ca243fc684fbbb8',
            'cert':
                '',
            'asname':
                'CLOUDFLARENET',
            'asnum':
                13335,
            'matches_control':
                'ip http asnum asname'
        }]
    }, {
        'domain':
            '1337x.to',
        'is_control':
            False,
        'category':
            'Media sharing',
        'ip':
            '8.8.4.4',
        'is_control_ip':
            True,
        'country':
            'US',
        'date':
            '2022-01-02',
        'start_time':
            '2022-01-02T14:47:22.609624461-05:00',
        'end_time':
            '2022-01-02T14:47:22.98110208-05:00',
        'error':
            None,
        'anomaly':
            False,
        'success':
            True,
        'source':
            'CP_Satellite-2022-01-02-12-00-01',
        'controls_failed':
            True,
        'rcode':
            0,
        'average_confidence':
            0,
        'matches_confidence':
            None,
        'untagged_controls':
            False,
        'untagged_response':
            False,
        'excluded':
            False,
        'exclude_reason':
            '',
        'has_type_a':
            True,
        'received': [{
            'ip':
                '104.31.16.11',
            'http':
                'ecd1a8f3bd8db93d2d69e957cd3a114b43e8ba452d5cb2239f8eb6f6b92574ab',
            'cert':
                '',
            'asname':
                'CLOUDFLARENET',
            'asnum':
                13335,
            'matches_control':
                ''
        }, {
            'ip':
                '104.31.16.118',
            'http':
                '7255d6747fcfdc1c16a30c0da7f039571d8a1bdefe2f56fa0ca243fc684fbbb8',
            'cert':
                '',
            'asname':
                'CLOUDFLARENET',
            'asnum':
                13335,
            'matches_control':
                ''
        }]
    }, {
        'domain':
            '1337x.to',
        'is_control':
            False,
        'category':
            'Media sharing',
        'ip':
            '64.6.64.6',
        'is_control_ip':
            True,
        'country':
            'US',
        'date':
            '2022-01-02',
        'start_time':
            '2022-01-02T16:41:54.579216934-05:00',
        'end_time':
            '2022-01-02T16:41:54.617330171-05:00',
        'error':
            None,
        'anomaly':
            False,
        'success':
            True,
        'source':
            'CP_Satellite-2022-01-02-12-00-01',
        'controls_failed':
            False,
        'rcode':
            0,
        'average_confidence':
            100,
        'matches_confidence': [100, 100],
        'untagged_controls':
            False,
        'untagged_response':
            False,
        'excluded':
            False,
        'exclude_reason':
            '',
        'has_type_a':
            True,
        'received': [{
            'ip':
                '104.31.16.11',
            'http':
                'ecd1a8f3bd8db93d2d69e957cd3a114b43e8ba452d5cb2239f8eb6f6b92574ab',
            'cert':
                '',
            'asname':
                'CLOUDFLARENET',
            'asnum':
                13335,
            'matches_control':
                'ip http asnum asname'
        }, {
            'ip':
                '104.31.16.118',
            'http':
                '7255d6747fcfdc1c16a30c0da7f039571d8a1bdefe2f56fa0ca243fc684fbbb8',
            'cert':
                '',
            'asname':
                'CLOUDFLARENET',
            'asnum':
                13335,
            'matches_control':
                'ip http asnum asname'
        }]
    }, {
        'domain':
            '1337x.to',
        'is_control':
            False,
        'category':
            'Media sharing',
        'ip':
            '64.6.65.6',
        'is_control_ip':
            True,
        'country':
            'US',
        'date':
            '2022-01-02',
        'start_time':
            '2022-01-02T15:08:04.399147076-05:00',
        'end_time':
            '2022-01-02T15:08:04.437950734-05:00',
        'error':
            None,
        'anomaly':
            False,
        'success':
            True,
        'source':
            'CP_Satellite-2022-01-02-12-00-01',
        'controls_failed':
            False,
        'rcode':
            0,
        'average_confidence':
            100,
        'matches_confidence': [100, 100],
        'untagged_controls':
            False,
        'untagged_response':
            False,
        'excluded':
            False,
        'exclude_reason':
            '',
        'has_type_a':
            True,
        'received': [{
            'ip':
                '104.31.16.11',
            'http':
                'ecd1a8f3bd8db93d2d69e957cd3a114b43e8ba452d5cb2239f8eb6f6b92574ab',
            'cert':
                '',
            'asname':
                'CLOUDFLARENET',
            'asnum':
                13335,
            'matches_control':
                'ip http asnum asname'
        }, {
            'ip':
                '104.31.16.118',
            'http':
                '7255d6747fcfdc1c16a30c0da7f039571d8a1bdefe2f56fa0ca243fc684fbbb8',
            'cert':
                '',
            'asname':
                'CLOUDFLARENET',
            'asnum':
                13335,
            'matches_control':
                'ip http asnum asname'
        }]
    }, {
        'domain':
            '1337x.to',
        'is_control':
            False,
        'category':
            'Media sharing',
        'ip':
            '77.247.174.150',
        'is_control_ip':
            False,
        'country':
            'RU',
        'date':
            '2022-01-02',
        'start_time':
            '2022-01-02T14:47:22.708705995-05:00',
        'end_time':
            '2022-01-02T14:47:22.983863812-05:00',
        'error':
            None,
        'anomaly':
            True,
        'success':
            True,
        'source':
            'CP_Satellite-2022-01-02-12-00-01',
        'controls_failed':
            False,
        'rcode':
            0,
        'average_confidence':
            0,
        'matches_confidence': [0],
        'untagged_controls':
            False,
        'untagged_response':
            False,
        'excluded':
            False,
        'exclude_reason':
            '',
        'has_type_a':
            True,
        'received': [{
            'ip':
                '188.186.157.49',
            'http':
                '177a8341782a57778766a7334d3e99ecb61ce54bbcc48838ddda846ea076726d',
            'cert':
                '',
            'asname':
                'ERTELECOM-DC-AS',
            'asnum':
                31483,
            'matches_control':
                ''
        }]
    }]
    test2 = [{
        'domain':
            '1337x.to',
        'is_control':
            False,
        'category':
            'Media sharing',
        'ip':
            '77.247.174.150',
        'is_control_ip':
            False,
        'country':
            'RU',
        'date':
            '2022-01-02',
        'start_time':
            '2022-01-02T14:47:22.708705995-05:00',
        'end_time':
            '2022-01-02T14:47:22.983863812-05:00',
        'error':
            None,
        'anomaly':
            True,
        'success':
            True,
        'source':
            'CP_Satellite-2022-01-02-12-00-01',
        'controls_failed':
            False,
        'rcode':
            0,
        'average_confidence':
            0,
        'matches_confidence': [0],
        'untagged_controls':
            False,
        'untagged_response':
            False,
        'excluded':
            False,
        'exclude_reason':
            '',
        'has_type_a':
            True,
        'received': [{
            'ip':
                '188.186.157.49',
            'http':
                '177a8341782a57778766a7334d3e99ecb61ce54bbcc48838ddda846ea076726d',
            'cert':
                '',
            'asname':
                'ERTELECOM-DC-AS',
            'asnum':
                31483,
            'matches_control':
                ''
        }]
    }, {
        'domain':
            '1337x.to',
        'is_control':
            False,
        'category':
            'Media sharing',
        'ip':
            '8.8.8.8',
        'is_control_ip':
            True,
        'country':
            'US',
        'date':
            '2022-01-02',
        'start_time':
            '2022-01-02T14:47:22.608859091-05:00',
        'end_time':
            '2022-01-02T14:47:22.987814778-05:00',
        'error':
            None,
        'anomaly':
            False,
        'success':
            True,
        'source':
            'CP_Satellite-2022-01-02-12-00-01',
        'controls_failed':
            False,
        'rcode':
            0,
        'average_confidence':
            100.0,
        'matches_confidence': [100.0, 100.0],
        'untagged_controls':
            False,
        'untagged_response':
            False,
        'excluded':
            None,
        'exclude_reason':
            None,
        'has_type_a':
            True,
        'received': [{
            'ip':
                '104.31.16.11',
            'http':
                'ecd1a8f3bd8db93d2d69e957cd3a114b43e8ba452d5cb2239f8eb6f6b92574ab',
            'cert':
                '',
            'asname':
                'CLOUDFLARENET',
            'asnum':
                13335,
            'matches_control':
                'ip http asnum asname'
        }, {
            'ip':
                '104.31.16.118',
            'http':
                '7255d6747fcfdc1c16a30c0da7f039571d8a1bdefe2f56fa0ca243fc684fbbb8',
            'cert':
                '',
            'asname':
                'CLOUDFLARENET',
            'asnum':
                13335,
            'matches_control':
                'ip http asnum asname'
        }]
    }, {
        'domain':
            '1337x.to',
        'is_control':
            False,
        'category':
            'Media sharing',
        'ip':
            '8.8.4.4',
        'is_control_ip':
            True,
        'country':
            'US',
        'date':
            '2022-01-02',
        'start_time':
            '2022-01-02T14:47:22.609624461-05:00',
        'end_time':
            '2022-01-02T14:47:22.98110208-05:00',
        'error':
            None,
        'anomaly':
            False,
        'success':
            True,
        'source':
            'CP_Satellite-2022-01-02-12-00-01',
        'controls_failed':
            True,
        'rcode':
            0,
        'average_confidence':
            0.0,
        'matches_confidence': [0.0, 0.0],
        'untagged_controls':
            False,
        'untagged_response':
            False,
        'excluded':
            None,
        'exclude_reason':
            None,
        'has_type_a':
            True,
        'received': [{
            'ip':
                '104.31.16.11',
            'http':
                'ecd1a8f3bd8db93d2d69e957cd3a114b43e8ba452d5cb2239f8eb6f6b92574ab',
            'cert':
                '',
            'asname':
                'CLOUDFLARENET',
            'asnum':
                13335,
            'matches_control':
                ''
        }, {
            'ip':
                '104.31.16.118',
            'http':
                '7255d6747fcfdc1c16a30c0da7f039571d8a1bdefe2f56fa0ca243fc684fbbb8',
            'cert':
                '',
            'asname':
                'CLOUDFLARENET',
            'asnum':
                13335,
            'matches_control':
                ''
        }]
    }, {
        'domain':
            '1337x.to',
        'is_control':
            False,
        'category':
            'Media sharing',
        'ip':
            '64.6.64.6',
        'is_control_ip':
            True,
        'country':
            'US',
        'date':
            '2022-01-02',
        'start_time':
            '2022-01-02T16:41:54.579216934-05:00',
        'end_time':
            '2022-01-02T16:41:54.617330171-05:00',
        'error':
            None,
        'anomaly':
            False,
        'success':
            True,
        'source':
            'CP_Satellite-2022-01-02-12-00-01',
        'controls_failed':
            False,
        'rcode':
            0,
        'average_confidence':
            100.0,
        'matches_confidence': [100.0, 100.0],
        'untagged_controls':
            False,
        'untagged_response':
            False,
        'excluded':
            None,
        'exclude_reason':
            None,
        'has_type_a':
            True,
        'received': [{
            'ip':
                '104.31.16.11',
            'http':
                'ecd1a8f3bd8db93d2d69e957cd3a114b43e8ba452d5cb2239f8eb6f6b92574ab',
            'cert':
                '',
            'asname':
                'CLOUDFLARENET',
            'asnum':
                13335,
            'matches_control':
                'ip http asnum asname'
        }, {
            'ip':
                '104.31.16.118',
            'http':
                '7255d6747fcfdc1c16a30c0da7f039571d8a1bdefe2f56fa0ca243fc684fbbb8',
            'cert':
                '',
            'asname':
                'CLOUDFLARENET',
            'asnum':
                13335,
            'matches_control':
                'ip http asnum asname'
        }]
    }, {
        'domain':
            '1337x.to',
        'is_control':
            False,
        'category':
            'Media sharing',
        'ip':
            '64.6.65.6',
        'is_control_ip':
            True,
        'country':
            'US',
        'date':
            '2022-01-02',
        'start_time':
            '2022-01-02T15:08:04.399147076-05:00',
        'end_time':
            '2022-01-02T15:08:04.437950734-05:00',
        'error':
            None,
        'anomaly':
            False,
        'success':
            True,
        'source':
            'CP_Satellite-2022-01-02-12-00-01',
        'controls_failed':
            False,
        'rcode':
            0,
        'average_confidence':
            100.0,
        'matches_confidence': [100.0, 100.0],
        'untagged_controls':
            False,
        'untagged_response':
            False,
        'excluded':
            None,
        'exclude_reason':
            None,
        'has_type_a':
            True,
        'received': [{
            'ip':
                '104.31.16.11',
            'http':
                'ecd1a8f3bd8db93d2d69e957cd3a114b43e8ba452d5cb2239f8eb6f6b92574ab',
            'cert':
                '',
            'asname':
                'CLOUDFLARENET',
            'asnum':
                13335,
            'matches_control':
                'ip http asnum asname'
        }, {
            'ip':
                '104.31.16.118',
            'http':
                '7255d6747fcfdc1c16a30c0da7f039571d8a1bdefe2f56fa0ca243fc684fbbb8',
            'cert':
                '',
            'asname':
                'CLOUDFLARENET',
            'asnum':
                13335,
            'matches_control':
                'ip http asnum asname'
        }]
    }]

    domains1 = [x['domain'] for x in test1]
    domains2 = [x['domain'] for x in test2]

    self.assertCountEqual(domains1, domains2)

    test1 = sorted(test1, key=lambda row: row['start_time'])
    test2 = sorted(test2, key=lambda row: row['start_time'])

    pprint((test1, test2))

    #self.assertCountEqual(test1, test2)
    self.assertEqual(test1[0], test2[0])
    self.assertEqual(test1[1], test2[1])

  def not_test_does_nothing(self) -> None:
    first1 = {
        'ip': '87.119.233.243',
        'is_control_ip': False,
        'country': 'RU',
        'name': '87-119-233-243.saransk.ru.',
        'domain': 'feedly.com',
        'is_control': False,
        'category': 'E-commerce',
        'error': None,
        'anomaly': False,
        'success': False,
        'controls_failed': True,
        'received': [],
        'rcode': [],
        'date': '2021-04-18',
        'start_time': '2021-04-18T14:49:01.62448452-04:00',
        'end_time': '2021-04-18T14:49:03.624563629-04:00',
        'source': 'CP_Satellite-2021-04-18-12-00-01',
        'measurement_id': ''
    }
    second1 = {
        'received': [],
        'domain': 'feedly.com',
        'is_control': False,
        'category': 'E-commerce',
        'ip': '87.119.233.243',
        'is_control_ip': False,
        'country': 'RU',
        'date': '2021-04-18',
        'start_time': '2021-04-18T14:49:01.62448452-04:00',
        'end_time': '2021-04-18T14:49:03.624563629-04:00',
        'error': None,
        'anomaly': False,
        'success': False,
        'measurement_id': '',
        'source': 'CP_Satellite-2021-04-18-12-00-01',
        'controls_failed': True,
        'name': '87-119-233-243.saransk.ru.'
    }

    self.assertEqual(first1, second1)
