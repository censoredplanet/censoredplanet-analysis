import unittest

def get_small(row):
  new_row = {
    "start_time": row["start_time"],
    "end_time": row["end_time"],
    "domain": row["domain"]
  }
  return new_row




class FormatTest(unittest.TestCase):

  def test_lists_equal(self) -> None:

    self.maxDiff = None

    list1 = [{'ip': '87.119.233.243', 'is_control_ip': False, 'country': 'RU', 'name': '87-119-233-243.saransk.ru.', 'domain': 'feedly.com', 'is_control': False, 'category': 'E-commerce', 'error': None, 'anomaly': False, 'success': False, 'controls_failed': True, 'received': [], 'rcode': [], 'date': '2021-04-18', 'start_time': '2021-04-18T14:49:01.62448452-04:00', 'end_time': '2021-04-18T14:49:03.624563629-04:00', 'source': 'CP_Satellite-2021-04-18-12-00-01', 'measurement_id': ''}, {'ip': '12.5.76.236', 'is_control_ip': False, 'country': 'US', 'name': 'ns1327.ztomy.com.', 'domain': 'ultimate-guitar.com', 'is_control': False, 'category': 'History arts and literature', 'error': None, 'anomaly': True, 'success': True, 'controls_failed': False, 'received': [], 'rcode': ['2'], 'date': '2021-04-18', 'start_time': '2021-04-18T14:49:07.712972288-04:00', 'end_time': '2021-04-18T14:49:07.749265765-04:00', 'source': 'CP_Satellite-2021-04-18-12-00-01', 'measurement_id': ''}, {'ip': '64.6.65.6', 'is_control_ip': True, 'name': 'rec1pubns2.ultradns.net.', 'domain': 'a.root-servers.net', 'is_control': True, 'category': 'Control', 'error': None, 'anomaly': None, 'success': True, 'controls_failed': False, 'has_type_a': True, 'received': [{'ip': '198.41.0.4"'}], 'rcode': ['0'], 'date': '2021-04-18', 'start_time': '2021-04-18T14:51:57.561175746-04:00', 'end_time': '2021-04-18T14:51:57.587097567-04:00', 'source': 'CP_Satellite-2021-04-18-12-00-01', 'measurement_id': ''}, {'ip': '64.6.65.6', 'is_control_ip': True, 'name': 'rec1pubns2.ultradns.net.', 'domain': 'ultimate-guitar.com', 'is_control': False, 'category': 'History arts and literature', 'error': None, 'anomaly': None, 'success': True, 'controls_failed': False, 'has_type_a': True, 'received': [{'ip': '178.18.22.152'}], 'rcode': ['0'], 'date': '2021-04-18', 'start_time': '2021-04-18T14:51:57.587109091-04:00', 'end_time': '2021-04-18T14:51:57.61294601-04:00', 'source': 'CP_Satellite-2021-04-18-12-00-01', 'measurement_id': ''}, {'ip': '64.6.65.6', 'is_control_ip': True, 'name': 'rec1pubns2.ultradns.net.', 'domain': 'a.root-servers.net', 'is_control': True, 'category': 'Control', 'error': None, 'anomaly': None, 'success': True, 'controls_failed': False, 'has_type_a': True, 'received': [{'ip': '198.41.0.4'}], 'rcode': ['0'], 'date': '2021-04-18', 'start_time': '2021-04-18T14:51:45.836310062-04:00', 'end_time': '2021-04-18T14:51:45.862080031-04:00', 'source': 'CP_Satellite-2021-04-18-12-00-01', 'measurement_id': ''}, {'ip': '64.6.65.6', 'is_control_ip': True, 'name': 'rec1pubns2.ultradns.net.', 'domain': 'www.awid.org', 'is_control': False, 'category': 'Human Rights Issues', 'error': 'read udp 141.212.123.185:39868->64.6.65.6:53: i/o timeout', 'anomaly': None, 'success': True, 'controls_failed': False, 'has_type_a': False, 'received': [], 'rcode': ['-1'], 'date': '2021-04-18', 'start_time': '2021-04-18T14:51:45.862091022-04:00', 'end_time': '2021-04-18T14:51:47.862170832-04:00', 'source': 'CP_Satellite-2021-04-18-12-00-01', 'measurement_id': ''}, {'ip': '64.6.65.6', 'is_control_ip': True, 'name': 'rec1pubns2.ultradns.net.', 'domain': 'www.awid.org', 'is_control': False, 'category': 'Human Rights Issues', 'error': None, 'anomaly': None, 'success': True, 'controls_failed': False, 'has_type_a': True, 'received': [{'ip': '204.187.13.189'}], 'rcode': ['0'], 'date': '2021-04-18', 'start_time': '2021-04-18T14:51:47.862183185-04:00', 'end_time': '2021-04-18T14:51:48.162724942-04:00', 'source': 'CP_Satellite-2021-04-18-12-00-01', 'measurement_id': ''}]
    list2 =  [{'received': [{'ip': '198.41.0.4'}], 'domain': 'a.root-servers.net', 'is_control': True, 'category': 'Control', 'ip': '64.6.65.6', 'is_control_ip': True, 'date': '2021-04-18', 'start_time': '2021-04-18T14:51:57.561175746-04:00', 'end_time': '2021-04-18T14:51:57.587097567-04:00', 'anomaly': None, 'success': True, 'controls_failed': False, 'rcode': ['0'], 'has_type_a': True, 'measurement_id': '', 'source': 'CP_Satellite-2021-04-18-12-00-01', 'error': None, 'name': 'rec1pubns2.ultradns.net.'}, {'received': [{'ip': '198.41.0.4'}], 'domain': 'a.root-servers.net', 'is_control': True, 'category': 'Control', 'ip': '64.6.65.6', 'is_control_ip': True, 'date': '2021-04-18', 'start_time': '2021-04-18T14:51:45.836310062-04:00', 'end_time': '2021-04-18T14:51:45.862080031-04:00', 'anomaly': None, 'success': True, 'controls_failed': False, 'rcode': ['0'], 'has_type_a': True, 'measurement_id': '', 'source': 'CP_Satellite-2021-04-18-12-00-01', 'error': None, 'name': 'rec1pubns2.ultradns.net.'}, {'received': [], 'domain': 'www.awid.org', 'is_control': False, 'category': 'Human Rights Issues', 'ip': '64.6.65.6', 'is_control_ip': True, 'date': '2021-04-18', 'start_time': '2021-04-18T14:51:45.862091022-04:00', 'end_time': '2021-04-18T14:51:47.862170832-04:00', 'anomaly': None, 'success': True, 'controls_failed': False, 'rcode': ['-1'], 'has_type_a': False, 'measurement_id': '', 'source': 'CP_Satellite-2021-04-18-12-00-01', 'error': 'read udp 141.212.123.185:39868->64.6.65.6:53: i/o timeout', 'name': 'rec1pubns2.ultradns.net.'}, {'received': [{'ip': '204.187.13.189'}], 'domain': 'www.awid.org', 'is_control': False, 'category': 'Human Rights Issues', 'ip': '64.6.65.6', 'is_control_ip': True, 'date': '2021-04-18', 'start_time': '2021-04-18T14:51:47.862183185-04:00', 'end_time': '2021-04-18T14:51:48.162724942-04:00', 'anomaly': None, 'success': True, 'controls_failed': False, 'rcode': ['0'], 'has_type_a': True, 'measurement_id': '', 'source': 'CP_Satellite-2021-04-18-12-00-01', 'error': None, 'name': 'rec1pubns2.ultradns.net.'}, {'received': [], 'domain': 'feedly.com', 'is_control': False, 'category': 'E-commerce', 'ip': '87.119.233.243', 'is_control_ip': False, 'country': 'RU', 'date': '2021-04-18', 'start_time': '2021-04-18T14:49:01.62448452-04:00', 'end_time': '2021-04-18T14:49:03.624563629-04:00', 'error': None, 'anomaly': False, 'success': False, 'measurement_id': '', 'source': 'CP_Satellite-2021-04-18-12-00-01', 'controls_failed': True, 'rcode': [], 'name': '87-119-233-243.saransk.ru.'}, {'received': [], 'domain': 'ultimate-guitar.com', 'is_control': False, 'category': 'History arts and literature', 'ip': '12.5.76.236', 'is_control_ip': False, 'country': 'US', 'date': '2021-04-18', 'start_time': '2021-04-18T14:49:07.712972288-04:00', 'end_time': '2021-04-18T14:49:07.749265765-04:00', 'error': None, 'anomaly': True, 'success': True, 'measurement_id': '', 'source': 'CP_Satellite-2021-04-18-12-00-01', 'controls_failed': False, 'rcode': ['2'], 'name': 'ns1327.ztomy.com.'}, {'received': [{'ip': '178.18.22.152'}], 'domain': 'ultimate-guitar.com', 'is_control': False, 'category': 'History arts and literature', 'ip': '64.6.65.6', 'is_control_ip': True, 'date': '2021-04-18', 'start_time': '2021-04-18T14:51:57.587109091-04:00', 'end_time': '2021-04-18T14:51:57.61294601-04:00', 'anomaly': None, 'success': True, 'controls_failed': False, 'rcode': ['0'], 'has_type_a': True, 'measurement_id': '', 'source': 'CP_Satellite-2021-04-18-12-00-01', 'error': None, 'name': 'rec1pubns2.ultradns.net.'}]


    sorted_list_1 = sorted(list1, key=lambda x: x['start_time'])
    sorted_list_2 = sorted(list2, key=lambda x: x['start_time'])
    
    # test
    for (l1, l2) in zip(sorted_list_1, sorted_list_2):
      self.assertEqual(l1, l2)

    self.assertListEqual(list1, list2)

    small_list1 = list(map(get_small, list1))
    small_list2 = list(map(get_small, list2))

    sorted_list_1 = sorted(small_list1, key=lambda x: x['start_time'])
    sorted_list_2 = sorted(small_list2, key=lambda x: x['start_time'])

    from pprint import pprint

    pprint(sorted_list_1)
    pprint(sorted_list_2)

    self.assertListEqual(sorted_list_1, sorted_list_2)