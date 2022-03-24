"""Unit tests for flattening data"""

import unittest

from pipeline.metadata import flatten
from pipeline.metadata.schema import HyperquackRow, SatelliteRow, SatelliteAnswer, SatelliteTags


class FlattenMeasurementTest(unittest.TestCase):
  """Unit tests for pipeline flattening."""

  def test_flattenmeasurement_invalid_json(self) -> None:
    """Test logging an error when parsing invalid JSON."""
    line = 'invalid json'

    with self.assertLogs(level='WARNING') as cm:
      flattener = flatten.FlattenMeasurement()
      flattener.setup()
      rows = list(flattener.process(('test_filename.json', line)))
      self.assertEqual(
          cm.output[0], 'WARNING:root:JSONDecodeError: '
          'Expecting value: line 1 column 1 (char 0)\n'
          'Filename: test_filename.json\ninvalid json\n')

    self.assertEqual(len(rows), 0)

  def test_flattenmeasurement_hyperquack(self) -> None:
    """Test flattening a hyperquack measurement."""

    filename = 'gs://firehook-scans/echo/CP_Quack-echo-2019-10-16-01-01-17/results.json'

    line = """
    {
      "Server":"146.112.62.39",
      "Keyword":"google.com.ua",
      "Retries":0,
      "Results":[
          {
            "Sent":"GET / HTTP/1.1\\r\\nHost: google.com.ua\\r\\n\\r\\n",
            "Success":true,
            "StartTime":"2020-11-14T07:54:49.246304766-05:00",
            "EndTime":"2020-11-14T07:54:49.279150352-05:00"
          }
      ],
      "Blocked":false,
      "FailSanity":false,
      "StatefulBlock":false
    }
    """

    expected_row = HyperquackRow(
        anomaly=False,
        category='News Media',
        controls_failed=False,
        date='2020-11-14',
        domain='google.com.ua',
        end_time='2020-11-14T07:54:49.279150352-05:00',
        ip='146.112.62.39',
        is_control=False,
        measurement_id='74897db64fb45a848facb4ee40f7a00e',
        source='CP_Quack-echo-2019-10-16-01-01-17',
        start_time='2020-11-14T07:54:49.246304766-05:00',
        stateful_block=False,
        success=True)

    flattener = flatten.FlattenMeasurement()
    flattener.setup()

    row = list(flattener.process((filename, line)))[0]
    self.assertEqual(row, expected_row)

  def test_flattenmeasurement_satellite(self) -> None:
    """Test flattening a satellite measurement."""

    filename = 'gs://firehook-scans/satellite/CP_Satellite-2020-09-02-12-00-01/interference.json'

    line = """
    {
      "resolver": "67.69.184.215",
      "query": "asana.com",
      "answers": {
        "151.101.1.184": ["ip", "http", "cert", "asnum", "asname"]
      },
      "passed": true
    }
    """

    expected_row = SatelliteRow(
        domain='asana.com',
        is_control=False,
        controls_failed=False,
        category='E-commerce',
        ip='67.69.184.215',
        is_control_ip=False,
        date='2020-09-02',
        error=None,
        anomaly=False,
        success=True,
        received=[
            SatelliteAnswer(
                ip='151.101.1.184',
                tags=SatelliteTags(matches_control='ip http cert asnum asname'))
        ],
        rcode=0,
        measurement_id='87a324f6c03150dda81c93bdeddb4adf',
        source='CP_Satellite-2020-09-02-12-00-01')

    flattener = flatten.FlattenMeasurement()
    flattener.setup()

    row = list(flattener.process((filename, line)))[0]
    self.assertEqual(row, expected_row)
