# Copyright 2020 Jigsaw Operations LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Test top-level runner for beam pipelines."""

import argparse
import datetime
import unittest
from unittest.mock import call, patch, MagicMock

from pipeline import beam_tables
from pipeline import run_beam_tables


class RunBeamTablesTest(unittest.TestCase):
  """Test running beam pipelines.

  This tests the runner arg parsing and parallelization,
  not the beam pipelines themselves.
  """

  def test_run_single_pipelines(self) -> None:
    """Test running a single dated pipeline."""
    mock_runner = MagicMock(beam_tables.ScanDataBeamPipelineRunner)
    mock_runner.output_bucket = 'firehook-test'

    run_beam_tables.run_parallel_pipelines(mock_runner, 'base', ['echo'], True,
                                           datetime.date(2020, 1, 1),
                                           datetime.date(2020, 1, 2), False)

    mock_runner.run_beam_pipeline.assert_called_with('echo', True,
                                                     'append-base-echo-scan',
                                                     'base.echo_scan', None,
                                                     datetime.date(2020, 1, 1),
                                                     datetime.date(2020, 1,
                                                                   2), False)

    run_beam_tables.run_parallel_pipelines(mock_runner, 'base', ['echo'], True,
                                           datetime.date(2020, 1, 1),
                                           datetime.date(2020, 1, 2), True)

    mock_runner.run_beam_pipeline.assert_called_with(
        'echo', True, 'append-gs-firehook-test-base-echo', None,
        'gs://firehook-test/base/echo', datetime.date(2020, 1, 1),
        datetime.date(2020, 1, 2), True)

  def test_run_parallel_pipelines(self) -> None:
    """Test running two pipelines in parallel."""
    mock_runner = MagicMock(beam_tables.ScanDataBeamPipelineRunner)
    mock_runner.output_bucket = 'firehook-test'

    run_beam_tables.run_parallel_pipelines(mock_runner, 'laplante',
                                           ['http', 'https'], False, None, None,
                                           False)

    call1 = call('http', False, 'write-laplante-http-scan',
                 'laplante.http_scan', None, None, None, False)
    call2 = call('https', False, 'write-laplante-https-scan',
                 'laplante.https_scan', None, None, None, False)
    mock_runner.run_beam_pipeline.assert_has_calls([call1, call2],
                                                   any_order=True)

    run_beam_tables.run_parallel_pipelines(mock_runner, 'laplante',
                                           ['http', 'https'], False, None, None,
                                           True)

    call3 = call('http', False, 'write-gs-firehook-test-laplante-http', None,
                 'gs://firehook-test/laplante/http', None, None, True)
    call4 = call('https', False, 'write-gs-firehook-test-laplante-https', None,
                 'gs://firehook-test/laplante/https', None, None, True)
    mock_runner.run_beam_pipeline.assert_has_calls([call3, call4],
                                                   any_order=True)

  def test_main_prod(self) -> None:
    """Test arg parsing for prod pipelines."""
    mock_runner = MagicMock(beam_tables.ScanDataBeamPipelineRunner)
    mock_runner.output_bucket = 'firehook-test'

    with patch('pipeline.run_beam_tables.get_beam_pipeline_runner',
               lambda _: mock_runner):
      args = argparse.Namespace(
          full=False,
          scan_type='all',
          env='prod',
          start_date=None,
          end_date=None,
          export_gcs=False)
      run_beam_tables.main(args)

      call1 = call('echo', True, 'append-base-echo-scan', 'base.echo_scan',
                   None, None, None, False)
      call2 = call('discard', True, 'append-base-discard-scan',
                   'base.discard_scan', None, None, None, False)
      call3 = call('http', True, 'append-base-http-scan', 'base.http_scan',
                   None, None, None, False)
      call4 = call('https', True, 'append-base-https-scan', 'base.https_scan',
                   None, None, None, False)
      call5 = call('satellite', True, 'append-base-satellite-scan',
                   'base.satellite_scan', None, None, None, False)
      mock_runner.run_beam_pipeline.assert_has_calls(
          [call1, call2, call3, call4, call5], any_order=True)
      # No extra calls
      self.assertEqual(5, mock_runner.run_beam_pipeline.call_count)

      args = argparse.Namespace(
          full=False,
          scan_type='all',
          env='prod',
          start_date=None,
          end_date=None,
          export_gcs=True)
      run_beam_tables.main(args)

      call6 = call('echo', True, 'append-gs-firehook-test-base-echo', None,
                   'gs://firehook-test/base/echo', None, None, True)
      call7 = call('discard', True, 'append-gs-firehook-test-base-discard',
                   None, 'gs://firehook-test/base/discard', None, None, True)
      call8 = call('http', True, 'append-gs-firehook-test-base-http', None,
                   'gs://firehook-test/base/http', None, None, True)
      call9 = call('https', True, 'append-gs-firehook-test-base-https', None,
                   'gs://firehook-test/base/https', None, None, True)
      call10 = call('satellite', True, 'append-gs-firehook-test-base-satellite',
                    None, 'gs://firehook-test/base/satellite', None, None, True)
      mock_runner.run_beam_pipeline.assert_has_calls(
          [call6, call7, call8, call9, call10], any_order=True)
      # No extra calls
      self.assertEqual(10, mock_runner.run_beam_pipeline.call_count)

  def test_main_user_dates(self) -> None:
    """Test arg parsing for a user pipeline with dates."""
    mock_runner = MagicMock(beam_tables.ScanDataBeamPipelineRunner)
    mock_runner.output_bucket = 'firehook-test'

    with patch('pipeline.run_beam_tables.get_beam_pipeline_runner',
               lambda _: mock_runner):
      args = argparse.Namespace(
          full=False,
          scan_type='echo',
          env='user',
          user_dataset='laplante',
          start_date=datetime.date(2021, 1, 8),
          end_date=datetime.date(2021, 1, 15),
          export_gcs=False)
      run_beam_tables.main(args)

      call1 = call('echo', True, 'append-laplante-echo-scan',
                   'laplante.echo_scan', None, datetime.date(2021, 1, 8),
                   datetime.date(2021, 1, 15), False)
      mock_runner.run_beam_pipeline.assert_has_calls([call1])
      self.assertEqual(1, mock_runner.run_beam_pipeline.call_count)

      args = argparse.Namespace(
          full=False,
          scan_type='echo',
          env='user',
          user_dataset='laplante',
          start_date=datetime.date(2021, 1, 8),
          end_date=datetime.date(2021, 1, 15),
          export_gcs=True)
      run_beam_tables.main(args)

      call2 = call('echo', True, 'append-gs-firehook-test-laplante-echo',
                   None, 'gs://firehook-test/laplante/echo',
                   datetime.date(2021, 1, 8), datetime.date(2021, 1, 15), True)
      mock_runner.run_beam_pipeline.assert_has_calls([call2])
      self.assertEqual(2, mock_runner.run_beam_pipeline.call_count)


if __name__ == '__main__':
  unittest.main()
