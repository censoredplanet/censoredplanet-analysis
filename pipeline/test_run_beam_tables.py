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
from collections import namedtuple
import datetime
import unittest
from unittest.mock import call, patch, MagicMock

from freezegun import freeze_time

from pipeline import beam_tables
from pipeline import run_beam_tables


class RunBeamTablesTest(unittest.TestCase):
  """Test running beam pipelines.

  This tests the runner arg parsing and parallelization,
  not the beam pipelines themselves.
  """

  # pylint: disable=no-self-use

  def test_run_single_pipelines(self) -> None:
    """Test running a single dated pipeline."""
    mock_runner = MagicMock(beam_tables.ScanDataBeamPipelineRunner)

    run_beam_tables.run_parallel_pipelines(mock_runner, 'base', ['echo'], True,
                                           datetime.date(2020, 1, 1),
                                           datetime.date(2020, 1, 2))

    mock_runner.run_beam_pipeline.assert_called_with('echo', True,
                                                     'append-base-echo-scan',
                                                     'base.echo_scan',
                                                     datetime.date(2020, 1, 1),
                                                     datetime.date(2020, 1, 2))

  def test_run_parallel_pipelines(self) -> None:
    """Test running two pipelines in parallel."""
    mock_runner = MagicMock(beam_tables.ScanDataBeamPipelineRunner)

    run_beam_tables.run_parallel_pipelines(mock_runner, 'laplante',
                                           ['http', 'https'], False, None, None)

    call1 = call('http', False, 'write-laplante-http-scan',
                 'laplante.http_scan', None, None)
    call2 = call('https', False, 'write-laplante-https-scan',
                 'laplante.https_scan', None, None)
    mock_runner.run_beam_pipeline.assert_has_calls([call1, call2],
                                                   any_order=True)

  @freeze_time('2020-01-15')
  def test_run_user_pipeline_full(self) -> None:
    """Test running a user pipeline with automatically chosen dates."""
    mock_runner = MagicMock(beam_tables.ScanDataBeamPipelineRunner)

    run_beam_tables.run_user_pipelines(mock_runner, 'laplante', ['echo'], False,
                                       None, None)

    mock_runner.run_beam_pipeline.assert_called_with(
        'echo', False, 'write-laplante-echo-scan', 'laplante.echo_scan',
        datetime.date(2020, 1, 1), datetime.date(2020, 1, 8))

  @freeze_time('2020-01-15')
  def test_run_user_pipeline_incremental(self) -> None:
    """Test running a user pipeline with automatic incremental dates."""
    mock_runner = MagicMock(beam_tables.ScanDataBeamPipelineRunner)

    run_beam_tables.run_user_pipelines(mock_runner, 'laplante', ['echo'], True,
                                       None, None)

    mock_runner.run_beam_pipeline.assert_called_with(
        'echo', True, 'append-laplante-echo-scan', 'laplante.echo_scan',
        datetime.date(2020, 1, 8), datetime.date(2020, 1, 15))

  def test_run_user_pipeline_dated(self) -> None:
    """Test running a user pipeline with automatic incremental dates."""
    mock_runner = MagicMock(beam_tables.ScanDataBeamPipelineRunner)

    run_beam_tables.run_user_pipelines(mock_runner, 'laplante', ['echo'], True,
                                       datetime.date(2021, 1, 8),
                                       datetime.date(2021, 1, 15))

    mock_runner.run_beam_pipeline.assert_called_with(
        'echo', True, 'append-laplante-echo-scan', 'laplante.echo_scan',
        datetime.date(2021, 1, 8), datetime.date(2021, 1, 15))

  def test_main_prod(self) -> None:
    """Test arg parsing for prod pipelines."""
    mock_runner = MagicMock(beam_tables.ScanDataBeamPipelineRunner)

    with patch('pipeline.run_beam_tables.get_firehook_beam_pipeline_runner',
               lambda: mock_runner):
      args = argparse.Namespace(
          full=False,
          scan_type='all',
          env='prod',
          start_date=None,
          end_date=None)
      run_beam_tables.main(args)

      call1 = call('echo', True, 'append-base-echo-scan', 'base.echo_scan',
                   None, None)
      call2 = call('discard', True, 'append-base-discard-scan',
                   'base.discard_scan', None, None)
      call3 = call('http', True, 'append-base-http-scan', 'base.http_scan',
                   None, None)
      call4 = call('https', True, 'append-base-https-scan', 'base.https_scan',
                   None, None)
      mock_runner.run_beam_pipeline.assert_has_calls(
          [call1, call2, call3, call4], any_order=True)
      # No extra calls
      self.assertEqual(4, mock_runner.run_beam_pipeline.call_count)

  @patch('pwd.getpwuid', lambda x: namedtuple('mock_uid', ['pw_name'])
         ('laplante'))
  def test_main_user_dates(self) -> None:
    """Test arg parsing for a user pipeline with dates."""
    mock_runner = MagicMock(beam_tables.ScanDataBeamPipelineRunner)

    with patch('pipeline.run_beam_tables.get_firehook_beam_pipeline_runner',
               lambda: mock_runner):
      args = argparse.Namespace(
          full=False,
          scan_type='echo',
          env='user',
          start_date=datetime.date(2021, 1, 8),
          end_date=datetime.date(2021, 1, 15))
      run_beam_tables.main(args)

      call1 = call('echo', True, 'append-laplante-echo-scan',
                   'laplante.echo_scan', datetime.date(2021, 1, 8),
                   datetime.date(2021, 1, 15))
      mock_runner.run_beam_pipeline.assert_has_calls([call1])
      self.assertEqual(1, mock_runner.run_beam_pipeline.call_count)

  @freeze_time('2020-01-15')
  @patch('pwd.getpwuid', lambda x: namedtuple('mock_uid', ['pw_name'])
         ('laplante'))
  def test_main_user_no_dates(self) -> None:
    """Test arg parsing for a user pipeline with no dates."""
    mock_runner = MagicMock(beam_tables.ScanDataBeamPipelineRunner)

    with patch('pipeline.run_beam_tables.get_firehook_beam_pipeline_runner',
               lambda: mock_runner):
      args = argparse.Namespace(
          full=False,
          scan_type='echo',
          env='user',
          start_date=None,
          end_date=None)
      run_beam_tables.main(args)

      call1 = call('echo', True, 'append-laplante-echo-scan',
                   'laplante.echo_scan', datetime.date(2020, 1, 8),
                   datetime.date(2020, 1, 15))
      mock_runner.run_beam_pipeline.assert_has_calls([call1])
      self.assertEqual(1, mock_runner.run_beam_pipeline.call_count)

  @patch('pwd.getpwuid', lambda x: namedtuple('mock_uid', ['pw_name'])
         ('laplante'))
  def test_main_user_mixed_dates(self) -> None:
    """Test arg parsing for a user pipeline with one date specified."""
    mock_runner = MagicMock(beam_tables.ScanDataBeamPipelineRunner)

    with patch('pipeline.run_beam_tables.get_firehook_beam_pipeline_runner',
               lambda: mock_runner):
      args = argparse.Namespace(
          full=False,
          scan_type='echo',
          env='user',
          start_date=datetime.date(2021, 1, 8),
          end_date=None)
      run_beam_tables.main(args)

      call1 = call('echo', True, 'append-laplante-echo-scan',
                   'laplante.echo_scan', datetime.date(2021, 1, 8), None)
      mock_runner.run_beam_pipeline.assert_has_calls([call1])
      self.assertEqual(1, mock_runner.run_beam_pipeline.call_count)

  # pylint: enable=no-self-use


if __name__ == '__main__':
  unittest.main()
